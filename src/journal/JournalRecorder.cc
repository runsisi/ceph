// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalRecorder.h"
#include "common/errno.h"
#include "journal/Entry.h"
#include "journal/Utils.h"

#include <atomic>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalRecorder: " << this << " "

using std::shared_ptr;

namespace journal {

namespace {

struct C_Flush : public Context {
  JournalMetadataPtr journal_metadata;
  Context *on_finish;
  std::atomic<int64_t> pending_flushes = { 0 };
  int ret_val;

  C_Flush(JournalMetadataPtr _journal_metadata, Context *_on_finish,
          size_t _pending_flushes)
    : journal_metadata(_journal_metadata), on_finish(_on_finish),
      pending_flushes(_pending_flushes), ret_val(0) {
  }

  void complete(int r) override {
    if (r < 0 && ret_val == 0) {
      ret_val = r;
    }
    if (--pending_flushes == 0) {
      // ensure all prior callback have been flushed as well
      journal_metadata->queue(on_finish, ret_val);
      delete this;
    }
  }
  void finish(int r) override {
  }
};

} // anonymous namespace

// created by
// Journaler::start_append
JournalRecorder::JournalRecorder(librados::IoCtx &ioctx,
                                 const std::string &object_oid_prefix,
                                 const JournalMetadataPtr& journal_metadata,
                                 uint32_t flush_interval, uint64_t flush_bytes,
                                 double flush_age,
                                 uint64_t max_in_flight_appends)
  : m_cct(NULL), m_object_oid_prefix(object_oid_prefix),
    m_journal_metadata(journal_metadata), m_flush_interval(flush_interval),
    m_flush_bytes(flush_bytes), m_flush_age(flush_age),
    m_max_in_flight_appends(max_in_flight_appends), m_listener(this),
    m_object_handler(this), m_lock("JournalerRecorder::m_lock"),
    m_current_set(m_journal_metadata->get_active_set()) {

  Mutex::Locker locker(m_lock);

  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());

  uint8_t splay_width = m_journal_metadata->get_splay_width();

  for (uint8_t splay_offset = 0; splay_offset < splay_width; ++splay_offset) {
    m_object_locks.push_back(shared_ptr<Mutex>(
                                          new Mutex("ObjectRecorder::m_lock::"+
                                          std::to_string(splay_offset))));

    uint64_t object_number = splay_offset + (m_current_set * splay_width);

    m_object_ptrs[splay_offset] = create_object_recorder(
                                                object_number,
                                                m_object_locks[splay_offset]);
  }

  // implements JournalRecorder::handle_update event
  m_journal_metadata->add_listener(&m_listener);
}

JournalRecorder::~JournalRecorder() {
  m_journal_metadata->remove_listener(&m_listener);

  Mutex::Locker locker(m_lock);
  ceph_assert(m_in_flight_advance_sets == 0);
  ceph_assert(m_in_flight_object_closes == 0);
}

// called by Journaler::append
Future JournalRecorder::append(uint64_t tag_tid,
                               const bufferlist &payload_bl) {

  m_lock.Lock();

  // entry id is tag specific, i.e., global to a specific tag, it determines
  // which ObjectRecorder this entry will be appended to
  // i.e., entry_tid =  m_allocated_entry_tids[tag_tid]++
  uint64_t entry_tid = m_journal_metadata->allocate_entry_tid(tag_tid);

  uint8_t splay_width = m_journal_metadata->get_splay_width();

  // determine which object to write
  uint8_t splay_offset = entry_tid % splay_width;

  // one ObjectRecorder at an offset
  ObjectRecorderPtr object_ptr = get_object(splay_offset);

  // commit id is global to the JournalMetadata instance, it is used to
  // record the pending entries, by the commit id we can find the
  // entry to be committed, see JournalMetadata::m_pending_commit_tids

  // NOTE: entry_tid is not global unique, so need an other global unique
  // tid, i.e., commit_tid to identify the entry in the journal
  // <m_tag_tid, m_entry_tid> will be encoded with journal::EventEntry and
  // appended into journal object, while m_commit_tid is a in memory global
  // unique id, it will be used by Journaler::committed later

  // construct an CommitEntry and register into m_pending_commit_tids
  // i.e., JournalMetadata::
  //   ++m_commit_tid
  //   m_pending_commit_tids[commit_tid] = CommitEntry()
  uint64_t commit_tid = m_journal_metadata->allocate_commit_tid(
    object_ptr->get_object_number(), tag_tid, entry_tid);

  // a future represents an append of librbd::journal::EventEntry, see
  // Journal<I>::append_io_events
  FutureImplPtr future(new FutureImpl(tag_tid, entry_tid, commit_tid));

  // set FutureImpl::m_prev_future, so we can chain all the futures
  future->init(m_prev_future); // chain the FutureImpl::m_consistent_ack callback

  m_prev_future = future;

  m_object_locks[splay_offset]->Lock();

  m_lock.Unlock();

  bufferlist entry_bl;
  encode(Entry(future->get_tag_tid(), future->get_entry_tid(), payload_bl),
	 entry_bl);
  ceph_assert(entry_bl.length() <= m_journal_metadata->get_object_size());

  bool object_full = object_ptr->append_unlock({{future, entry_bl}});
  if (object_full) {
    // the written size to the object since the creation of this ObjectRecorder instance
    // has been reach the limit
    ldout(m_cct, 10) << "object " << object_ptr->get_oid() << " now full"
                     << dendl;

    Mutex::Locker l(m_lock);

    // can also be called by JournalRecorder::handle_overflow, which means we has not
    // written too much since the creation of the ObjectRecorder instance, but this
    // object has been written much during the last journal recording period
    close_and_advance_object_set(object_ptr->get_object_number() / splay_width);
  }

  return Future(future);
}

// called by
// Journaler::stop_append
// Journaler::flush_append, which never used
void JournalRecorder::flush(Context *on_safe) {
  C_Flush *ctx;

  {
    Mutex::Locker locker(m_lock);

    // we are to flush every object, after all objects flushed, i.e., pending
    // flushes count to 0, the user's callback will be called
    ctx = new C_Flush(m_journal_metadata, on_safe, m_object_ptrs.size() + 1);

    for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
         it != m_object_ptrs.end(); ++it) {

      // ObjectRecoder::flush
      it->second->flush(ctx);
    }

  }

  // avoid holding the lock in case there is nothing to flush
  ctx->complete(0);
}

ObjectRecorderPtr JournalRecorder::get_object(uint8_t splay_offset) {
  ceph_assert(m_lock.is_locked());

  ObjectRecorderPtr object_recoder = m_object_ptrs[splay_offset];
  ceph_assert(object_recoder != NULL);
  return object_recoder;
}

// called by
// JournalRecorder::append
// JournalRecorder::handle_overflow
void JournalRecorder::close_and_advance_object_set(uint64_t object_set) {
  ceph_assert(m_lock.is_locked());

  // entry overflow from open object
  if (m_current_set != object_set) {
    // m_current_set has been increased, i.e., JournalRecorder::close_and_advance_object_set
    // has been called previously, which mean the previous call was issued coz JournalRecorder::append
    // returned full, and the current call is issued by the overflow
    ldout(m_cct, 20) << __func__ << ": close already in-progress" << dendl;

    return;
  }

  // we shouldn't overflow upon append if already closed and we
  // shouldn't receive an overflowed callback if already closed
  ceph_assert(m_in_flight_advance_sets == 0);
  ceph_assert(m_in_flight_object_closes == 0);

  uint64_t active_set = m_journal_metadata->get_active_set();
  ceph_assert(m_current_set == active_set);
  ++m_current_set;
  ++m_in_flight_advance_sets;

  ldout(m_cct, 20) << __func__ << ": closing active object set "
                   << object_set << dendl;

  if (close_object_set(m_current_set)) { // issue object close op for this set of objects

    // no in-flight buffers, i.e., this set of objects closed succeeded

    // update active set in journal metadata object
    advance_object_set();
  }
}

// called by
// JournalRecorder::close_and_advance_object_set
// JournalRecorder::handle_closed
void JournalRecorder::advance_object_set() {
  ceph_assert(m_lock.is_locked());

  ceph_assert(m_in_flight_object_closes == 0);
  ldout(m_cct, 20) << __func__ << ": advance to object set " << m_current_set
                   << dendl;

  // journal_recorder->handle_advance_object_set, m_current_set has been
  // inc by JournalRecorder::close_and_advance_object_set
  m_journal_metadata->set_active_set(m_current_set, new C_AdvanceObjectSet(
    this));
}

// called by
// C_AdvanceObjectSet::finish
void JournalRecorder::handle_advance_object_set(int r) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << __func__ << ": r=" << r << dendl;

  ceph_assert(m_in_flight_advance_sets > 0);
  --m_in_flight_advance_sets;

  if (r < 0 && r != -ESTALE) {
    lderr(m_cct) << __func__ << ": failed to advance object set: "
                 << cpp_strerror(r) << dendl;
  }

  if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
    // object set closed and journal metadata updated
    open_object_set();
  }
}

// called by
// JournalRecorder::handle_advance_object_set
// JournalRecorder::handle_update
// JournalRecorder::handle_closed
void JournalRecorder::open_object_set() {
  ceph_assert(m_lock.is_locked());

  ldout(m_cct, 10) << __func__ << ": opening object set " << m_current_set
                   << dendl;

  uint8_t splay_width = m_journal_metadata->get_splay_width();

  lock_object_recorders();

  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    ObjectRecorderPtr object_recorder = it->second;

    uint64_t object_number = object_recorder->get_object_number();

    if (object_number / splay_width != m_current_set) {
      ceph_assert(object_recorder->is_closed());

      // ready to close object and open object in active set
      create_next_object_recorder_unlock(object_recorder);
    } else {
      uint8_t splay_offset = object_number % splay_width;

      m_object_locks[splay_offset]->Unlock();
    }
  }
}

// called by
// JournalRecorder::close_and_advance_object_set
// JournalRecorder::handle_update
bool JournalRecorder::close_object_set(uint64_t active_set) {
  ceph_assert(m_lock.is_locked());

  // object recorders will invoke overflow handler as they complete
  // closing the object to ensure correct order of future appends
  uint8_t splay_width = m_journal_metadata->get_splay_width();

  lock_object_recorders();

  for (ObjectRecorderPtrs::iterator it = m_object_ptrs.begin();
       it != m_object_ptrs.end(); ++it) {
    ObjectRecorderPtr object_recorder = it->second;

    if (object_recorder->get_object_number() / splay_width != active_set) {
      ldout(m_cct, 10) << __func__ << ": closing object "
                       << object_recorder->get_oid() << dendl;

      // flush out all queued appends and hold future appends
      if (!object_recorder->close()) { // flush pending buffers
        ++m_in_flight_object_closes;
      } else {
        ldout(m_cct, 20) << __func__ << ": object "
                         << object_recorder->get_oid() << " closed" << dendl;
      }
    }
  }

  unlock_object_recorders();

  return (m_in_flight_object_closes == 0);
}

// called by
// JournalRecorder::JournalRecorder
// JournalRecorder::create_next_object_recorder_unlock
ObjectRecorderPtr JournalRecorder::create_object_recorder(
    uint64_t object_number, shared_ptr<Mutex> lock) {
  ObjectRecorderPtr object_recorder(new ObjectRecorder(
    m_ioctx, utils::get_object_name(m_object_oid_prefix, object_number),
    object_number, lock, m_journal_metadata->get_work_queue(),
    m_journal_metadata->get_timer(), m_journal_metadata->get_timer_lock(),
    &m_object_handler, m_journal_metadata->get_order(), m_flush_interval,
    m_flush_bytes, m_flush_age, m_max_in_flight_appends));
  return object_recorder;
}

// called by
// JournalRecorder::open_object_set
void JournalRecorder::create_next_object_recorder_unlock(
    ObjectRecorderPtr object_recorder) {
  ceph_assert(m_lock.is_locked());

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;

  ceph_assert(m_object_locks[splay_offset]->is_locked());

  ObjectRecorderPtr new_object_recorder = create_object_recorder(
     (m_current_set * splay_width) + splay_offset, m_object_locks[splay_offset]);

  ldout(m_cct, 10) << __func__ << ": "
                   << "old oid=" << object_recorder->get_oid() << ", "
                   << "new oid=" << new_object_recorder->get_oid() << dendl;

  AppendBuffers append_buffers;

  // stash ObjectRecorder::m_append_buffers into append_buffers
  object_recorder->claim_append_buffers(&append_buffers);

  // update the commit record to point to the correct object number
  for (auto &append_buffer : append_buffers) {
    m_journal_metadata->overflow_commit_tid(
      append_buffer.first->get_commit_tid(),
      new_object_recorder->get_object_number());
  }

  // re-append all those stashed appends, ObjectRecorder called in
  // JournalRecorder::append will check if the object is full, but here
  // we do not, we can only let the -EOVERFLOW to drive us to start
  // a new set of objects
  new_object_recorder->append_unlock(std::move(append_buffers));

  m_object_ptrs[splay_offset] = new_object_recorder;
}

// called by
// JournalMetadata::handle_refresh_complete, registered to listen JournalMetadata
// by ctor of JournalRecorder
void JournalRecorder::handle_update() {
  Mutex::Locker locker(m_lock);

  uint64_t active_set = m_journal_metadata->get_active_set();

  if (m_current_set < active_set) {
    // peer journal client advanced the active set
    ldout(m_cct, 20) << __func__ << ": "
                     << "current_set=" << m_current_set << ", "
                     << "active_set=" << active_set << dendl;

    uint64_t current_set = m_current_set;

    m_current_set = active_set;

    if (m_in_flight_advance_sets == 0 && m_in_flight_object_closes == 0) {
      ldout(m_cct, 20) << __func__ << ": closing current object set "
                       << current_set << dendl;

      if (close_object_set(active_set)) {
        open_object_set();
      }
    }
  }
}

// called by
// JournalRecorder::ObjectHandler::closed, which called by journal::ObjectRecorder::notify_handler_unlock
void JournalRecorder::handle_closed(ObjectRecorder *object_recorder) {
  ldout(m_cct, 10) << __func__ << ": " << object_recorder->get_oid() << dendl;

  Mutex::Locker locker(m_lock);

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;

  ObjectRecorderPtr active_object_recorder = m_object_ptrs[splay_offset];
  ceph_assert(active_object_recorder->get_object_number() == object_number);

  ceph_assert(m_in_flight_object_closes > 0);
  --m_in_flight_object_closes;

  // object closed after advance active set committed
  ldout(m_cct, 20) << __func__ << ": object "
                   << active_object_recorder->get_oid() << " closed" << dendl;

  if (m_in_flight_object_closes == 0) {

    // all objects of this set have been closed

    if (m_in_flight_advance_sets == 0) {
      // peer forced closing of object set, i.e., JournalRecorder::close_object_set
      // was issued by JournalRecorder::handle_update
      open_object_set();
    } else {
      // local overflow advanced object set, i.e., JournalRecorder::close_object_set
      // was issued by JournalRecorder::close_and_advance_object_set
      advance_object_set();
    }
  }
}

// called by
// JournalRecorder::ObjectHandler::overflow, which called by ObjectRecorder::notify_handler_unlock
void JournalRecorder::handle_overflow(ObjectRecorder *object_recorder) {
  ldout(m_cct, 10) << __func__ << ": " << object_recorder->get_oid() << dendl;

  Mutex::Locker locker(m_lock);

  uint64_t object_number = object_recorder->get_object_number();
  uint8_t splay_width = m_journal_metadata->get_splay_width();
  uint8_t splay_offset = object_number % splay_width;

  ObjectRecorderPtr active_object_recorder = m_object_ptrs[splay_offset];
  ceph_assert(active_object_recorder->get_object_number() == object_number);

  ldout(m_cct, 20) << __func__ << ": object "
                   << active_object_recorder->get_oid() << " overflowed"
                   << dendl;

  // overflow will drive us to close the object set
  close_and_advance_object_set(object_number / splay_width);
}

} // namespace journal
