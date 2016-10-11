// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectRecorder.h"
#include "journal/Future.h"
#include "journal/Utils.h"
#include "include/assert.h"
#include "common/Timer.h"
#include "cls/journal/cls_journal_client.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "ObjectRecorder: " << this << " "

using namespace cls::journal;
using std::shared_ptr;

namespace journal {

ObjectRecorder::ObjectRecorder(librados::IoCtx &ioctx, const std::string &oid,
                               uint64_t object_number, shared_ptr<Mutex> lock,
                               ContextWQ *work_queue, SafeTimer &timer,
                               Mutex &timer_lock, Handler *handler,
                               uint8_t order, uint32_t flush_interval,
                               uint64_t flush_bytes, double flush_age)
  : RefCountedObject(NULL, 0), m_oid(oid), m_object_number(object_number),
    m_cct(NULL), m_op_work_queue(work_queue), m_timer(timer),
    m_timer_lock(timer_lock), m_handler(handler), m_order(order),
    m_soft_max_size(1 << m_order), m_flush_interval(flush_interval),
    m_flush_bytes(flush_bytes), m_flush_age(flush_age), m_flush_handler(this),
    m_append_task(NULL), m_lock(lock), m_append_tid(0), m_pending_bytes(0),
    m_size(0), m_overflowed(false), m_object_closed(false),
    m_in_flight_flushes(false), m_aio_scheduled(false) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  assert(m_handler != NULL);
}

ObjectRecorder::~ObjectRecorder() {
  assert(m_append_task == NULL);
  assert(m_append_buffers.empty());
  assert(m_in_flight_tids.empty());
  assert(m_in_flight_appends.empty());
  assert(!m_aio_scheduled);
}

bool ObjectRecorder::append_unlock(AppendBuffers &&append_buffers) {
  assert(m_lock->is_locked());

  FutureImplPtr last_flushed_future;
  bool schedule_append = false;

  if (m_overflowed) {
    // already overflowed, we stash this buffer into m_append_buffers
    // and after all in-flight flush buffers returned, we will notify
    // the JournalRecorder to close the current object set and create
    // a new object set to restart flush those buffers
    m_append_buffers.insert(m_append_buffers.end(),
                            append_buffers.begin(), append_buffers.end());
    m_lock->Unlock();
    return false;
  }

  // std::list<std::pair<FutureImplPtr, bufferlist> >
  for (AppendBuffers::const_iterator iter = append_buffers.begin();
       iter != append_buffers.end(); ++iter) {

    // if we are called by Journaler then the append_buffers will only
    // contain a buffer, but if we are called by a internal restart
    // then there may be more than one buffer

    // if multiple buffers to append, then it may flush multiple times if
    // the pending buffers are big enough, the later appends will fail
    // with -EOVERFLOW

    if (append(*iter, &schedule_append)) { // push back of m_append_buffers and try to flush

      // the object has not been closed or overflowed, and this buffer
      // had been requested to flush and delayed by the
      // overflow, so we need to flush it now (may have been flushed
      // in this call already)


      last_flushed_future = iter->first;
    }
  }

  if (last_flushed_future) {

    // we have to flush thru this future, because the last flush has been delayed

    flush(last_flushed_future);
    m_lock->Unlock();
  } else {
    m_lock->Unlock();
    if (schedule_append) {
      // currently not enough buffers to flush, schedule it later
      schedule_append_task();
    } else {

      // object closed or overflowed or flushed, which means currently no
      // pending buffers to flush or we do not want to flush in this
      // ObjectRecorder

      cancel_append_task();
    }
  }

  // TODO: we should have flushed in append called above ???
  return (!m_object_closed && !m_overflowed &&
          m_size + m_pending_bytes >= m_soft_max_size);
}

void ObjectRecorder::flush(Context *on_safe) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << dendl;

  // we will flush it manually, so no need to flush it driven
  // by timer
  cancel_append_task();

  Future future;

  {
    Mutex::Locker locker(*m_lock);

    // if currently handling flush callback, wait so that
    // we notify in the correct order (since lock is dropped on
    // callback)
    if (m_in_flight_flushes) {

      // raced with rados callback, we are busy setting all flushed
      // futures to safe

      m_in_flight_flushes_cond.Wait(*(m_lock.get()));
    }

    // attach the flush to the most recent append
    if (!m_append_buffers.empty()) { // pending buffers
      future = Future(m_append_buffers.rbegin()->first);

      // flush those pending buffers
      flush_appends(true);
    } else if (!m_in_flight_appends.empty()) { // in-flight buffers
      AppendBuffers &append_buffers = m_in_flight_appends.rbegin()->second;
      assert(!append_buffers.empty());

      future = Future(append_buffers.rbegin()->first);
    }
  }

  if (future.is_valid()) {

    // now the future points to the last in-flight buffer, flush all the way
    // up to all previous futures

    future.flush(on_safe);
  } else {

    // no in-flight or pending buffers, we can finish this flush request
    // immediately

    on_safe->complete(0);
  }
}

void ObjectRecorder::flush(const FutureImplPtr &future) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << " flushing " << *future
                   << dendl;

  assert(m_lock->is_locked());

  if (future->get_flush_handler().get() != &m_flush_handler) {

    // if we don't own this future, re-issue the flush so that it hits the
    // correct journal object owner

    // the future belongs to a flush all the way up to all previous futures

    future->flush();

    return;
  } else if (future->is_flush_in_progress()) {
    return;
  }

  if (m_object_closed || m_overflowed) {
    return;
  }

  AppendBuffers::reverse_iterator r_it;
  for (r_it = m_append_buffers.rbegin(); r_it != m_append_buffers.rend();
       ++r_it) {
    if (r_it->first == future) {
      break;
    }
  }
  assert(r_it != m_append_buffers.rend());

  auto it = (++r_it).base();
  assert(it != m_append_buffers.end());
  ++it;

  AppendBuffers flush_buffers;

  // gather those buffers to flush
  flush_buffers.splice(flush_buffers.end(), m_append_buffers,
                       m_append_buffers.begin(), it);

  // construct a rados write op from those buffers and write
  send_appends(&flush_buffers);
}

void ObjectRecorder::claim_append_buffers(AppendBuffers *append_buffers) {
  ldout(m_cct, 20) << __func__ << ": " << m_oid << dendl;

  assert(m_lock->is_locked());
  assert(m_in_flight_tids.empty());
  assert(m_in_flight_appends.empty());
  assert(m_object_closed || m_overflowed);

  append_buffers->splice(append_buffers->end(), m_append_buffers,
                         m_append_buffers.begin(), m_append_buffers.end());
}

bool ObjectRecorder::close() {
  assert (m_lock->is_locked());

  ldout(m_cct, 20) << __func__ << ": " << m_oid << dendl;

  cancel_append_task();

  // flush all pending buffers
  flush_appends(true);

  assert(!m_object_closed);
  m_object_closed = true;
  return (m_in_flight_tids.empty() && !m_in_flight_flushes && !m_aio_scheduled);
}

void ObjectRecorder::handle_append_task() {

  // m_timer_lock and m_timer::lock are the same mutex, see Journaler::Threads::Threads
  assert(m_timer_lock.is_locked());

  m_append_task = NULL;

  Mutex::Locker locker(*m_lock);

  // timer driven flush, flush all pending buffers
  flush_appends(true);
}

void ObjectRecorder::cancel_append_task() {
  Mutex::Locker locker(m_timer_lock);

  if (m_append_task != NULL) {
    m_timer.cancel_event(m_append_task);

    m_append_task = NULL;
  }
}

void ObjectRecorder::schedule_append_task() {
  Mutex::Locker locker(m_timer_lock);

  if (m_append_task == NULL && m_flush_age > 0) {
    m_append_task = new C_AppendTask(this);

    m_timer.add_event_after(m_flush_age, m_append_task);
  }
}

bool ObjectRecorder::append(const AppendBuffer &append_buffer,
                            bool *schedule_append) {
  assert(m_lock->is_locked());

  bool flush_requested = false;

  if (!m_object_closed && !m_overflowed) {

    // attach flush handler to the future

    flush_requested = append_buffer.first->attach(&m_flush_handler);
  }

  m_append_buffers.push_back(append_buffer);
  m_pending_bytes += append_buffer.second.length();

  if (!flush_appends(false)) { // try to flush pending buffers

    // not enough buffers to send and we will schedule to flush it later

    *schedule_append = true;
  }

  // if it is true which means this buffer had been requested to flush or
  // in-progress of flush, we need to flush through this future
  return flush_requested;
}

bool ObjectRecorder::flush_appends(bool force) {
  assert(m_lock->is_locked());
  if (m_object_closed || m_overflowed) {
    return true;
  }

  // try to flush pending buffers

  if (m_append_buffers.empty() ||
      (!force &&
       m_size + m_pending_bytes < m_soft_max_size &&
       (m_flush_interval > 0 && m_append_buffers.size() < m_flush_interval) &&
       (m_flush_bytes > 0 && m_pending_bytes < m_flush_bytes))) {

    return false;
  }

  // send all pending buffers in a single rados op

  m_pending_bytes = 0;

  AppendBuffers append_buffers;
  append_buffers.swap(m_append_buffers);

  // send multiple buffers, i.e., entries, in a single rados op, after this the
  // buffers will be in state of FLUSH_STATE_IN_PROGRESS
  send_appends(&append_buffers);

  return true;
}

void ObjectRecorder::handle_append_flushed(uint64_t tid, int r) {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " tid=" << tid
                   << ", r=" << r << dendl;

  AppendBuffers append_buffers;

  {
    m_lock->Lock();
    auto tid_iter = m_in_flight_tids.find(tid);
    assert(tid_iter != m_in_flight_tids.end());
    m_in_flight_tids.erase(tid_iter);

    InFlightAppends::iterator iter = m_in_flight_appends.find(tid);

    if (r == -EOVERFLOW || m_overflowed) {

      // this write op overflowed

      if (iter != m_in_flight_appends.end()) {

        m_overflowed = true;
      } else {
        // must have seen an overflow on a previous append op

        assert(r == -EOVERFLOW && m_overflowed);
      }

      // notify of overflow once all in-flight ops are complete
      if (m_in_flight_tids.empty() && !m_aio_scheduled) {
        append_overflowed();
        notify_handler_unlock();
      } else {
        m_lock->Unlock();
      }

      return;
    }

    // an append succeeded, remove this append from in-flight appends

    assert(iter != m_in_flight_appends.end());
    append_buffers.swap(iter->second);
    assert(!append_buffers.empty());

    m_in_flight_appends.erase(iter);
    m_in_flight_flushes = true;
    m_lock->Unlock();
  }

  // Flag the associated futures as complete.
  for (AppendBuffers::iterator buf_it = append_buffers.begin();
       buf_it != append_buffers.end(); ++buf_it) {
    ldout(m_cct, 20) << __func__ << ": " << *buf_it->first << " marked safe"
                     << dendl;

    // this buffer has written to object safely, so set m_safe = true and
    // reset m_flush_handler to release ref
    buf_it->first->safe(r);
  }

  // wake up any flush requests that raced with a RADOS callback
  m_lock->Lock();
  m_in_flight_flushes = false;
  m_in_flight_flushes_cond.Signal();

  if (m_in_flight_appends.empty() && !m_aio_scheduled && m_object_closed) {
    // all remaining unsent appends should be redirected to new object
    notify_handler_unlock();
  } else {
    m_lock->Unlock();
  }
}

void ObjectRecorder::append_overflowed() {
  ldout(m_cct, 10) << __func__ << ": " << m_oid << " append overflowed"
                   << dendl;

  assert(m_lock->is_locked());
  assert(!m_in_flight_appends.empty());

  cancel_append_task();

  InFlightAppends in_flight_appends;
  in_flight_appends.swap(m_in_flight_appends);

  // collect the in-flight buffers and pending buffers together
  AppendBuffers restart_append_buffers;

  for (InFlightAppends::iterator it = in_flight_appends.begin();
       it != in_flight_appends.end(); ++it) {
    // each append may consist of multiple buffers

    restart_append_buffers.insert(restart_append_buffers.end(),
                                  it->second.begin(), it->second.end());
  }

  restart_append_buffers.splice(restart_append_buffers.end(),
                                m_append_buffers,
                                m_append_buffers.begin(),
                                m_append_buffers.end());

  // now all in-flight buffers and pending buffers are on the same list,
  // i.e., m_append_buffers, we will re-append it to a new object latter
  restart_append_buffers.swap(m_append_buffers);

  for (AppendBuffers::const_iterator it = m_append_buffers.begin();
       it != m_append_buffers.end(); ++it) {
    ldout(m_cct, 20) << __func__ << ": overflowed " << *it->first
                     << dendl;
    it->first->detach();
  }
}

void ObjectRecorder::send_appends(AppendBuffers *append_buffers) {
  assert(m_lock->is_locked());
  assert(!append_buffers->empty());

  for (AppendBuffers::iterator it = append_buffers->begin();
       it != append_buffers->end(); ++it) {
    ldout(m_cct, 20) << __func__ << ": flushing " << *it->first
                     << dendl;
    it->first->set_flush_in_progress();
    m_size += it->second.length();
  }

  m_pending_buffers.splice(m_pending_buffers.end(), *append_buffers,
                           append_buffers->begin(), append_buffers->end());
  if (!m_aio_scheduled) {
    m_op_work_queue->queue(new FunctionContext([this] (int r) {
        send_appends_aio();
    }));
    m_aio_scheduled = true;
  }
}

void ObjectRecorder::send_appends_aio() {
  AppendBuffers *append_buffers;
  uint64_t append_tid;
  {
    Mutex::Locker locker(*m_lock);
    append_tid = m_append_tid++;
    m_in_flight_tids.insert(append_tid);

    // safe to hold pointer outside lock until op is submitted
    append_buffers = &m_in_flight_appends[append_tid];
    append_buffers->swap(m_pending_buffers);
  }

  ldout(m_cct, 10) << __func__ << ": " << m_oid << " flushing journal tid="
                   << append_tid << dendl;

  C_AppendFlush *append_flush = new C_AppendFlush(this, append_tid);
  C_Gather *gather_ctx = new C_Gather(m_cct, append_flush);

  librados::ObjectWriteOperation op;

  // the already written size must less then m_soft_max_size
  client::guard_append(&op, m_soft_max_size);
  for (AppendBuffers::iterator it = append_buffers->begin();
       it != append_buffers->end(); ++it) {
    ldout(m_cct, 20) << __func__ << ": flushing " << *it->first
                     << dendl;
    op.append(it->second);
    op.set_op_flags2(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
  }

  librados::AioCompletion *rados_completion =
    librados::Rados::aio_create_completion(gather_ctx->new_sub(), nullptr,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();

  {
    m_lock->Lock();
    if (m_pending_buffers.empty()) {
      m_aio_scheduled = false;
      if (m_in_flight_appends.empty() && m_object_closed) {
        // all remaining unsent appends should be redirected to new object
        notify_handler_unlock();
      } else {
        m_lock->Unlock();
      }
    } else {
      // additional pending items -- reschedule
      m_op_work_queue->queue(new FunctionContext([this] (int r) {
          send_appends_aio();
        }));
      m_lock->Unlock();
    }
  }

  // allow append op to complete
  gather_ctx->activate();
}

void ObjectRecorder::notify_handler_unlock() {
  assert(m_lock->is_locked());
  if (m_object_closed) {
    m_lock->Unlock();
    m_handler->closed(this);
  } else {
    // TODO need to delay completion until after aio_notify completes
    m_lock->Unlock();
    m_handler->overflow(this);
  }
}

} // namespace journal
