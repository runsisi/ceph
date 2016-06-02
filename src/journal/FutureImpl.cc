// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/FutureImpl.h"
#include "journal/Utils.h"

namespace journal {

// created by
// JournalRecorder::append
FutureImpl::FutureImpl(uint64_t tag_tid, uint64_t entry_tid,
                       uint64_t commit_tid)
  : RefCountedObject(NULL, 0), m_tag_tid(tag_tid), m_entry_tid(entry_tid),
    m_commit_tid(commit_tid),
    m_lock("FutureImpl::m_lock", false, false), m_safe(false),
    m_consistent(false), m_return_value(0), m_flush_state(FLUSH_STATE_NONE),
    m_consistent_ack(this) {
}

// called by
// JournalRecorder::append
void FutureImpl::init(const FutureImplPtr &prev_future) {
  // chain ourself to the prior future (if any) to that we known when the
  // journal is consistent
  if (prev_future) {
    m_prev_future = prev_future;

    // push back of FutureImpl::m_contexts if the previous future has not
    // completed yet then push m_consistent_ack to back of FutureImpl::m_contexts,
    // which means the previous future will set FutureImpl::m_consistent for us
    // when the previous future has completed, else complete this callback
    // directly, i.e., call m_consistent_ack.complete(m_return_value)
    m_prev_future->wait(&m_consistent_ack);
  } else {
    // this is the first future, no previous future to set m_consistent for us,
    // so set m_consistent = true for myself
    m_consistent_ack.complete(0);
  }
}

// called by
// Future::flush
// ObjectRecorder::flush(const FutureImplPtr), when future->m_flush_handler != object->m_flush_handler
void FutureImpl::flush(Context *on_safe) { // on_safe is a default parameter

  bool complete;

  // std::map<FlushHandlerPtr, FutureImplPtr>
  FlushHandlers flush_handlers;
  FutureImplPtr prev_future;

  {
    Mutex::Locker locker(m_lock);

    complete = (m_safe && m_consistent);

    if (!complete) {
      if (on_safe != nullptr) {
        m_contexts.push_back(on_safe);
      }

      // m_flush_state = FLUSH_STATE_REQUESTED, and insert {m_flush_handler, this}
      // into flush_handlers
      prev_future = prepare_flush(&flush_handlers, m_lock);
    }
  }

  // instruct prior futures to flush as well
  while (prev_future) {
    prev_future = prev_future->prepare_flush(&flush_handlers);
  }

  if (complete && on_safe != NULL) {

    // future completed

    on_safe->complete(m_return_value);
  } else if (!flush_handlers.empty()) {
    // attached to journal object -- instruct it to flush all entries through
    // this one.  possible to become detached while lock is released, so flush
    // will be re-requested by the object if it doesn't own the future
    for (auto &pair : flush_handlers) {
      // ObjectRecorder::FlushHandler::flush, i.e., object_recorder->flush(future),
      // a FlushHandler represents an ObjectRecorder
      pair.first->flush(pair.second);
    }
  }
}

// called by
// FutureImpl::flush
FutureImplPtr FutureImpl::prepare_flush(FlushHandlers *flush_handlers) {
  Mutex::Locker locker(m_lock);
  return prepare_flush(flush_handlers, m_lock);
}

FutureImplPtr FutureImpl::prepare_flush(FlushHandlers *flush_handlers,
                                        Mutex &lock) {
  assert(m_lock.is_locked());

  if (m_flush_state == FLUSH_STATE_NONE) {
    m_flush_state = FLUSH_STATE_REQUESTED;

    // m_flush_handler was set by FutureImpl::attach, which called by ObjectRecorder::append
    // it is a pointer point to ObjectRecorder::m_flush_handler
    if (m_flush_handler && flush_handlers->count(m_flush_handler) == 0) {
      // so ObjectRecorder::flush(const FutureImplPtr has:
      // future->get_flush_handler().get() == &m_flush_handler
      flush_handlers->insert({m_flush_handler, this});
    }
  }

  return m_prev_future;
}

// called by
// Future::wait
// FutureImpl::init
void FutureImpl::wait(Context *on_safe) {
  assert(on_safe != NULL);

  {
    Mutex::Locker locker(m_lock);

    if (!m_safe || !m_consistent) {
      m_contexts.push_back(on_safe);

      return;
    }
  }

  on_safe->complete(m_return_value);
}

bool FutureImpl::is_complete() const {
  Mutex::Locker locker(m_lock);
  return m_safe && m_consistent;
}

int FutureImpl::get_return_value() const {
  Mutex::Locker locker(m_lock);
  assert(m_safe && m_consistent);
  return m_return_value;
}

// called by
// ObjectRecorder::append
bool FutureImpl::attach(const FlushHandlerPtr &flush_handler) {
  Mutex::Locker locker(m_lock);

  assert(!m_flush_handler);
  m_flush_handler = flush_handler;

  return m_flush_state != FLUSH_STATE_NONE;
}

// called by
// ObjectRecorder::handle_append_flushed, which called by ObjectRecorder::C_AppendFlush::finish
// which created by ObjectRecorder::send_appends_aio
void FutureImpl::safe(int r) {
  m_lock.Lock();

  assert(!m_safe);

  m_safe = true;

  if (m_return_value == 0) {
    m_return_value = r;
  }

  m_flush_handler.reset();

  if (m_consistent) {

    // complete FutureImpl::m_contexts, which pushed back by
    // FutureImpl::wait, see Journal<I>::append_io_events
    finish_unlock();
  } else {
    m_lock.Unlock();
  }
}

// called by
// FutureImpl::C_ConsistentAck::complete, i.e., the previous
// futureimpl's FutureImpl::safe
void FutureImpl::consistent(int r) {
  m_lock.Lock();

  assert(!m_consistent);
  m_consistent = true;

  m_prev_future.reset();

  if (m_return_value == 0) {
    m_return_value = r;
  }

  if (m_safe) {

    // finish all contexts on m_contexts

    finish_unlock();
  } else {
    m_lock.Unlock();
  }
}

void FutureImpl::finish_unlock() {
  assert(m_lock.is_locked());

  assert(m_safe && m_consistent);

  Contexts contexts;
  contexts.swap(m_contexts);

  m_lock.Unlock();

  for (Contexts::iterator it = contexts.begin();
       it != contexts.end(); ++it) {
    (*it)->complete(m_return_value);
  }
}

std::ostream &operator<<(std::ostream &os, const FutureImpl &future) {
  os << "Future[tag_tid=" << future.m_tag_tid << ", "
     << "entry_tid=" << future.m_entry_tid << ", "
     << "commit_tid=" << future.m_commit_tid << "]";
  return os;
}

void intrusive_ptr_add_ref(FutureImpl::FlushHandler *p) {
  p->get();
}

void intrusive_ptr_release(FutureImpl::FlushHandler *p) {
  p->put();
}

} // namespace journal
