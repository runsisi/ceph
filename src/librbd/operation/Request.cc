// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Request: "

namespace librbd {
namespace operation {

template <typename I>
Request<I>::Request(I &image_ctx, Context *on_finish, uint64_t journal_op_tid)
  : AsyncRequest<I>(image_ctx, on_finish), m_op_tid(journal_op_tid) {
}

template <typename I>
void Request<I>::send() {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.owner_lock.is_locked());

  // automatically create the event if we don't need to worry
  // about affecting concurrent IO ops
  if (can_affect_io() || !append_op_event()) {

    // 1) ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
    // affects concurrent IO ops, need to block and flush ImageCtx::async_ops, see:
    // ResizeRequest<I>::send_pre_block_writes,
    // SnapshotCreateRequest<I>::send_suspend_aio,
    // EnableFeaturesRequest<I>::handle_prepare_lock,
    // DisableFeaturesRequest<I>::handle_prepare_lock
    // or
    // 2) journaling not available currently now

    // for ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest,
    // will call Request<I>::append_op_event(T *request) to try to append journal Event,
    // other type of requests will send request directly
    send_op();
  }
}

// called by:
// ResizeRequest, SnapshotCreateRequest, and SnapshotRollbackRequest,
// EnableFeaturesRequest, DisableFeaturesRequest
template <typename I>
Context *Request<I>::create_context_finisher(int r) {
  // automatically commit the event if required (delete after commit)
  if (m_appended_op_event && !m_committed_op_event &&
      commit_op_event(r)) {

    // commit op event initiated, Request<I>::handle_commit_op_event will
    // be called after the op event committed

    return nullptr;
  }

  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  return util::create_context_callback<Request<I>, &Request<I>::finish>(this);
}

// called by librbd::AsyncRequest::complete
template <typename I>
void Request<I>::finish_and_destroy(int r) {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  // m_appended_op_event has been set by librbd::operation::Request::C_AppendOpEvent::finish,
  // now we are to

  // automatically commit the event if required (delete after commit)
  if (m_appended_op_event && !m_committed_op_event &&
      commit_op_event(r)) {

    // commit op event initiated, Request<I>::handle_commit_op_event will
    // be called after the op event committed

    return;
  }

  // finish(r);
  // delete this;
  AsyncRequest<I>::finish_and_destroy(r);
}

template <typename I>
void Request<I>::finish(int r) {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  // either we need appended the event or the event has committed
  assert(!m_appended_op_event || m_committed_op_event);

  // finish_request();
  // m_on_finish->complete(r);
  AsyncRequest<I>::finish(r);
}

// called by Request<I>::send if the op does not affect io, i.e.,
// can_affect_io() returns false, i.e., requests except:
// ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
template <typename I>
bool Request<I>::append_op_event() {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.owner_lock.is_locked());

  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  if (image_ctx.journal != nullptr &&
      image_ctx.journal->is_journal_appending()) {

    // STATE_READY

    // if the journal are opened by acquiring exclusive lock, then
    // ImageCtx::journal is set before the journal has opened, see
    // AcquireRequest<I>::send_open_journal

    // allocate op event tid and append the op event now
    append_op_event(util::create_context_callback<
      Request<I>, &Request<I>::handle_op_event_safe>(this));

    return true;
  }

  // journal not enabled or lirbd::Journal is not ready
  return false;
}

// called by
// Request<I>::create_context_finisher
// Request<I>::finish_and_destroy
template <typename I>
bool Request<I>::commit_op_event(int r) {
  I &image_ctx = this->m_image_ctx;

  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  // m_appended_op_event was set to true in Request<I>::replay_op_ready or
  // C_OpEventSafe::finish(r >= 0), see librbd/operation/Request.h

  if (!m_appended_op_event) {
    // actually, this check is no need, bc we are only be called
    // when m_appended_op_event is true
    return false;
  }

  assert(m_op_tid != 0);

  assert(!m_committed_op_event);
  m_committed_op_event = true;

  if (image_ctx.journal != nullptr &&
      image_ctx.journal->is_journal_appending()) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

    // ops will be canceled / completed before closing journal
    assert(image_ctx.journal->is_journal_ready());

    // C_CommitOpEvent::finish will call request->handle_commit_op_event
    image_ctx.journal->commit_op_event(m_op_tid, r,
                                       new C_CommitOpEvent(this, r));

    return true;
  }

  return false;
}

// called by Request<I>::C_CommitOpEvent::finish
template <typename I>
void Request<I>::handle_commit_op_event(int r, int original_ret_val) {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to commit op event to journal: " << cpp_strerror(r)
               << dendl;
  }

  if (original_ret_val < 0) {
    r = original_ret_val;
  }

  finish(r);
}

// called by librbd::operation::Request<I>::append_op_event(T *request)
template <typename I>
void Request<I>::replay_op_ready(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.owner_lock.is_locked());
  assert(image_ctx.snap_lock.is_locked());

  // see librbd::journal::ExecuteOp
  assert(m_op_tid != 0);

  m_appended_op_event = true;

  // will call Replay<I>::replay_op_ready eventually
  image_ctx.journal->replay_op_ready(
    m_op_tid, util::create_async_context_callback(image_ctx, on_safe));
}

// called by
// Request<I>::append_op_event()
// Request<I>::append_op_event(T *request)
template <typename I>
void Request<I>::append_op_event(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.owner_lock.is_locked());
  assert(image_ctx.snap_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // Journal::m_op_tid.inc()
  m_op_tid = image_ctx.journal->allocate_op_tid();

  // librbd::Journal must be STATE_READY
  // C_AppendOpEvent::finish will set request->m_appended_op_event = true and
  // call on_safe->complete(r)
  image_ctx.journal->append_op_event(
    m_op_tid, journal::EventEntry{create_event(m_op_tid)},
    new C_AppendOpEvent(this, on_safe));
}

// for requests except ResizeRequest, SnapshotCreateRequest,
// EnableFeaturesRequest, DisableFeaturesRequest
template <typename I>
void Request<I>::handle_op_event_safe(int r) {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to commit op event to journal: " << cpp_strerror(r)
               << dendl;

    // append op event to journal failed, finish it early
    this->finish(r);
    delete this;
  } else {

    // if can_affect_io() returns true, then Request<I>::send will call send_op() instead
    // of calling Request<I>::append_op_event(), i.e., this method will not be called

    assert(!can_affect_io());

    // haven't started the request state machine yet
    RWLock::RLocker owner_locker(image_ctx.owner_lock);

    // for requests except ResizeRequest, SnapshotCreateRequest,
    // EnableFeaturesRequest, DisableFeaturesRequest
    // in the end, the state machine will call this->complete
    send_op();
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::Request<librbd::ImageCtx>;
