// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_REQUEST_H
#define CEPH_LIBRBD_OPERATION_REQUEST_H

#include "librbd/AsyncRequest.h"
#include "include/Context.h"
#include "common/RWLock.h"
#include "librbd/Utils.h"
#include "librbd/Journal.h"

namespace librbd {

class ImageCtx;

namespace operation {

// pure virtual function:
// send_op, create_event, should_complete
// virtual function:
// can_affect_io, send, finish
template <typename ImageCtxT = ImageCtx>
class Request : public AsyncRequest<ImageCtxT> {
public:
  Request(ImageCtxT &image_ctx, Context *on_finish,
          uint64_t journal_op_tid = 0);

  void send();

protected:
  void finish(int r) override;

  // called by
  // Request<I>::send, for SnapshotCreateRequest/ResizeRequest/EnableFeaturesRequest/DisableFeaturesRequest
  //    i.e., send Op to block IO first then append Op event and resume the Op state machine
  // Request<I>::handle_op_event_safe, for other requests
  //    i.e., append Op event first then send Op to start the Op state machine
  virtual void send_op() = 0;

  // only ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
  // overrides this and return true
  virtual bool can_affect_io() const {
    return false;
  }

  virtual journal::Event create_event(uint64_t op_tid) const = 0;

  // T is a specific request type, can be:
  // ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
  // called by
  // DisableFeaturesRequest<I>::send_append_op_event
  // EnableFeaturesRequest<I>::send_append_op_event
  // ResizeRequest<I>::send_append_op_event
  // SnapshotCreateRequest<I>::send_append_op_event
  template <typename T, Context*(T::*MF)(int*)>
  bool append_op_event(T *request) { // "this"
    ImageCtxT &image_ctx = this->m_image_ctx;

    ceph_assert(can_affect_io());
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);

    if (image_ctx.journal != nullptr) {

      // journaling enabled, when the journal::Event is safe, the
      // callback MF, i.e., XxxRequest<I>::handle_append_op_event will
      // be called, see:
      // ResizeRequest<I>::handle_append_op_event
      // SnapshotCreateRequest<I>::handle_append_op_event
      // EnableFeaturesRequest<I>::handle_append_op_event
      // DisableFeaturesRequest<I>::handle_append_op_event

      if (image_ctx.journal->is_journal_replaying()) {
        // m_state == STATE_REPLAYING || STATE_FLUSHING_REPLAY ||
        // STATE_FLUSHING_RESTART || STATE_RESTARTING_REPLAY

        // this is an journaled Op we are replaying of, either by
        // Journal local replay or ImageReplayer, see Replay<I>::handle_event(XxxEvent)
        // do not append journal, setup op_event.on_op_finish_event and wait the
        // OpFinishEvent to drive the Op state machine to resume

        Context *ctx = util::create_context_callback<T, MF>(request);

        // call librbd::operation::Request<I>::replay_op_ready
        replay_op_ready(ctx);

        return true;
      } else if (image_ctx.journal->is_journal_appending()) {

        // Journal is in state of STATE_READY, -AND-
        // journal policy for append has not been disabled

        // NOTE: for local mirror image, the Journal has re-transit into
        // STATE_REPLAYING after the journal opened(which was in STATE_READY),
        // and the journal policy has disabled the appending, see ImageReplayer<I>::start_replay
        // and OpenLocalImageRequest/MirrorJournalPolicy

        Context *ctx = util::create_context_callback<T, MF>(request);

        // call Request<I>::append_op_event(Context *on_safe)
        // NOTE: XxxRequest<I>::handle_append_op_event is wrapped in the ctx
        append_op_event(ctx);

        return true;
      }
    }

    // journaling not enabled,

    return false;
  }

  bool append_op_event();

  // NOTE: append OpFinishEvent, the same behavior as finish_and_destroy,
  // -EXCEPT- for SnapshotCreate/Resize/EnableFeatures/DisableFeatures/SnapshotRollback,
  // those Op do not rely on AsyncRequest::complete and AsyncRequest::should_complete,
  // instead they have their own state machines
  // NOTE: temporary until converted to new state machine format
  Context *create_context_finisher(int r);
  // called by
  // librbd::AsyncRequest::complete
  // NOTE: append OpFinishEvent, the same behavior as create_context_finisher,
  // -EXCEPT- for other Ops, those Ops rely AsyncRequest::complete and AsyncRequest::should_complete
  // as their state machines
  void finish_and_destroy(int r) override;

private:
  // created by
  // Request<I>::append_op_event(Context *on_safe)
  struct C_AppendOpEvent : public Context {
    Request *request;
    Context *on_safe;
    C_AppendOpEvent(Request *request, Context *on_safe)
      : request(request), on_safe(on_safe) {
    }
    void finish(int r) override {
      if (r >= 0) {
        request->m_appended_op_event = true;
      }

      on_safe->complete(r);
    }
  };

  // created by
  // Request<I>::commit_op_event
  struct C_CommitOpEvent : public Context {
    Request *request;
    int ret_val;
    C_CommitOpEvent(Request *request, int ret_val)
      : request(request), ret_val(ret_val) {
    }
    void finish(int r) override {
      request->handle_commit_op_event(r, ret_val);
      delete request;
    }
  };

  uint64_t m_op_tid = 0;

  bool m_appended_op_event = false;
  bool m_committed_op_event = false;

  void replay_op_ready(Context *on_safe);
  void append_op_event(Context *on_safe);
  void handle_op_event_safe(int r);

  bool commit_op_event(int r);
  void handle_commit_op_event(int r, int original_ret_val);

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::Request<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_REQUEST_H
