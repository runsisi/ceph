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

  // if this type of request affect the io, i.e., this is ResizeRequest, SnapshotCreateRequest,
  // EnableFeaturesRequest, DisableFeaturesRequest, or if we can not journal it now, i.e.,
  // ImageCtx->journal is nullptr or the journal currently is replaying, we call send_op, else
  // we call append_op_event and then call send_op when the journalled Event is safe
  virtual void send();

protected:
  // call commit_op_event() based on can_affect_io()
  virtual void finish(int r) override;

  // called by send and Request<I>::handle_op_event_safe
  virtual void send_op() = 0;

  // only ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
  // overrides this and return true
  virtual bool can_affect_io() const {
    return false;
  }

  virtual journal::Event create_event(uint64_t op_tid) const = 0;

  // T is a specific request type, can be:
  // ResizeRequest, SnapshotCreateRequest, EnableFeaturesRequest, DisableFeaturesRequest
  template <typename T, Context*(T::*MF)(int*)>
  bool append_op_event(T *request) {
    ImageCtxT &image_ctx = this->m_image_ctx;

    // only ResizeRequest, SnapshotCreateRequest EnableFeaturesRequest, DisableFeaturesRequest
    // overrides the can_affect_io and returns true
    assert(can_affect_io());

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

        // Journal<I>::m_state is in one of:
        // STATE_REPLAYING, STATE_FLUSHING_REPLAY, STATE_FLUSHING_RESTART, STATE_RESTARTING_REPLAY

        Context *ctx = util::create_context_callback<T, MF>(request);

        replay_op_ready(ctx);

        return true;
      } else if (image_ctx.journal->is_journal_appending()) {
        Context *ctx = util::create_context_callback<T, MF>(request);

        // call Request<I>::append_op_event(Context *on_safe)
        // NOTE: XxxRequest<I>::handle_append_op_event is wrapped in the ctx
        append_op_event(ctx);

        return true;
      }
    }

    return false;
  }

  bool append_op_event();

  // NOTE: temporary until converted to new state machine format
  Context *create_context_finisher(int r);
  virtual void finish_and_destroy(int r) override;

private:
  // used by Request<I>::append_op_event(Context *on_safe)
  struct C_AppendOpEvent : public Context {
    Request *request;
    Context *on_safe;
    C_AppendOpEvent(Request *request, Context *on_safe)
      : request(request), on_safe(on_safe) {
    }

    virtual void finish(int r) override {
      if (r >= 0) {
        request->m_appended_op_event = true;
      }

      on_safe->complete(r);
    }
  };

  // used by Request<I>::commit_op_event
  struct C_CommitOpEvent : public Context {
    Request *request;
    int ret_val;
    C_CommitOpEvent(Request *request, int ret_val)
      : request(request), ret_val(ret_val) {
    }

    virtual void finish(int r) override {
      request->handle_commit_op_event(r, ret_val);
      delete request;
    }
  };

  // only ResizeRequest and SnapshotCreateRequest will create the request
  // with non-zero journal_op_tid constructed, see Operations<I>::execute_resize
  // and Operations<I>::execute_snap_create
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
