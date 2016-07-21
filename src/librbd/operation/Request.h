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

  // call send_op() based on can_affect_io(), if this op affect the io,
  // i.e., this is ResizeRequest or SnapshotCreateRequest, or if we can
  // not journal it now, i.e., ImageCtx->journal is nullptr is the journal
  // currently is replaying
  virtual void send();

protected:
  // call commit_op_event() based on can_affect_io()
  virtual void finish(int r) override;

  // called by send and Request<I>::handle_op_event_safe
  virtual void send_op() = 0;

  // only ResizeRequest and SnapshotCreateRequest override this and return true
  virtual bool can_affect_io() const {
    return false;
  }

  virtual journal::Event create_event(uint64_t op_tid) const = 0;

  // T is a specific request type, either ResizeRequest or SnapshotCreateRequest
  template <typename T, Context*(T::*MF)(int*)>
  bool append_op_event(T *request) {
    ImageCtxT &image_ctx = this->m_image_ctx;

    // only ResizeRequest or SnapshotCreateRequest overrides the can_affect_io
    // and returns true
    assert(can_affect_io());

    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);

    if (image_ctx.journal != NULL) {

      // the T::MF callback should be either ResizeRequest<I>::handle_append_op_event
      // or SnapshotCreateRequest<I>::handle_append_op_event
      Context *ctx = util::create_context_callback<T, MF>(request);

      // TODO: !journal->is_journal_ready() ???
      if (image_ctx.journal->is_journal_replaying()) {

        // will call Request<I>::replay_op_ready, which calls Replay<I>::replay_op_ready eventually
        replay_op_ready(ctx);
      } else {

        // librbd::Journal must be STATE_READY

        append_op_event(ctx);
      }

      return true;
    }

    return false;
  }

  bool append_op_event();
  void commit_op_event(int r);

  // NOTE: temporary until converted to new state machine format
  Context *create_context_finisher() {
    return util::create_context_callback<
      Request<ImageCtxT>, &Request<ImageCtxT>::finish>(this);
  }

private:
  // used by Request<I>::append_op_event(Context*) as a callback of
  // journal append
  struct C_OpEventSafe : public Context {
    Request *request;
    Context *on_safe;
    C_OpEventSafe(Request *request, Context *on_safe)
      : request(request), on_safe(on_safe) {
    }

    virtual void finish(int r) override {
      if (r >= 0) {
        request->m_appended_op_event = true;
      }

      on_safe->complete(r);
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

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::Request<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_REQUEST_H
