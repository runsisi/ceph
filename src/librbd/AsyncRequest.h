// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_ASYNC_REQUEST_H
#define CEPH_LIBRBD_ASYNC_REQUEST_H

#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "include/compat.h"

namespace librbd {

class ImageCtx;

// pure virtual function:
// send, should_complete
// virtual function:
// filter_return_code, finish
template <typename ImageCtxT = ImageCtx>
class AsyncRequest
{
public:
  AsyncRequest(ImageCtxT &image_ctx, Context *on_finish);
  virtual ~AsyncRequest();

  void complete(int r) {
    // pure virtual
    if (should_complete(r)) {
      r = filter_return_code(r);
      finish_and_destroy(r);
    }
  }

  virtual void send() = 0;

  inline bool is_canceled() const {
    return m_canceled;
  }

  // called by ImageCtx::cancel_async_requests
  inline void cancel() {
    m_canceled = true;
  }

protected:
  ImageCtxT &m_image_ctx;

  librados::AioCompletion *create_callback_completion();
  Context *create_callback_context();
  // called in TrimRequest<I>::send_clean_boundary
  Context *create_async_callback_context();

  // called by AsyncRequest<T>::create_async_callback_context to queue
  // a context(created by create_callback_context) on m_image_ctx.op_work_queue
  // and other places:
  // object_map::InvalidateRequest
  // operation::ResizeRequest, SnapshotProtectRequest, SnapshotRemoveRequest,
  // SnapshotUnprotectRequest, TrimRequest
  void async_complete(int r);

  virtual bool should_complete(int r) = 0;
  virtual int filter_return_code(int r) const {
    return r;
  }

  // NOTE: temporary until converted to new state machine format
  virtual void finish_and_destroy(int r) {
    finish(r);
    delete this;
  }

  virtual void finish(int r) {
    // remove from m_image_ctx.async_requests
    finish_request();

    m_on_finish->complete(r);
  }

private:
  Context *m_on_finish;
  bool m_canceled;

  // will be pushed back of ImageCtx::async_requests
  typename xlist<AsyncRequest<ImageCtxT> *>::item m_xlist_item;

  // push ourself back of m_image_ctx.async_requests
  void start_request();
  // remove ourself from m_image_ctx.async_requests and complete the waiters
  void finish_request();
};

} // namespace librbd

extern template class librbd::AsyncRequest<librbd::ImageCtx>;

#endif //CEPH_LIBRBD_ASYNC_REQUEST_H
