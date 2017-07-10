// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_REQUEST_H

#include "include/int_types.h"
#include "librbd/AsyncRequest.h"

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

// pure virtual function:
// send
// virtual function:
// should_complete, filter_return_code
class Request : public AsyncRequest<> {
public:
  Request(ImageCtx &image_ctx, uint64_t snap_id, Context *on_finish)
    : AsyncRequest(image_ctx, on_finish), m_snap_id(snap_id),
      m_state(STATE_REQUEST)
  {
  }

  void send() override = 0;

protected:
  const uint64_t m_snap_id;

  bool should_complete(int r) override;
  int filter_return_code(int r) const override {
    // never propagate an error back to the caller
    return 0;
  }

  // did not override virtual void finish(int r), so complete() will
  // call finish() to remove ourself from m_image_ctx.async_requests and
  // complete the m_on_finish

  // NOTE: AsyncRequest::finish_request is not a virtual method, so Request::finish_request is
  // not an override of AsyncRequest::finish_request

  // only ResizeRequest and UpdateRequest override this method, and UpdateRequest only
  // print a debug message
  virtual void finish_request() {
  }

private:
  /**
   * <start> ---> STATE_REQUEST ---> <finish>
   *                   |                ^
   *                   v                |
   *            STATE_INVALIDATE -------/
   */
  enum State {
    STATE_REQUEST,
    STATE_INVALIDATE
  };

  State m_state;

  // called by object_map::Request::should_complete
  bool invalidate();
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_REQUEST_H
