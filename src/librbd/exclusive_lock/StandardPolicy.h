// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H

#include "librbd/exclusive_lock/Policy.h"

namespace librbd {

struct ImageCtx;

namespace exclusive_lock {

class StandardPolicy : public Policy {
public:
  StandardPolicy(ImageCtx *image_ctx) : m_image_ctx(image_ctx) {
  }

  // checked by librbd::AioImageRequestWQ::queue and librbd::lock_acquire
  virtual bool may_auto_request_lock() {
    return false;
  }

  // always return -EROFS, called by ImageWatcher<I>::handle_payload
  virtual int lock_requested(bool force);

private:
  ImageCtx *m_image_ctx;

};

} // namespace exclusive_lock
} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H
