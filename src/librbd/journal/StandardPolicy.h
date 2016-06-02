// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
#define CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H

#include "librbd/journal/Policy.h"

namespace librbd {

struct ImageCtx;

namespace journal {

template<typename ImageCtxT = ImageCtx>
class StandardPolicy : public Policy {
public:
  StandardPolicy(ImageCtxT *image_ctx) : m_image_ctx(image_ctx) {
  }

  // checked by Journal<I>::is_journal_appending
  virtual bool append_disabled() const {
    return false;
  }

  // checked by
  // AcquireRequest<I>::send_open_journal and
  // RefreshRequest<I>::send_v2_open_journal
  virtual bool journal_disabled() const {
    return false;
  }

  // allocate tag if we are the primary, else return -EPERM
  // called by
  // AcquireRequest<I>::handle_open_journal -> AcquireRequest<I>::send_allocate_journal_tag
  virtual void allocate_tag_on_lock(Context *on_finish);

private:
  ImageCtxT *m_image_ctx;
};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::StandardPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
