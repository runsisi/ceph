// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H
#define CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H

#include "librbd/journal/Policy.h"

namespace librbd {

struct ImageCtx;

namespace journal {

class DisabledPolicy : public Policy {
public:
  // checked by Journal<I>::is_journal_appending
  bool append_disabled() const override {
    return true;
  }

  // checked by
  // AcquireRequest<I>::send_open_journal and
  // RefreshRequest<I>::send_v2_open_journal
  bool journal_disabled() const override {
    return true;
  }

  // called by
  // AcquireRequest<I>::handle_open_journal -> AcquireRequest<I>::send_allocate_journal_tag
  void allocate_tag_on_lock(Context *on_finish) override {
    ceph_abort();
  }
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H
