// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/StandardPolicy.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::StandardPolicy: "

namespace librbd {
namespace journal {

// called by AcquireRequest<I>::send_allocate_journal_tag after acquired
// exclusive lock of the image
void StandardPolicy::allocate_tag_on_lock(Context *on_finish) {
  assert(m_image_ctx->journal != nullptr);

  if (!m_image_ctx->journal->is_tag_owner()) {

    // we have acquired the exclusive lock, but we are not the primary
    // image, read/write io is not permitted until the administrator to
    // promote us to the primary image manually

    lderr(m_image_ctx->cct) << "local image not promoted" << dendl;
    m_image_ctx->op_work_queue->queue(on_finish, -EPERM);
    return;
  }

  // we have acquired the exclusive lock, and this image is the primary
  // image, so allocate a tag on the journal metadata object to identify
  // that we are the primary image now and previously the primary image
  // was also us
  m_image_ctx->journal->allocate_local_tag(on_finish);
}

} // namespace journal
} // namespace librbd
