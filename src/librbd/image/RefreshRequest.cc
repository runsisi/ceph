// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RefreshRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/journal/Policy.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;
using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshRequest<I>::RefreshRequest(I &image_ctx, bool acquiring_lock,
                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_acquiring_lock(acquiring_lock),
    m_on_finish(create_async_context_callback(m_image_ctx, on_finish)),
    m_error_result(0), m_flush_aio(false), m_exclusive_lock(nullptr),
    m_object_map(nullptr), m_journal(nullptr), m_refresh_parent(nullptr) {
}

template <typename I>
RefreshRequest<I>::~RefreshRequest() {
  // these require state machine to close
  assert(m_exclusive_lock == nullptr);
  assert(m_object_map == nullptr);
  assert(m_journal == nullptr);
  assert(m_refresh_parent == nullptr);
  assert(!m_blocked_writes);
}

template <typename I>
void RefreshRequest<I>::send() {
  if (m_image_ctx.old_format) {
    send_v1_read_header();
  } else {
    send_v2_get_mutable_metadata();
  }
}

template <typename I>
void RefreshRequest<I>::send_v1_read_header() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  op.read(0, 0, nullptr, nullptr);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_read_header>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_read_header(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  rbd_obj_header_ondisk v1_header;
  if (*result < 0) {
    return m_on_finish;
  } else if (m_out_bl.length() < sizeof(v1_header)) {
    lderr(cct) << "v1 header too small" << dendl;
    *result = -EIO;
    return m_on_finish;
  } else if (memcmp(RBD_HEADER_TEXT, m_out_bl.c_str(),
                    sizeof(RBD_HEADER_TEXT)) != 0) {
    lderr(cct) << "unrecognized v1 header" << dendl;
    *result = -ENXIO;
    return m_on_finish;
  }

  memcpy(&v1_header, m_out_bl.c_str(), sizeof(v1_header));
  m_order = v1_header.options.order;
  m_size = v1_header.image_size;
  m_object_prefix = v1_header.block_name;
  send_v1_get_snapshots();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_get_snapshots() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::old_snapshot_list_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": " << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::old_snapshot_list_finish(
      &it, &m_snap_names, &m_snap_sizes, &m_snapc);
  }

  if (*result < 0) {
    lderr(cct) << "failed to retrieve v1 snapshots: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  if (!m_snapc.is_valid()) {
    lderr(cct) << "v1 image snap context is invalid" << dendl;
    *result = -EIO;
    return m_on_finish;
  }

  //m_snap_namespaces = {m_snap_names.size(), cls::rbd::UserSnapshotNamespace()};
  m_snap_namespaces = std::vector
			      <cls::rbd::SnapshotNamespace>(
					    m_snap_names.size(),
					    cls::rbd::UserSnapshotNamespace());

  send_v1_get_locks();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_get_locks() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v1_get_locks>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_get_locks(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  // If EOPNOTSUPP, treat image as if there are no locks (we can't
  // query them).
  if (*result == -EOPNOTSUPP) {
    *result = 0;
  } else if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    ClsLockType lock_type;
    *result = rados::cls::lock::get_lock_info_finish(&it, &m_lockers,
                                                     &lock_type, &m_lock_tag);
    if (*result == 0) {
      m_exclusive_locked = (lock_type == LOCK_EXCLUSIVE);
    }
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve locks: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v1_apply();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v1_apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // ensure we are not in a rados callback when applying updates
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v1_apply>(this);
  m_image_ctx.op_work_queue->queue(ctx, 0);
}

template <typename I>
Context *RefreshRequest<I>::handle_v1_apply(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  apply();
  return send_flush_aio();
}

template <typename I>
void RefreshRequest<I>::send_v2_get_mutable_metadata() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.snap_id;
  }

  bool read_only = m_image_ctx.read_only || snap_id != CEPH_NOSNAP;

  librados::ObjectReadOperation op;
  // size, features, snapcontext, parent and lock type
  // `read_only` flag is used to compute incompatible features between
  // rbd client and server
  cls_client::get_mutable_metadata_start(&op, read_only);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_mutable_metadata>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_mutable_metadata(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();

    // m_snapc: <snapid_t seq, vector<snapid_t> snaps>
    *result = cls_client::get_mutable_metadata_finish(&it, &m_size, &m_features,
                                                      &m_incompatible_features,
                                                      &m_lockers,
                                                      &m_exclusive_locked,
                                                      &m_lock_tag, &m_snapc,
                                                      &m_parent_md);
  }
  if (*result < 0) {
    lderr(cct) << "failed to retrieve mutable metadata: "
               << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  uint64_t unsupported = m_incompatible_features & ~RBD_FEATURES_ALL;
  if (unsupported != 0ULL) {
    lderr(cct) << "Image uses unsupported features: " << unsupported << dendl;
    *result = -ENOSYS;
    return m_on_finish;
  }

  if (!m_snapc.is_valid()) {
    lderr(cct) << "image snap context is invalid!" << dendl;
    *result = -EIO;
    return m_on_finish;
  }

  if (m_acquiring_lock && (m_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {

    // m_acquiring_lock means currently we have acquired the exclusive lock,
    // and we are in refreshing caused by acquiring exclusive lock, see
    // AcquireRequest<I>::handle_lock

    ldout(cct, 5) << "ignoring dynamically disabled exclusive lock" << dendl;

    m_features |= RBD_FEATURE_EXCLUSIVE_LOCK;

    // used by RefreshRequest<I>::send_flush_aio to return -ERESTART
    m_incomplete_update = true;
  }

  send_v2_get_flags();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_flags() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_flags_start(&op, m_snapc.snaps);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_flags>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_flags(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();

    cls_client::get_flags_finish(&it, &m_flags, m_snapc.snaps, &m_snap_flags);
  }

  if (*result == -EOPNOTSUPP) {
    // Older OSD doesn't support RBD flags, need to assume the worst
    *result = 0;

    ldout(cct, 10) << "OSD does not support RBD flags, disabling object map "
                   << "optimizations" << dendl;

    m_flags = RBD_FLAG_OBJECT_MAP_INVALID;
    if ((m_features & RBD_FEATURE_FAST_DIFF) != 0) {
      m_flags |= RBD_FLAG_FAST_DIFF_INVALID;
    }

    std::vector<uint64_t> default_flags(m_snapc.snaps.size(), m_flags);

    m_snap_flags = std::move(default_flags);
  } else if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;

    send_v2_get_mutable_metadata();

    return nullptr;
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve flags: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_group();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_group() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::image_get_group_start(&op);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_group>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_group(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    cls_client::image_get_group_finish(&it, &m_group_spec);
  }
  if (*result == -EOPNOTSUPP) {
    // Older OSD doesn't support RBD groups
    *result = 0;
    ldout(cct, 10) << "OSD does not support consistency groups" << dendl;
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve group: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_snapshots();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_snapshots() {
  if (m_snapc.snaps.empty()) {
    m_snap_names.clear();
    m_snap_namespaces.clear();
    m_snap_sizes.clear();
    m_snap_parents.clear();
    m_snap_protection.clear();

    send_v2_refresh_parent();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_list_start(&op, m_snapc.snaps);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_snapshots>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_snapshots(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::snapshot_list_finish(&it, m_snapc.snaps,
                                               &m_snap_names,
					       &m_snap_sizes,
                                               &m_snap_parents,
                                               &m_snap_protection);
  }

  if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;

    // m_snapc.snaps says it is not empty, but here we got no snapshots, so
    // we need to refresh the mutable metadata again

    send_v2_get_mutable_metadata();

    return nullptr;
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve snapshots: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_get_snap_namespaces();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_get_snap_namespaces() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;

  cls_client::snap_namespace_list_start(&op, m_snapc.snaps);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *comp = create_rados_ack_callback<
    klass, &klass::handle_v2_get_snap_namespaces>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op,
                                         &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_get_snap_namespaces(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *result = cls_client::snap_namespace_list_finish(&it, m_snapc.snaps,
						     &m_snap_namespaces);
  }

  if (*result == -ENOENT) {
    ldout(cct, 10) << "out-of-sync snapshot state detected" << dendl;

    send_v2_get_mutable_metadata();
    return nullptr;
  } else if (*result == -EOPNOTSUPP) {
    m_snap_namespaces = std::vector
				<cls::rbd::SnapshotNamespace>(
					     m_snap_names.size(),
					     cls::rbd::UserSnapshotNamespace());
    // Ignore it means no snap namespaces are available
  } else if (*result < 0) {
    lderr(cct) << "failed to retrieve snapshots: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_v2_refresh_parent();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_refresh_parent() {
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);

    // <parent_spec, overlap>
    parent_info parent_md;
    // get parent info of this image(maybe snapshot of the image)
    int r = get_parent_info(m_image_ctx.snap_id, &parent_md);
    if (r < 0 ||
        RefreshParentRequest<I>::is_refresh_required(m_image_ctx, parent_md)) {

      // if r < 0, then the parent_md will get a default parent spec, which
      // means no parent

      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 10) << this << " " << __func__ << dendl;

      using klass = RefreshRequest<I>;
      Context *ctx = create_context_callback<
        klass, &klass::handle_v2_refresh_parent>(this);

      m_refresh_parent = RefreshParentRequest<I>::create(
        m_image_ctx, parent_md, ctx);
    }
  }

  if (m_refresh_parent != nullptr) {

    // need to refresh the parent image first

    m_refresh_parent->send();
  } else {
    send_v2_init_exclusive_lock();
  }
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to refresh parent image: " << cpp_strerror(*result)
               << dendl;

    // save error code to RefreshRequest<I>::m_error_result
    save_result(result);

    // call RefreshRequest<I>::apply asynchronously
    send_v2_apply();

    return nullptr;
  }

  send_v2_init_exclusive_lock();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_init_exclusive_lock() {
  if ((m_features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0 ||
      m_image_ctx.read_only || !m_image_ctx.snap_name.empty() ||
      m_image_ctx.exclusive_lock != nullptr) {

    // 1) exclusive lock disabled, or 2) read only/snapshot,
    // or 3) exclusive lock is not null

    send_v2_open_object_map();

    return;
  }

  // need to create the exclusive_lock instance

  // create exclusive_lock and block io, the journal and object_map will be
  // created and opened by librbd::exclusive_lock::AcquireRequest,
  // see librbd::exclusive_lock::AcquireRequest<I>::apply

  // implies exclusive lock dynamically enabled or image open in-progress

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // TODO need safe shut down
  m_exclusive_lock = m_image_ctx.create_exclusive_lock();

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_init_exclusive_lock>(this);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  // block io, will unblock until the exclusive lock acquired, see
  // ExclusiveLock<I>::handle_acquire_lock
  m_exclusive_lock->init(m_features, ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_init_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize exclusive lock: "
               << cpp_strerror(*result) << dendl;
    save_result(result);
  }

  // if the exclusive_lock is dynamically enabled or the first time we
  // open the image, then we only have to create the exclusive_lock instance
  // here, and the object_map and journal instances will be created and
  // opened until exclusive_lock is acquired

  // call RefreshRequest<I>::apply asynchronously
  send_v2_apply();

  return nullptr;
}

// exclusive lock does not needed(disabled or image is read-only/snap),
// or is not null
template <typename I>
void RefreshRequest<I>::send_v2_open_journal() {
  bool journal_disabled = (
    (m_features & RBD_FEATURE_JOURNALING) == 0 ||
     m_image_ctx.read_only ||
     !m_image_ctx.snap_name.empty() ||
     m_image_ctx.journal != nullptr ||
     m_image_ctx.exclusive_lock == nullptr ||
     !m_image_ctx.exclusive_lock->is_lock_owner());

  bool journal_disabled_by_policy;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

    journal_disabled_by_policy = (
      !journal_disabled &&
      m_image_ctx.get_journal_policy()->journal_disabled());
  }

  if (journal_disabled || journal_disabled_by_policy) {

    // journal dynamically enabled -- doesn't own exclusive lock
    if ((m_features & RBD_FEATURE_JOURNALING) != 0 &&
        !journal_disabled_by_policy &&
        m_image_ctx.exclusive_lock != nullptr &&
        m_image_ctx.journal == nullptr) {

      // journal dynamically enabled but we are not the lock owner

      m_image_ctx.aio_work_queue->set_require_lock_on_read();
    }

    // journal disabled or is not null, or we are not lock owner, maybe
    // we need to close the journal, so try to block writes if the
    // journal is dynamically disabled

    send_v2_block_writes();

    return;
  }

  // need to create the journal instance

  // journal dynamically enabled, and we are the exclusive lock owner

  // implies journal dynamically enabled since ExclusiveLock will init
  // the journal upon acquiring the lock
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_open_journal>(this);

  // TODO need safe close
  m_journal = m_image_ctx.create_journal();

  m_journal->open(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_open_journal(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to initialize journal: " << cpp_strerror(*result)
               << dendl;
    save_result(result);
  }

  // journal opened, will not block writes becoz we are not to disable journaling

  send_v2_block_writes();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_block_writes() {
  bool disabled_journaling = false;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

    disabled_journaling = ((m_features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0 &&
                           (m_features & RBD_FEATURE_JOURNALING) == 0 &&
                           m_image_ctx.journal != nullptr);
  }

  if (!disabled_journaling) {

    // non-dynamically disabled journal, no need to block writes

    // call RefreshRequest<I>::apply asynchronously
    send_v2_apply();
    return;
  }

  // only journal dynamically disabled need to block writes, if we
  // disable exclusive lock dynamically then ReleaseRequest will
  // do its own block writes, see ReleaseRequest<I>::send_block_writes

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // we need to block writes temporarily to avoid in-flight journal
  // writes
  // this is a debug only flag
  m_blocked_writes = true;

  Context *ctx = create_context_callback<
    RefreshRequest<I>, &RefreshRequest<I>::handle_v2_block_writes>(this);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  m_image_ctx.aio_work_queue->block_writes(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_block_writes(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result)
               << dendl;

    save_result(result);
  }

  // call RefreshRequest<I>::apply asynchronously
  send_v2_apply();

  return nullptr;
}

// called by
// RefreshRequest<I>::send_v2_init_exclusive_lock, with no need to create
// (and init) the exclusive_lock instance
template <typename I>
void RefreshRequest<I>::send_v2_open_object_map() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0 ||
      m_image_ctx.object_map != nullptr ||
      (m_image_ctx.snap_name.empty() &&
       (m_image_ctx.read_only ||
        m_image_ctx.exclusive_lock == nullptr ||
        !m_image_ctx.exclusive_lock->is_lock_owner()))) {

    // object map disabled or not null, or: HEAD image and not own exclusive lock

    send_v2_open_journal();
    return;
  }

  // need to create the object_map instance

  // implies object map dynamically enabled or image open in-progress
  // since SetSnapRequest loads the object map for a snapshot and
  // ExclusiveLock loads the object map for HEAD
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  if (m_image_ctx.snap_name.empty()) {
    m_object_map = m_image_ctx.create_object_map(CEPH_NOSNAP);
  } else {
    for (size_t snap_idx = 0; snap_idx < m_snap_names.size(); ++snap_idx) {
      if (m_snap_names[snap_idx] == m_image_ctx.snap_name) {
        m_object_map = m_image_ctx.create_object_map(
          m_snapc.snaps[snap_idx].val);
        break;
      }
    }

    if (m_object_map == nullptr) {
      lderr(cct) << "failed to locate snapshot: " << m_image_ctx.snap_name
                 << dendl;

      send_v2_open_journal();
      return;
    }
  }

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_open_object_map>(this);

  m_object_map->open(ctx);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_open_object_map(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to open object map: " << cpp_strerror(*result)
               << dendl;
    delete m_object_map;
    m_object_map = nullptr;
  }

  send_v2_open_journal();

  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_v2_apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // ensure we are not in a rados callback when applying updates
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_apply>(this);

  m_image_ctx.op_work_queue->queue(ctx, 0);
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_apply(int *result) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 10) << this << " " << __func__ << dendl;

  // refresh finished, now update ImageCtx
  apply();

  // start to do the cleanup, i.e., shutdown exclusive lock, close
  // object_map and journal
  return send_v2_finalize_refresh_parent();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_finalize_refresh_parent() {
  if (m_refresh_parent == nullptr) {

    // check if we need to shutdown the exclusive lock (dynamically disabled)

    return send_v2_shut_down_exclusive_lock();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_finalize_refresh_parent>(this);

  m_refresh_parent->finalize(ctx);

  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_finalize_refresh_parent(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(m_refresh_parent != nullptr);
  delete m_refresh_parent;
  m_refresh_parent = nullptr;

  return send_v2_shut_down_exclusive_lock();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_shut_down_exclusive_lock() {

  // NOTE: m_exclusive_lock, m_journal, m_object_map have been swapped by
  // RefreshRequest<I>::apply before we get here, and the nullity of
  // these member variables can be used to determine if the features have
  // been disabled/enabled dynamically

  if (m_exclusive_lock == nullptr) {

    // exclusive lock did not disabled dynamically, no need to shutdown

    return send_v2_close_journal();
  }

  // exclusive lock dynamically disabled, see RefreshRequest<I>::apply

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // exclusive lock feature was dynamically disabled. in-flight IO will be
  // flushed and in-flight requests will be canceled before releasing lock
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_shut_down_exclusive_lock>(this);

  // release exclusive lock: including cancel op requests, close journal,
  // close object_map, see ReleaseRequest<I>::send_close_journal and others
  m_exclusive_lock->shut_down(ctx);

  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_shut_down_exclusive_lock(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to shut down exclusive lock: "
               << cpp_strerror(*result) << dendl;
    save_result(result);
  }

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);

    assert(m_image_ctx.exclusive_lock == nullptr);
  }

  assert(m_exclusive_lock != nullptr);
  delete m_exclusive_lock;
  m_exclusive_lock = nullptr;

  return send_v2_close_journal();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_close_journal() {
  if (m_journal == nullptr) {
    return send_v2_close_object_map();
  }

  // journal dynamically disabled, see RefreshRequest<I>::apply

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // journal feature was dynamically disabled
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_close_journal>(this);

  m_journal->close(ctx);

  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_close_journal(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    save_result(result);
    lderr(cct) << "failed to close journal: " << cpp_strerror(*result)
               << dendl;
  }

  assert(m_journal != nullptr);
  delete m_journal;
  m_journal = nullptr;

  assert(m_blocked_writes);
  m_blocked_writes = false;

  // journal dynamically disabled, and we have blocked the writes in
  // RefreshRequest<I>::send_v2_block_writes
  m_image_ctx.aio_work_queue->unblock_writes();

  return send_v2_close_object_map();
}

template <typename I>
Context *RefreshRequest<I>::send_v2_close_object_map() {
  if (m_object_map == nullptr) {
    return send_flush_aio();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // object map was dynamically disabled
  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_v2_close_object_map>(this);

  m_object_map->close(ctx);
  return nullptr;
}

template <typename I>
Context *RefreshRequest<I>::handle_v2_close_object_map(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(*result == 0);
  assert(m_object_map != nullptr);
  delete m_object_map;
  m_object_map = nullptr;

  return send_flush_aio();
}

template <typename I>
Context *RefreshRequest<I>::send_flush_aio() {
  if (m_incomplete_update && m_error_result == 0) {

    // m_incomplete_update is set in RefreshRequest<I>::handle_v2_get_mutable_metadata
    // when we have acquired the exclusive lock while the refreshed image
    // features turns out it does not support exclusive lock, see
    // AcquireRequest<I>::handle_lock

    // if this was a partial refresh, notify ImageState
    m_error_result = -ERESTART;
  }

  if (m_flush_aio) {

    // m_flush_aio is set in RefreshRequest<I>::apply if new snapshot added

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << dendl;

    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);

    using klass = RefreshRequest<I>;
    Context *ctx = create_context_callback<
      klass, &klass::handle_flush_aio>(this);

    // flush ObjectCacher and ImageCtx::async_ops
    m_image_ctx.flush(ctx);

    return nullptr;
  } else if (m_error_result < 0) {
    // propagate saved error back to caller
    Context *ctx = create_context_callback<
      RefreshRequest<I>, &RefreshRequest<I>::handle_error>(this);

    m_image_ctx.op_work_queue->queue(ctx, 0);
    return nullptr;
  }

  return m_on_finish;
}

template <typename I>
Context *RefreshRequest<I>::handle_flush_aio(int *result) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to flush pending AIO: " << cpp_strerror(*result)
               << dendl;
  }

  return handle_error(result);
}

template <typename I>
Context *RefreshRequest<I>::handle_error(int *result) {
  if (m_error_result < 0) {
    *result = m_error_result;

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;
  }

  return m_on_finish;
}

// called by
// RefreshRequest<I>::handle_v2_apply
template <typename I>
void RefreshRequest<I>::apply() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker md_locker(m_image_ctx.md_lock);

  {
    Mutex::Locker cache_locker(m_image_ctx.cache_lock);
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::WLocker parent_locker(m_image_ctx.parent_lock);

    // --------------------------------------------------------
    // to update mutable metadata
    // --------------------------------------------------------

    m_image_ctx.size = m_size;
    m_image_ctx.lockers = m_lockers;
    m_image_ctx.lock_tag = m_lock_tag;
    m_image_ctx.exclusive_locked = m_exclusive_locked;

    if (m_image_ctx.old_format) {
      m_image_ctx.order = m_order;
      m_image_ctx.features = 0;
      m_image_ctx.flags = 0;
      m_image_ctx.object_prefix = std::move(m_object_prefix);
      m_image_ctx.init_layout();
    } else {
      m_image_ctx.features = m_features;
      m_image_ctx.flags = m_flags;
      m_image_ctx.group_spec = m_group_spec;
      m_image_ctx.parent_md = m_parent_md;
    }

    // --------------------------------------------------------
    // to update snapshot related fields
    // --------------------------------------------------------

    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {

      // check if new snapshot added

      std::vector<librados::snap_t>::const_iterator it = std::find(
        m_image_ctx.snaps.begin(), m_image_ctx.snaps.end(),
        m_snapc.snaps[i].val);

      if (it == m_image_ctx.snaps.end()) {

        // new snapshot added, it does not exist in previous refresh

        // will be tested by RefreshRequest<I>::send_flush_aio
        m_flush_aio = true;

        ldout(cct, 20) << "new snapshot id=" << m_snapc.snaps[i].val
                       << " name=" << m_snap_names[i]
                       << " size=" << m_snap_sizes[i]
                       << dendl;
      }
    }

    m_image_ctx.snaps.clear();
    m_image_ctx.snap_info.clear();
    m_image_ctx.snap_ids.clear();

    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      uint64_t flags = m_image_ctx.old_format ? 0 : m_snap_flags[i];
      uint8_t protection_status = m_image_ctx.old_format ?
        static_cast<uint8_t>(RBD_PROTECTION_STATUS_UNPROTECTED) :
        m_snap_protection[i];

      parent_info parent;
      if (!m_image_ctx.old_format) {
        parent = m_snap_parents[i];
      }

      // add to ImageCtx::snaps, ImageCtx::snap_info, ImageCtx::snap_ids
      m_image_ctx.add_snap(m_snap_names[i], m_snap_namespaces[i], m_snapc.snaps[i].val,
                           m_snap_sizes[i], parent, protection_status, flags);
    }

    m_image_ctx.snapc = m_snapc;

    if (m_image_ctx.snap_id != CEPH_NOSNAP &&
        m_image_ctx.get_snap_id(m_image_ctx.snap_name) != m_image_ctx.snap_id) {
      lderr(cct) << "tried to read from a snapshot that no longer exists: "
                 << m_image_ctx.snap_name << dendl;

      // the only place where the ImageCtx::snap_exists can be set to false
      m_image_ctx.snap_exists = false;
    }

    // --------------------------------------------------------
    // to update m_image_ctx.parent
    // --------------------------------------------------------
    if (m_refresh_parent != nullptr) {
      m_refresh_parent->apply();
    }

    // --------------------------------------------------------
    // to update m_image_ctx.snapc
    // --------------------------------------------------------
    m_image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(m_image_ctx.snapc.seq,
                                                        m_image_ctx.snaps);

    // --------------------------------------------------------
    // to update the exclusive_lock, object_map, journal instances
    // --------------------------------------------------------

    // handle dynamically enabled / disabled features
    if (m_image_ctx.exclusive_lock != nullptr &&
        !m_image_ctx.test_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                   m_image_ctx.snap_lock)) {
      // disabling exclusive lock will automatically handle closing
      // object map and journaling

      assert(m_exclusive_lock == nullptr);

      // ExclusiveLock<I>::handle_shutdown_released will set
      // m_image_ctx.exclusive_lock to NULL
      m_exclusive_lock = m_image_ctx.exclusive_lock;
    } else {

      // exclusive lock already enabled or dynamically enabled

      if (m_exclusive_lock != nullptr) {

        // dynamically enabled exclusive lock

        assert(m_image_ctx.exclusive_lock == nullptr);

        std::swap(m_exclusive_lock, m_image_ctx.exclusive_lock);
      }

      if (!m_image_ctx.test_features(RBD_FEATURE_JOURNALING,
                                     m_image_ctx.snap_lock)) {

        // journaling disabled

        if (m_image_ctx.journal != nullptr) {

          // journaling dynamically disabled

          m_image_ctx.aio_work_queue->clear_require_lock_on_read();
        }

        std::swap(m_journal, m_image_ctx.journal);
      } else if (m_journal != nullptr) {

        // journaling dynamically enabled

        std::swap(m_journal, m_image_ctx.journal);
      }

      if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                     m_image_ctx.snap_lock) ||
          m_object_map != nullptr) {
        std::swap(m_object_map, m_image_ctx.object_map);
      }
    }
  }
}

template <typename I>
int RefreshRequest<I>::get_parent_info(uint64_t snap_id,
                                       parent_info *parent_md) {
  if (snap_id == CEPH_NOSNAP) {
    *parent_md = m_parent_md;
    return 0;
  } else {
    for (size_t i = 0; i < m_snapc.snaps.size(); ++i) {
      if (m_snapc.snaps[i].val == snap_id) {
        *parent_md = m_snap_parents[i];
        return 0;
      }
    }
  }
  return -ENOENT;
}

} // namespace image
} // namespace librbd

template class librbd::image::RefreshRequest<librbd::ImageCtx>;
