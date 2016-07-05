// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SnapshotCopyRequest.h"
#include "SnapshotCreateRequest.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "journal/Journaler.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SnapshotCopyRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

namespace {

template <typename I>
const std::string &get_snapshot_name(I *image_ctx, librados::snap_t snap_id) {
  // <snap_name, snap_id>
  auto snap_it = std::find_if(image_ctx->snap_ids.begin(),
                              image_ctx->snap_ids.end(),
                              [snap_id](
      const std::pair<std::string, librados::snap_t> &pair) {
    return pair.second == snap_id;
  });

  assert(snap_it != image_ctx->snap_ids.end());

  // snap_name
  return snap_it->first;
}

} // anonymous namespace

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
SnapshotCopyRequest<I>::SnapshotCopyRequest(I *local_image_ctx,
                                            I *remote_image_ctx,
                                            SnapMap *snap_map,
                                            Journaler *journaler,
                                            librbd::journal::MirrorPeerClientMeta *meta,
                                            ContextWQ *work_queue,
                                            Context *on_finish)
  : BaseRequest("rbd::mirror::image_sync::SnapshotCopyRequest",
		local_image_ctx->cct, on_finish),
    m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_snap_map(snap_map), m_journaler(journaler), m_client_meta(meta),
    m_work_queue(work_queue), m_snap_seqs(meta->snap_seqs),
    m_lock(unique_lock_name("SnapshotCopyRequest::m_lock", this)) {
  m_snap_map->clear();

  // snap ids ordered from oldest to newest
  m_remote_snap_ids.insert(remote_image_ctx->snaps.begin(),
                           remote_image_ctx->snaps.end());
  m_local_snap_ids.insert(local_image_ctx->snaps.begin(),
                          local_image_ctx->snaps.end());
}

template <typename I>
void SnapshotCopyRequest<I>::send() {
  librbd::parent_spec remote_parent_spec;

  // the remote image must has snapshots, because we have created
  // sync point in SyncPointCreateRequest<I>::send_create_snap

  // get the parent spec of the remote image's
  // snapshots(if deep-flatten feature is not enabled for the cloned
  // image, after flatten the parent spec will be different between
  // the HEAD and the snapshots)
  int r = validate_parent(m_remote_image_ctx, &remote_parent_spec);
  if (r < 0) {
    derr << ": remote image parent spec mismatch" << dendl;
    error(r);
    return;
  }

  // get local image's parent spec
  r = validate_parent(m_local_image_ctx, &m_local_parent_spec);
  if (r < 0) {
    derr << ": local image parent spec mismatch" << dendl;
    error(r);
    return;
  }

  // unprotect snapshots one by one
  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  dout(20) << dendl;

  m_canceled = true;
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_unprotect() {

  SnapIdSet::iterator snap_id_it = m_local_snap_ids.begin();

  if (m_prev_snap_id != CEPH_NOSNAP) {

    // the next snap id

    snap_id_it = m_local_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_local_snap_ids.end(); ++snap_id_it) {

    // iterate snapshots of local image

    librados::snap_t local_snap_id = *snap_id_it;

    m_local_image_ctx->snap_lock.get_read();

    bool local_unprotected;

    // find SnapInfo from ImageCtx::snap_info and get the protection status
    int r = m_local_image_ctx->is_snap_unprotected(local_snap_id,
                                                   &local_unprotected);
    if (r < 0) {
      derr << ": failed to retrieve local snap unprotect status: "
           << cpp_strerror(r) << dendl;
      m_local_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }

    m_local_image_ctx->snap_lock.put_read();

    if (local_unprotected) {

      // snap is already unprotected -- check next snap

      continue;
    }

    // local snapshot protected, check if we should unprotect it

    // if local snapshot is protected and (1) it isn't in our mapping
    // table, or (2) the remote snapshot isn't protected, unprotect it


    auto snap_seq_it = std::find_if(
      m_snap_seqs.begin(), m_snap_seqs.end(),
      [local_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == local_snap_id;
      });

    if (snap_seq_it != m_snap_seqs.end()) {

      // the local snap id is in m_snap_seqs

      m_remote_image_ctx->snap_lock.get_read();

      bool remote_unprotected;
      r = m_remote_image_ctx->is_snap_unprotected(snap_seq_it->first,
                                                  &remote_unprotected);
      if (r < 0) {
        derr << ": failed to retrieve remote snap unprotect status: "
             << cpp_strerror(r) << dendl;
        m_remote_image_ctx->snap_lock.put_read();
        finish(r);
        return;
      }

      m_remote_image_ctx->snap_lock.put_read();

      if (remote_unprotected) {
        // remote is unprotected -- unprotect local snap
        break;
      }
    } else {
      // remote snapshot doesn't exist -- unprotect local snap
      break;
    }
  }

  if (snap_id_it == m_local_snap_ids.end()) {

    // finished iterating all local snapshots
    // no local snapshots to unprotect

    m_prev_snap_id = CEPH_NOSNAP;

    // remove snapshot one by one
    send_snap_remove();

    return;
  }

  // need to unprotect local snapshot

  // note down the last unprotected local snapshot, used as the start
  // snapshot id of the next iteration
  m_prev_snap_id = *snap_id_it;

  m_snap_name = get_snapshot_name(m_local_image_ctx, m_prev_snap_id);

  dout(20) << ": "
           << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_unprotect>(
      this);

  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);

  m_local_image_ctx->operations->execute_snap_unprotect(m_snap_name.c_str(),
                                                        ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_unprotect(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to unprotect snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (handle_cancellation())
  {
    return;
  }

  // unprotect the next snapshot start from m_prev_snap_id
  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_remove() {
  SnapIdSet::iterator snap_id_it = m_local_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_local_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_local_snap_ids.end(); ++snap_id_it) {
    librados::snap_t local_snap_id = *snap_id_it;

    // if the local snapshot isn't in our mapping table, remove it
    auto snap_seq_it = std::find_if(
      m_snap_seqs.begin(), m_snap_seqs.end(),
      [local_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == local_snap_id;
      });

    if (snap_seq_it == m_snap_seqs.end()) {
      break;
    }
  }

  if (snap_id_it == m_local_snap_ids.end()) {
    // no local snapshots to delete
    m_prev_snap_id = CEPH_NOSNAP;

    // create snapshot one by one
    send_snap_create();

    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_local_image_ctx, m_prev_snap_id);

  dout(20) << ": "
           << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_remove>(
      this);

  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);

  m_local_image_ctx->operations->execute_snap_remove(m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_remove(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to remove snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (handle_cancellation())
  {
    return;
  }

  // remove the next snapshot
  send_snap_remove();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_create() {
  SnapIdSet::iterator snap_id_it = m_remote_snap_ids.begin();

  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_remote_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_remote_snap_ids.end(); ++snap_id_it) {
    librados::snap_t remote_snap_id = *snap_id_it;

    // if the remote snapshot isn't in our mapping table, create it
    if (m_snap_seqs.find(remote_snap_id) == m_snap_seqs.end()) {
      break;
    }
  }

  if (snap_id_it == m_remote_snap_ids.end()) {
    // no remote snapshots to create
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_protect();
    return;
  }

  m_prev_snap_id = *snap_id_it;

  // get snap name by snap id
  m_snap_name = get_snapshot_name(m_remote_image_ctx, m_prev_snap_id);

  m_remote_image_ctx->snap_lock.get_read();

  auto snap_info_it = m_remote_image_ctx->snap_info.find(m_prev_snap_id);

  if (snap_info_it == m_remote_image_ctx->snap_info.end()) {
    m_remote_image_ctx->snap_lock.put_read();
    derr << ": failed to retrieve remote snap info: " << m_snap_name
         << dendl;
    finish(-ENOENT);
    return;
  }

  uint64_t size = snap_info_it->second.size;
  librbd::parent_spec parent_spec;
  uint64_t parent_overlap = 0;

  if (snap_info_it->second.parent.spec.pool_id != -1) {
    parent_spec = m_local_parent_spec;
    parent_overlap = snap_info_it->second.parent.overlap;
  }

  m_remote_image_ctx->snap_lock.put_read();

  dout(20) << ": "
           << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << ", "
           << "size=" << size << ", "
           << "parent_info=["
           << "pool_id=" << parent_spec.pool_id << ", "
           << "image_id=" << parent_spec.image_id << ", "
           << "snap_id=" << parent_spec.snap_id << ", "
           << "overlap=" << parent_overlap << "]" << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_create>(
      this);
  SnapshotCreateRequest<I> *req = SnapshotCreateRequest<I>::create(
    m_local_image_ctx, m_snap_name, size, parent_spec, parent_overlap, ctx);

  req->send();
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_create(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (handle_cancellation())
  {
    return;
  }

  assert(m_prev_snap_id != CEPH_NOSNAP);

  auto snap_it = m_local_image_ctx->snap_ids.find(m_snap_name);
  assert(snap_it != m_local_image_ctx->snap_ids.end());
  librados::snap_t local_snap_id = snap_it->second;

  dout(20) << ": mapping remote snap id " << m_prev_snap_id << " to "
           << local_snap_id << dendl;

  m_snap_seqs[m_prev_snap_id] = local_snap_id;

  // create the next snapshot
  send_snap_create();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_protect() {
  SnapIdSet::iterator snap_id_it = m_remote_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_remote_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_remote_snap_ids.end(); ++snap_id_it) {
    librados::snap_t remote_snap_id = *snap_id_it;

    m_remote_image_ctx->snap_lock.get_read();
    bool remote_protected;
    int r = m_remote_image_ctx->is_snap_protected(remote_snap_id,
                                                  &remote_protected);
    if (r < 0) {
      derr << ": failed to retrieve remote snap protect status: "
           << cpp_strerror(r) << dendl;
      m_remote_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_remote_image_ctx->snap_lock.put_read();

    if (!remote_protected) {
      // snap is not protected -- check next snap
      continue;
    }

    // if local snapshot is not protected, protect it
    auto snap_seq_it = m_snap_seqs.find(remote_snap_id);
    assert(snap_seq_it != m_snap_seqs.end());

    m_local_image_ctx->snap_lock.get_read();
    bool local_protected;
    r = m_local_image_ctx->is_snap_protected(snap_seq_it->second,
                                             &local_protected);
    if (r < 0) {
      derr << ": failed to retrieve local snap protect status: "
           << cpp_strerror(r) << dendl;
      m_local_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_local_image_ctx->snap_lock.put_read();

    if (!local_protected) {
      break;
    }
  }

  if (snap_id_it == m_remote_snap_ids.end()) {
    // no local snapshots to protect
    m_prev_snap_id = CEPH_NOSNAP;
    send_update_client();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_remote_image_ctx, m_prev_snap_id);

  dout(20) << ": "
           << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_protect>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_protect(m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_protect(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to protect snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  send_snap_protect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_update_client() {
  dout(20) << dendl;

  compute_snap_map();

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.snap_seqs = m_snap_seqs;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_update_client>(
      this);
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_update_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }
  if (handle_cancellation())
  {
    return;
  }

  m_client_meta->snap_seqs = m_snap_seqs;

  finish(0);
}

template <typename I>
bool SnapshotCopyRequest<I>::handle_cancellation() {
  {
    Mutex::Locker locker(m_lock);
    if (!m_canceled) {
      return false;
    }
  }
  dout(10) << ": snapshot copy canceled" << dendl;
  finish(-ECANCELED);
  return true;
}

template <typename I>
void SnapshotCopyRequest<I>::error(int r) {
  dout(20) << ": r=" << r << dendl;

  m_work_queue->queue(new FunctionContext([this, r](int r1) { finish(r); }));
}

template <typename I>
void SnapshotCopyRequest<I>::compute_snap_map() {
  SnapIds local_snap_ids;
  for (auto &pair : m_snap_seqs) {
    local_snap_ids.reserve(1 + local_snap_ids.size());
    local_snap_ids.insert(local_snap_ids.begin(), pair.second);
    m_snap_map->insert(std::make_pair(pair.first, local_snap_ids));
  }
}

// if deep-flatten feature is not enabled for the cloned image, then
// after flatten the HEAD and snapshots will have different parent
// specs, see cls_rbd::remove_parent
template <typename I>
int SnapshotCopyRequest<I>::validate_parent(I *image_ctx,
                                            librbd::parent_spec *spec) {
  RWLock::RLocker owner_locker(image_ctx->owner_lock);
  RWLock::RLocker snap_locker(image_ctx->snap_lock);

  // ensure remote image's parent specs are still consistent

  *spec = image_ctx->parent_md.spec;

  for (auto &snap_info_pair : image_ctx->snap_info) {

    // iterate all parent specs of the snapshots of this image

    // parent spec of the snapshot, if the deep-flatten feature is not
    // enabled for the clone image, then the parent spec of the snapshot
    // will be different than the HEAD after flatten, the HEAD should not
    // have parent, while the snapshots still have parent, see cls_rbd::remove_parent
    // and http://ceph-users.ceph.narkive.com/H1A9Hryx/how-to-understand-deep-flatten-implementation
    auto &parent_spec = snap_info_pair.second.parent.spec;

    if (parent_spec.pool_id == -1) {

      // this specific snapshot has no parent

      continue;
    } else if (spec->pool_id == -1) {

      // this specific snapshot has parent, while the HEAD has no parent,
      // because once the 'spec' variable is updated, then the spec->pool_id
      // can never be -1 again

      *spec = parent_spec;

      continue;
    }

    // this snapshot has parent, and the HEAD/last recorded snapshot has parent,
    // check if the two parent specs are the same

    if (*spec != parent_spec) {
      return -EINVAL;
    }
  }

  return 0;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SnapshotCopyRequest<librbd::ImageCtx>;
