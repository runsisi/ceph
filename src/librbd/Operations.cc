// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Operations.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/journal/DisabledPolicy.h"
#include "librbd/journal/StandardPolicy.h"
#include "librbd/operation/DisableFeaturesRequest.h"
#include "librbd/operation/EnableFeaturesRequest.h"
#include "librbd/operation/FlattenRequest.h"
#include "librbd/operation/MetadataRemoveRequest.h"
#include "librbd/operation/MetadataSetRequest.h"
#include "librbd/operation/ObjectMapIterate.h"
#include "librbd/operation/RebuildObjectMapRequest.h"
#include "librbd/operation/RenameRequest.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "librbd/operation/SnapshotProtectRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "librbd/operation/SnapshotRenameRequest.h"
#include "librbd/operation/SnapshotRollbackRequest.h"
#include "librbd/operation/SnapshotUnprotectRequest.h"
#include "librbd/operation/SnapshotLimitRequest.h"
#include <set>
#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Operations: "

namespace librbd {

namespace {

// used by the 10 operations, notify the update then after the notification
// acked by the peers then call the user callback
template <typename I>
struct C_NotifyUpdate : public Context {
  I &image_ctx;
  Context *on_finish;
  bool notified = false;

  C_NotifyUpdate(I &image_ctx, Context *on_finish)
    : image_ctx(image_ctx), on_finish(on_finish) {
  }

  void complete(int r) override {
    CephContext *cct = image_ctx.cct;

    if (notified) {

      // has notified the header update

      if (r == -ETIMEDOUT) {
        // don't fail the op if a peer fails to get the update notification
        lderr(cct) << "update notification timed-out" << dendl;
        r = 0;
      } else if (r == -ENOENT) {
        // don't fail if header is missing (e.g. v1 image rename)
        ldout(cct, 5) << "update notification on missing header" << dendl;
        r = 0;
      } else if (r < 0) {
        lderr(cct) << "update notification failed: " << cpp_strerror(r)
                   << dendl;
      }

      // complete the user callback
      Context::complete(r);
      return;
    }

    if (r < 0) {
      // op failed -- no need to send update notification
      Context::complete(r);
      return;
    }

    // notify first, then to complete the ctx

    notified = true;

    image_ctx.notify_update(this);
  }
  void finish(int r) override {
    on_finish->complete(r);
  }
};

template <typename I>
struct C_InvokeAsyncRequest : public Context {
  /**
   * @verbatim
   *
   *               <start>
   *                  |
   *    . . . . . .   |   . . . . . . . . . . . . . . . . . .
   *    .         .   |   .                                 .
   *    .         v   v   v                                 .
   *    .       REFRESH_IMAGE (skip if not needed)          .
   *    .             |                                     .
   *    .             v                                     .
   *    .       ACQUIRE_LOCK (skip if exclusive lock        .
   *    .             |       disabled or has lock)         .
   *    .             |                                     .
   *    .   /--------/ \--------\   . . . . . . . . . . . . .
   *    .   |                   |   .
   *    .   v                   v   .
   *  LOCAL_REQUEST       REMOTE_REQUEST
   *        |                   |
   *        |                   |
   *        \--------\ /--------/
   *                  |
   *                  v
   *              <finish>
   *
   * @endverbatim
   */

  I &image_ctx;
  std::string request_type;
  bool permit_snapshot;
  boost::function<void(Context*)> local;
  boost::function<void(Context*)> remote;
  std::set<int> filter_error_codes;
  Context *on_finish;
  bool request_lock = false;

  C_InvokeAsyncRequest(I &image_ctx, const std::string& request_type,
                       bool permit_snapshot,
                       const boost::function<void(Context*)>& local,
                       const boost::function<void(Context*)>& remote,
                       const std::set<int> &filter_error_codes,
                       Context *on_finish)
    : image_ctx(image_ctx), request_type(request_type),
      permit_snapshot(permit_snapshot), local(local), remote(remote),
      filter_error_codes(filter_error_codes), on_finish(on_finish) {
  }

  void send() {

    // start the whole process, i.e., refresh -> acquire lock -> call local/remote

    send_refresh_image();
  }

  // called by
  // send, i.e., above
  // handle_remote_request
  // handle_local_request
  void send_refresh_image() {
    if (!image_ctx.state->is_refresh_required()) {
      send_acquire_exclusive_lock();
      return;
    }

    // need to refresh

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_context_callback<
      C_InvokeAsyncRequest<I>,
      &C_InvokeAsyncRequest<I>::handle_refresh_image>(this);

    image_ctx.state->refresh(ctx);
  }

  void handle_refresh_image(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    send_acquire_exclusive_lock();
  }

  void send_acquire_exclusive_lock() {
    // context can complete before owner_lock is unlocked
    RWLock &owner_lock(image_ctx.owner_lock);

    owner_lock.get_read();

    image_ctx.snap_lock.get_read();

    if (image_ctx.read_only ||
        (!permit_snapshot && image_ctx.snap_id != CEPH_NOSNAP)) {
      // release lock in case the complete(-EROFS) called below finished
      // too quick
      image_ctx.snap_lock.put_read();

      owner_lock.put_read();

      complete(-EROFS);
      return;
    }

    image_ctx.snap_lock.put_read();

    if (image_ctx.exclusive_lock == nullptr) {

      // exclusive lock not enabled

      send_local_request();

      owner_lock.put_read();
      return;
    } else if (image_ctx.image_watcher == nullptr) {

      // ImageCtx::image_watcher is set in ImageCtx::register_watch and
      // deleted in ImageCtx::shutdown
      // we always registered the image watcher if we have opened
      // the image without read-only, see OpenRequest<I>::send_register_watch

      // TODO: ImageCtx::image_watcher can be nullptr only after shutdown
      // or the image is opened with read-only, so we do not need this check ???

      owner_lock.put_read();
      complete(-EROFS);
      return;
    }

    // image_ctx.exclusive_lock is not nullptr

    int r;
    if (image_ctx.exclusive_lock->is_lock_owner() &&
        image_ctx.exclusive_lock->accept_requests(&r)) {

      // currently we are lock owner and can accept requests

      send_local_request();

      owner_lock.put_read();
      return;
    }

    // currently we are not the lock owner, so lock it before we can do
    // some modification

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
        C_InvokeAsyncRequest<I>,
        &C_InvokeAsyncRequest<I>::handle_acquire_exclusive_lock>(this));

    if (request_lock) { // was set by C_InvokeAsyncRequest::handle_remote_request
      // current lock owner doesn't support op -- try to perform
      // the action locally
      request_lock = false;

      image_ctx.exclusive_lock->acquire_lock(ctx);
    } else {
      image_ctx.exclusive_lock->try_acquire_lock(ctx);
    }

    owner_lock.put_read();
  }

  void handle_acquire_exclusive_lock(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r < 0) {
      complete(-EROFS);
      return;
    }

    // context can complete before owner_lock is unlocked
    RWLock &owner_lock(image_ctx.owner_lock);
    owner_lock.get_read();

    if (image_ctx.exclusive_lock->is_lock_owner()) {

      // call local function object with user callback set to
      // C_InvokeAsyncRequest<I>::handle_local_request
      send_local_request();

      owner_lock.put_read();
      return;
    }

    // try to lock exclusive lock failed, the remote locker is still alive,
    // see AcquireRequest<I>::handle_get_watchers, we do not persist on get the
    // lock, we are good if we are not the owner of the exclusive lock,
    // we will notify the remote lock owner to do the request for us

    // let the remote lock owner to do the request instead of locally,
    // call remote function object with user callback set to
    // C_InvokeAsyncRequest<I>::handle_remote_request
    send_remote_request();

    owner_lock.put_read();
  }

  void send_remote_request() {
    assert(image_ctx.owner_lock.is_locked());

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_context_callback<
      C_InvokeAsyncRequest<I>, &C_InvokeAsyncRequest<I>::handle_remote_request>(
        this);

    remote(ctx);
  }

  void handle_remote_request(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r == -EOPNOTSUPP) {
      ldout(cct, 5) << request_type << " not supported by current lock owner"
                    << dendl;

      request_lock = true;

      send_refresh_image();
      return;
    } else if (r != -ETIMEDOUT && r != -ERESTART) {
      image_ctx.state->handle_update_notification();

      complete(r);

      return;
    }

    // r == -ETIMEDOUT || r == -ERESTART, i.e. non-fatal error,
    // restart the whole process

    ldout(cct, 5) << request_type << " timed out notifying lock owner"
                  << dendl;

    // start over again, i.e., refresh -> acquire lock -> call local/remote

    send_refresh_image();
  }

  void send_local_request() {
    assert(image_ctx.owner_lock.is_locked());

    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << dendl;

    Context *ctx = util::create_async_context_callback(
      image_ctx, util::create_context_callback<
        C_InvokeAsyncRequest<I>,
        &C_InvokeAsyncRequest<I>::handle_local_request>(this));

    local(ctx);
  }

  void handle_local_request(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << __func__ << ": r=" << r << dendl;

    if (r == -ERESTART) {

      // start over again, i.e., refresh -> acquire lock -> call local/remote

      send_refresh_image();
      return;
    }

    complete(r);
  }

  void finish(int r) override {
    if (filter_error_codes.count(r) != 0) {
      r = 0;
    }

    // the user callback
    on_finish->complete(r);
  }
};

// called by
// librbd/operation/ObjectMapIterate.cc:object_map_action
template <typename I>
bool needs_invalidate(I& image_ctx, uint64_t object_no,
		     uint8_t current_state, uint8_t new_state) {
  if ( (current_state == OBJECT_EXISTS ||
	current_state == OBJECT_EXISTS_CLEAN) &&
       (new_state == OBJECT_NONEXISTENT ||
	new_state == OBJECT_PENDING)) {

    // return false means no need to invalidate the object map

    return false;
  }

  return true;
}

} // anonymous namespace

template <typename I>
Operations<I>::Operations(I &image_ctx)
  : m_image_ctx(image_ctx), m_async_request_seq(0) {
}

template <typename I>
int Operations<I>::flatten(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);

    if (m_image_ctx.parent_md.spec.pool_id == -1) {
      lderr(cct) << "image has no parent" << dendl;
      return -EINVAL;
    }
  }

  uint64_t request_id = ++m_async_request_seq;

  r = invoke_async_request("flatten", false,
                           boost::bind(&Operations<I>::execute_flatten, this,
                                       boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher<I>::notify_flatten,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx), _1));

  if (r < 0 && r != -EINVAL) {
    return r;
  }

  ldout(cct, 20) << "flatten finished" << dendl;

  return 0;
}

// called by
// librbd::Operations<I>::flatten
// ImageWatcher<I>::handle_payload(const FlattenPayload)
// librbd/journal/Replay.cc:ExecuteOp::execute(const journal::FlattenEvent)
template <typename I>
void Operations<I>::execute_flatten(ProgressContext &prog_ctx,
                                    Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "flatten" << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.snap_lock.get_read();
  m_image_ctx.parent_lock.get_read();

  // can't flatten a non-clone
  if (m_image_ctx.parent_md.spec.pool_id == -1) {
    lderr(cct) << "image has no parent" << dendl;
    m_image_ctx.parent_lock.put_read();
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EINVAL);
    return;
  }

  if (m_image_ctx.snap_id != CEPH_NOSNAP) {
    lderr(cct) << "snapshots cannot be flattened" << dendl;
    m_image_ctx.parent_lock.put_read();
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EROFS);
    return;
  }

  ::SnapContext snapc = m_image_ctx.snapc;

  assert(m_image_ctx.parent != NULL);

  uint64_t overlap;
  int r = m_image_ctx.get_parent_overlap(CEPH_NOSNAP, &overlap);
  assert(r == 0);

  assert(overlap <= m_image_ctx.size);

  uint64_t object_size = m_image_ctx.get_object_size();
  uint64_t  overlap_objects = Striper::get_num_objects(m_image_ctx.layout,
                                                       overlap);

  m_image_ctx.parent_lock.put_read();
  m_image_ctx.snap_lock.put_read();

  operation::FlattenRequest<I> *req = new operation::FlattenRequest<I>(
    m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), object_size,
    overlap_objects, snapc, prog_ctx);

  req->send();
}

// called by
// librbd::Image::rebuild_object_map
// librbd::rbd_rebuild_object_map
template <typename I>
int Operations<I>::rebuild_object_map(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "rebuild_object_map" << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  uint64_t request_id = ++m_async_request_seq;

  r = invoke_async_request("rebuild object map", true,
                           boost::bind(&Operations<I>::execute_rebuild_object_map,
                                       this, boost::ref(prog_ctx), _1),
                           boost::bind(&ImageWatcher<I>::notify_rebuild_object_map,
                                       m_image_ctx.image_watcher, request_id,
                                       boost::ref(prog_ctx), _1));

  ldout(cct, 10) << "rebuild object map finished" << dendl;

  if (r < 0) {
    return r;
  }

  return 0;
}

// called by
// librbd::Operations<I>::rebuild_object_map
// ImageWatcher<I>::handle_payload(const RebuildObjectMapPayload)
template <typename I>
void Operations<I>::execute_rebuild_object_map(ProgressContext &prog_ctx,
                                               Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    lderr(cct) << "image must support object-map feature" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  operation::RebuildObjectMapRequest<I> *req =
    new operation::RebuildObjectMapRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), prog_ctx);

  req->send();
}

// called by
// librbd::Image::check_object_map
template <typename I>
int Operations<I>::check_object_map(ProgressContext &prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  r = invoke_async_request("check object map", true,
                           boost::bind(&Operations<I>::check_object_map, this,
                                       boost::ref(prog_ctx), _1),
			   [] (Context *c) { c->complete(-EOPNOTSUPP); });

  return r;
}

// called by
// librbd::Operations<I>::check_object_map(ProgressContext &prog_ctx, Context *on_finish)
template <typename I>
void Operations<I>::object_map_iterate(ProgressContext &prog_ctx,
				       operation::ObjectIterateWork<I> handle_mismatch,
				       Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    on_finish->complete(-EINVAL);
    return;
  }

  operation::ObjectMapIterateRequest<I> *req =
    new operation::ObjectMapIterateRequest<I>(m_image_ctx, on_finish,
					      prog_ctx, handle_mismatch);

  req->send();
}

// called by
// librbd::Operations<I>::check_object_map(ProgressContext &prog_ctx)
template <typename I>
void Operations<I>::check_object_map(ProgressContext &prog_ctx,
				     Context *on_finish) {
  object_map_iterate(prog_ctx, needs_invalidate, on_finish);
}

template <typename I>
int Operations<I>::rename(const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dstname
                << dendl;

  int r = librbd::detect_format(m_image_ctx.md_ctx, dstname, NULL, NULL);
  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error checking for existing image called "
               << dstname << ":" << cpp_strerror(r) << dendl;
    return r;
  }

  if (r == 0) {
    lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
    return -EEXIST;
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("rename", true,
                             boost::bind(&Operations<I>::execute_rename, this,
                                         dstname, _1),
                             boost::bind(&ImageWatcher<I>::notify_rename,
                                         m_image_ctx.image_watcher, dstname,
                                         _1));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);

    C_SaferCond cond_ctx;

    execute_rename(dstname, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.set_image_name(dstname);

  return 0;
}

template <typename I>
void Operations<I>::execute_rename(const std::string &dest_name,
                                   Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  m_image_ctx.snap_lock.get_read();

  if (m_image_ctx.name == dest_name) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EEXIST);
    return;
  }

  m_image_ctx.snap_lock.put_read();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": dest_name=" << dest_name
                << dendl;

  if (m_image_ctx.old_format) {
    // unregister watch before and register back after rename
    on_finish = new C_NotifyUpdate<I>(m_image_ctx, on_finish);
    on_finish = new FunctionContext([this, on_finish](int r) {
        if (m_image_ctx.old_format) {
          m_image_ctx.image_watcher->set_oid(m_image_ctx.header_oid);
        }
	m_image_ctx.image_watcher->register_watch(on_finish);
      });
    on_finish = new FunctionContext([this, dest_name, on_finish](int r) {
	operation::RenameRequest<I> *req = new operation::RenameRequest<I>(
	  m_image_ctx, on_finish, dest_name);
	req->send();
      });

    m_image_ctx.image_watcher->unregister_watch(on_finish);

    return;
  }

  operation::RenameRequest<I> *req = new operation::RenameRequest<I>(
    m_image_ctx, on_finish, dest_name);

  req->send();
}

// rbd_resize2 allow user to determine if the image can be shrinked
// rbd_resize and rbd_resize_with_progress always allow the image to be
// shrinked
template <typename I>
int Operations<I>::resize(uint64_t size, bool allow_shrink, ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.snap_lock.get_read();
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;
  m_image_ctx.snap_lock.put_read();

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP) &&
      !ObjectMap<>::is_compatible(m_image_ctx.layout, size)) {
    lderr(cct) << "New size not compatible with object map" << dendl;
    return -EINVAL;
  }

  uint64_t request_id = ++m_async_request_seq;

  r = invoke_async_request("resize", false,
                           boost::bind(&Operations<I>::execute_resize, this,
                                       size, allow_shrink, boost::ref(prog_ctx), _1, 0),
                           boost::bind(&ImageWatcher<I>::notify_resize,
                                       m_image_ctx.image_watcher, request_id,
                                       size, allow_shrink, boost::ref(prog_ctx), _1));

  m_image_ctx.perfcounter->inc(l_librbd_resize);

  ldout(cct, 2) << "resize finished" << dendl;

  return r;
}

// called by
// librbd::Operations<I>::resize, always has journal_op_tid set to zero
// librbd::journal::ExecuteOp::execute(journal::ResizeEvent), which has journal_op_tid set to non-zero
template <typename I>
void Operations<I>::execute_resize(uint64_t size, bool allow_shrink, ProgressContext &prog_ctx,
                                   Context *on_finish,
                                   uint64_t journal_op_tid) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.snap_lock.get_read();

  ldout(cct, 5) << this << " " << __func__ << ": "
                << "size=" << m_image_ctx.size << ", "
                << "new_size=" << size << dendl;

  if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EROFS);
    return;
  } else if (m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                       m_image_ctx.snap_lock) &&
             !ObjectMap<>::is_compatible(m_image_ctx.layout, size)) {
    // image size too big
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EINVAL);
    return;
  }

  m_image_ctx.snap_lock.put_read();

  operation::ResizeRequest<I> *req = new operation::ResizeRequest<I>(
    m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), size, allow_shrink,
    prog_ctx, journal_op_tid, false);

  req->send();
}

// called by
// Image::snap_create or rbd_snap_create
template <typename I>
int Operations<I>::snap_create(const char *snap_name,
			       const cls::rbd::SnapshotNamespace &snap_namespace) {
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  C_SaferCond ctx;
  snap_create(snap_name, snap_namespace, &ctx);
  r = ctx.wait();

  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_create);
  return r;
}

template <typename I>
void Operations<I>::snap_create(const char *snap_name,
				const cls::rbd::SnapshotNamespace &snap_namespace,
				Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  m_image_ctx.snap_lock.get_read();
  // iterate ImageCtx::snap_ids
  if (m_image_ctx.get_snap_id(snap_name) != CEPH_NOSNAP) {
    m_image_ctx.snap_lock.put_read();

    on_finish->complete(-EEXIST);
    return;
  }

  m_image_ctx.snap_lock.put_read();

  C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(
    m_image_ctx, "snap_create", true,
    boost::bind(&Operations<I>::execute_snap_create, this, snap_name,
		snap_namespace, _1, 0, false), // local
    boost::bind(&ImageWatcher<I>::notify_snap_create, m_image_ctx.image_watcher,
                snap_name, snap_namespace, _1), // remote, only called when we are not the exclusive lock owner
    {-EEXIST}, on_finish);

  req->send();
}

// called by
// librbd/journal/Replay.cc:ExecuteOp::execute(const journal::SnapCreateEvent), with journal_op_tid set to non-zero
// ImageWatcher<I>::handle_payload(const SnapCreatePayload)
// librbd::Operations<I>::snap_create
// rbd::mirror::image_sync::SnapshotCreateRequest<I>::send_snap_create, with skip_object_map set to true
template <typename I>
void Operations<I>::execute_snap_create(const std::string &snap_name,
					const cls::rbd::SnapshotNamespace &snap_namespace,
                                        Context *on_finish,
                                        uint64_t journal_op_tid,
                                        bool skip_object_map) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  m_image_ctx.snap_lock.get_read();

  // iterate ImageCtx::snap_ids to find the snap id
  if (m_image_ctx.get_snap_id(snap_name) != CEPH_NOSNAP) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EEXIST);
    return;
  }

  m_image_ctx.snap_lock.put_read();

  operation::SnapshotCreateRequest<I> *req =
    new operation::SnapshotCreateRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_name,
      snap_namespace, journal_op_tid, skip_object_map);

  req->send();
}

template <typename I>
int Operations<I>::snap_rollback(const char *snap_name,
                                 ProgressContext& prog_ctx) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  {
    // need to drop snap_lock before invalidating cache
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

    if (!m_image_ctx.snap_exists) {

      // ImageCtx::snap_exists was initialized to true by ImageCtx::ImageCtx, and can
      // only be set to false by librbd::image::RefreshRequest<I>::apply, which means
      // the snapshot has been removed when we are still using it

      return -ENOENT;
    }

    // we have opened the HEAD image
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      return -EROFS;
    }

    // we are to rollback to snap_name
    uint64_t snap_id = m_image_ctx.get_snap_id(snap_name);
    if (snap_id == CEPH_NOSNAP) {
      lderr(cct) << "No such snapshot found." << dendl;
      return -ENOENT;
    }
  }

  // other operations will use C_InvokeAsyncRequest or invoke_async_request,
  // so no need to call this interface
  // try to grab exclusive lock and wait synchronously, if the lock
  // has been held by others, then we will fail
  r = prepare_image_update();
  if (r < 0) {
    return -EROFS;
  }

  if (m_image_ctx.exclusive_lock != nullptr &&
      !m_image_ctx.exclusive_lock->is_lock_owner()) {
    return -EROFS;
  }

  C_SaferCond cond_ctx;

  execute_snap_rollback(snap_name, prog_ctx, &cond_ctx);

  r = cond_ctx.wait();
  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rollback);

  return r;
}

template <typename I>
void Operations<I>::execute_snap_rollback(const std::string &snap_name,
                                          ProgressContext& prog_ctx,
                                          Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  m_image_ctx.snap_lock.get_read();

  uint64_t snap_id = m_image_ctx.get_snap_id(snap_name);
  if (snap_id == CEPH_NOSNAP) {
    lderr(cct) << "No such snapshot found." << dendl;

    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-ENOENT);
    return;
  }

  uint64_t new_size = m_image_ctx.get_image_size(snap_id);

  m_image_ctx.snap_lock.put_read();

  // async mode used for journal replay
  operation::SnapshotRollbackRequest<I> *request =
    new operation::SnapshotRollbackRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_name,
      snap_id, new_size, prog_ctx);

  request->send();
}

// called by
// librbd::snap_remove
// rbd::mirror::ImageDeleter::process_image_delete
template <typename I>
int Operations<I>::snap_remove(const char *snap_name) {
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  C_SaferCond ctx;
  snap_remove(snap_name, &ctx);
  r = ctx.wait();

  if (r < 0) {
    return r;
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_remove);
  return 0;
}

// called by
// librbd::snap_remove
// librbd::Operations<I>::snap_remove(const char *snap_name)
// rbd::mirror::mirror_sync::SyncPointPruneRequest<I>::send_remove_snap
template <typename I>
void Operations<I>::snap_remove(const char *snap_name, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    on_finish->complete(-EROFS);
    return;
  }

  // quickly filter out duplicate ops
  m_image_ctx.snap_lock.get_read();

  if (m_image_ctx.get_snap_id(snap_name) == CEPH_NOSNAP) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-ENOENT);
    return;
  }

  bool proxy_op = ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0 ||
                   (m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0);

  m_image_ctx.snap_lock.put_read();

  if (proxy_op) {
    C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(
      m_image_ctx, "snap_remove", true,
      boost::bind(&Operations<I>::execute_snap_remove, this, snap_name, _1),
      boost::bind(&ImageWatcher<I>::notify_snap_remove, m_image_ctx.image_watcher,
                  snap_name, _1),
      {-ENOENT}, on_finish);

    req->send();
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);

    execute_snap_remove(snap_name, on_finish);
  }
}

// called by
// src/librd/journal/Replay.cc/librbd::journal::anon::ExecuteOp::execute(const journal::SnapRemoveEvent)
// librbd::mirror::DisableRequest<I>::send_remove_snap
// ImageWatcher<I>::handle_payload(const SnapRemovePayload)
// Operations<I>::snap_remove, i.e., above
// rbd::mirror::image_sync::SnapshotCopyRequest<I>::send_snap_remove
template <typename I>
void Operations<I>::execute_snap_remove(const std::string &snap_name,
                                        Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  {
    if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
      // see 883169f7, librbd: snap_remove proxy requests require fast diff feature
      assert(m_image_ctx.exclusive_lock == nullptr ||
             m_image_ctx.exclusive_lock->is_lock_owner());
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  m_image_ctx.snap_lock.get_read();
  uint64_t snap_id = m_image_ctx.get_snap_id(snap_name);
  if (snap_id == CEPH_NOSNAP) {
    lderr(m_image_ctx.cct) << "No such snapshot found." << dendl;
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-ENOENT);
    return;
  }

  bool is_protected;
  int r = m_image_ctx.is_snap_protected(snap_id, &is_protected);
  if (r < 0) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(r);
    return;
  } else if (is_protected) {
    lderr(m_image_ctx.cct) << "snapshot is protected" << dendl;
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EBUSY);
    return;
  }
  m_image_ctx.snap_lock.put_read();

  operation::SnapshotRemoveRequest<I> *req =
    new operation::SnapshotRemoveRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_name,
      snap_id);

  req->send();
}

template <typename I>
int Operations<I>::snap_rename(const char *srcname, const char *dstname) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_name=" << srcname << ", "
                << "new_snap_name=" << dstname << dendl;

  snapid_t snap_id;
  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0)
    return r;

  {
    RWLock::RLocker l(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.get_snap_id(srcname);
    if (snap_id == CEPH_NOSNAP) {
      return -ENOENT;
    }
    if (m_image_ctx.get_snap_id(dstname) != CEPH_NOSNAP) {
      return -EEXIST;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_rename", true,
                             boost::bind(&Operations<I>::execute_snap_rename,
                                         this, snap_id, dstname, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_rename,
                                         m_image_ctx.image_watcher, snap_id,
                                         dstname, _1));
    if (r < 0 && r != -EEXIST) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    execute_snap_rename(snap_id, dstname, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  m_image_ctx.perfcounter->inc(l_librbd_snap_rename);
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_rename(const uint64_t src_snap_id,
                                        const std::string &dest_snap_name,
                                        Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if ((m_image_ctx.features & RBD_FEATURE_JOURNALING) != 0) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  m_image_ctx.snap_lock.get_read();
  if (m_image_ctx.get_snap_id(dest_snap_name) != CEPH_NOSNAP) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EEXIST);
    return;
  }
  m_image_ctx.snap_lock.put_read();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_id=" << src_snap_id << ", "
                << "new_snap_name=" << dest_snap_name << dendl;

  operation::SnapshotRenameRequest<I> *req =
    new operation::SnapshotRenameRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), src_snap_id,
      dest_snap_name);

  req->send();
}

template <typename I>
int Operations<I>::snap_protect(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  if (!m_image_ctx.test_features(RBD_FEATURE_LAYERING)) {
    lderr(cct) << "image must support layering" << dendl;
    return -ENOSYS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    bool is_protected;
    r = m_image_ctx.is_snap_protected(m_image_ctx.get_snap_id(snap_name),
                                      &is_protected);
    if (r < 0) {
      return r;
    }

    if (is_protected) {
      return -EBUSY;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_protect", true,
                             boost::bind(&Operations<I>::execute_snap_protect,
                                         this, snap_name, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_protect,
                                         m_image_ctx.image_watcher, snap_name,
                                         _1));
    if (r < 0 && r != -EBUSY) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    execute_snap_protect(snap_name, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_protect(const std::string &snap_name,
                                         Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  m_image_ctx.snap_lock.get_read();
  bool is_protected;
  int r = m_image_ctx.is_snap_protected(m_image_ctx.get_snap_id(snap_name),
                                        &is_protected);
  if (r < 0) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(r);
    return;
  } else if (is_protected) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EBUSY);
    return;
  }
  m_image_ctx.snap_lock.put_read();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotProtectRequest<I> *request =
    new operation::SnapshotProtectRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_name);
  request->send();
}

template <typename I>
int Operations<I>::snap_unprotect(const char *snap_name) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    bool is_unprotected;
    r = m_image_ctx.is_snap_unprotected(m_image_ctx.get_snap_id(snap_name),
                                  &is_unprotected);
    if (r < 0) {
      return r;
    }

    if (is_unprotected) {
      return -EINVAL;
    }
  }

  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    r = invoke_async_request("snap_unprotect", true,
                             boost::bind(&Operations<I>::execute_snap_unprotect,
                                         this, snap_name, _1),
                             boost::bind(&ImageWatcher<I>::notify_snap_unprotect,
                                         m_image_ctx.image_watcher, snap_name,
                                         _1));
    if (r < 0 && r != -EINVAL) {
      return r;
    }
  } else {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond cond_ctx;
    execute_snap_unprotect(snap_name, &cond_ctx);

    r = cond_ctx.wait();
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

template <typename I>
void Operations<I>::execute_snap_unprotect(const std::string &snap_name,
                                           Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
    assert(m_image_ctx.exclusive_lock == nullptr ||
           m_image_ctx.exclusive_lock->is_lock_owner());
  }

  m_image_ctx.snap_lock.get_read();
  bool is_unprotected;
  int r = m_image_ctx.is_snap_unprotected(m_image_ctx.get_snap_id(snap_name),
                                          &is_unprotected);
  if (r < 0) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(r);
    return;
  } else if (is_unprotected) {
    m_image_ctx.snap_lock.put_read();
    on_finish->complete(-EINVAL);
    return;
  }
  m_image_ctx.snap_lock.put_read();

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_name=" << snap_name
                << dendl;

  operation::SnapshotUnprotectRequest<I> *request =
    new operation::SnapshotUnprotectRequest<I>(
      m_image_ctx, new C_NotifyUpdate<I>(m_image_ctx, on_finish), snap_name);
  request->send();
}

// called by
// librbd::snap_set_limit
// librbd::Image::snap_set_limit
template <typename I>
int Operations<I>::snap_set_limit(uint64_t limit) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": limit=" << limit << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);

    C_SaferCond limit_ctx;

    if (m_image_ctx.exclusive_lock != nullptr &&
	!m_image_ctx.exclusive_lock->is_lock_owner()) {
      C_SaferCond lock_ctx;

      m_image_ctx.exclusive_lock->acquire_lock(&lock_ctx);
      r = lock_ctx.wait();
      if (r < 0) {
	return r;
      }
    }

    execute_snap_set_limit(limit, &limit_ctx);
    r = limit_ctx.wait();
  }

  return r;
}

template <typename I>
void Operations<I>::execute_snap_set_limit(const uint64_t limit,
					   Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": limit=" << limit
                << dendl;

  operation::SnapshotLimitRequest<I> *request =
    new operation::SnapshotLimitRequest<I>(m_image_ctx, on_finish, limit);

  request->send();
}

// called by
// Image::update_features
// rbd_update_features
template <typename I>
int Operations<I>::update_features(uint64_t features, bool enabled) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": features=" << features
                << ", enabled=" << enabled << dendl;

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  } else if (m_image_ctx.old_format) {
    lderr(cct) << "old-format images do not support features" << dendl;
    return -EINVAL;
  }

  uint64_t disable_mask = (RBD_FEATURES_MUTABLE |
                           RBD_FEATURES_DISABLE_ONLY);
  if ((enabled && (features & RBD_FEATURES_MUTABLE) != features) ||
      (!enabled && (features & disable_mask) != features)) {
    lderr(cct) << "cannot update immutable features" << dendl;
    return -EINVAL;
  }

  if (features == 0) {
    lderr(cct) << "update requires at least one feature" << dendl;
    return -EINVAL;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (enabled && (features & m_image_ctx.features) != 0) {
      lderr(cct) << "one or more requested features are already enabled"
		 << dendl;
      return -EINVAL;
    }

    if (!enabled && (features & ~m_image_ctx.features) != 0) {
      lderr(cct) << "one or more requested features are already disabled"
		 << dendl;
      return -EINVAL;
    }
  }

  // if disabling journaling, avoid attempting to open the journal
  // when acquiring the exclusive lock in case the journal is corrupt
  bool disabling_journal = false;
  if (!enabled && ((features & RBD_FEATURE_JOURNALING) != 0)) {
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    m_image_ctx.set_journal_policy(new journal::DisabledPolicy());
    disabling_journal = true;
  }
  BOOST_SCOPE_EXIT_ALL( (this)(disabling_journal) ) {
    if (disabling_journal) {
      RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
      m_image_ctx.set_journal_policy(
        new journal::StandardPolicy<I>(&m_image_ctx));
    }
  };

  r = invoke_async_request("update_features", false,
                           boost::bind(&Operations<I>::execute_update_features,
                                       this, features, enabled, _1, 0),
                           boost::bind(&ImageWatcher<I>::notify_update_features,
                                       m_image_ctx.image_watcher, features,
                                       enabled, _1));
  ldout(cct, 2) << "update_features finished" << dendl;
  return r;
}

// called by
// librbd::Operations<I>::update_features, always has journal_op_tid set to zero
// librbd::journal::ExecuteOp::execute(journal::UpdateFeaturesEvent), which has journal_op_tid set to non-zero
template <typename I>
void Operations<I>::execute_update_features(uint64_t features, bool enabled,
                                            Context *on_finish,
                                            uint64_t journal_op_tid) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": features=" << features
                << ", enabled=" << enabled << dendl;

  if (enabled) {
    operation::EnableFeaturesRequest<I> *req =
      new operation::EnableFeaturesRequest<I>(
        m_image_ctx, on_finish, journal_op_tid, features);

    req->send();
  } else {
    operation::DisableFeaturesRequest<I> *req =
      new operation::DisableFeaturesRequest<I>(
        m_image_ctx, on_finish, journal_op_tid, features, false);
    req->send();
  }
}

template <typename I>
int Operations<I>::metadata_set(const std::string &key,
                                const std::string &value) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << ", value="
                << value << dendl;

  string start = m_image_ctx.METADATA_CONF_PREFIX;
  size_t conf_prefix_len = start.size();

  if (key.size() > conf_prefix_len && !key.compare(0, conf_prefix_len, start)) {
    // validate config setting
    string subkey = key.substr(conf_prefix_len, key.size() - conf_prefix_len);
    int r = md_config_t().set_val(subkey.c_str(), value);
    if (r < 0) {
      return r;
    }
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond metadata_ctx;

    if (m_image_ctx.exclusive_lock != nullptr &&
	!m_image_ctx.exclusive_lock->is_lock_owner()) {
      C_SaferCond lock_ctx;

      m_image_ctx.exclusive_lock->acquire_lock(&lock_ctx);
      r = lock_ctx.wait();
      if (r < 0) {
	return r;
      }
    }

    execute_metadata_set(key, value, &metadata_ctx);
    r = metadata_ctx.wait();
  }

  return r;
}

template <typename I>
void Operations<I>::execute_metadata_set(const std::string &key,
					const std::string &value,
					Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << ", value="
                << value << dendl;

  operation::MetadataSetRequest<I> *request =
    new operation::MetadataSetRequest<I>(m_image_ctx, on_finish, key, value);
  request->send();
}

template <typename I>
int Operations<I>::metadata_remove(const std::string &key) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << dendl;

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  int r = m_image_ctx.state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  if (m_image_ctx.read_only) {
    return -EROFS;
  }

  {
    RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
    C_SaferCond metadata_ctx;

    if (m_image_ctx.exclusive_lock != nullptr &&
        !m_image_ctx.exclusive_lock->is_lock_owner()) {
      C_SaferCond lock_ctx;

      m_image_ctx.exclusive_lock->acquire_lock(&lock_ctx);
      r = lock_ctx.wait();
      if (r < 0) {
        return r;
      }
    }

    execute_metadata_remove(key, &metadata_ctx);
    r = metadata_ctx.wait();
  }

  return r;
}

template <typename I>
void Operations<I>::execute_metadata_remove(const std::string &key,
                                           Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": key=" << key << dendl;

  operation::MetadataRemoveRequest<I> *request =
    new operation::MetadataRemoveRequest<I>(m_image_ctx, on_finish, key);
  request->send();
}

// called by
// librbd::remove
// librbd::Operations<I>::snap_rollback
template <typename I>
int Operations<I>::prepare_image_update() {
  assert(m_image_ctx.owner_lock.is_locked() &&
         !m_image_ctx.owner_lock.is_wlocked());

  if (m_image_ctx.image_watcher == NULL) {
    return -EROFS;
  }

  // need to upgrade to a write lock
  int r = 0;
  bool trying_lock = false;
  C_SaferCond ctx;

  m_image_ctx.owner_lock.put_read();

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);

    if (m_image_ctx.exclusive_lock != nullptr &&
        (!m_image_ctx.exclusive_lock->is_lock_owner() ||
         !m_image_ctx.exclusive_lock->accept_requests(&r))) {
      m_image_ctx.exclusive_lock->try_acquire_lock(&ctx);
      trying_lock = true;
    }
  }

  if (trying_lock) {
    r = ctx.wait();
  }

  m_image_ctx.owner_lock.get_read();

  return r;
}

// snap_create and snap_remove create C_InvokeAsyncRequest instance directly
// instead of call this interface
template <typename I>
int Operations<I>::invoke_async_request(const std::string& request_type,
                                        bool permit_snapshot,
                                        const boost::function<void(Context*)>& local_request,
                                        const boost::function<void(Context*)>& remote_request) {
  C_SaferCond ctx;

  C_InvokeAsyncRequest<I> *req = new C_InvokeAsyncRequest<I>(m_image_ctx,
                                                             request_type,
                                                             permit_snapshot,
                                                             local_request,
                                                             remote_request,
                                                             {}, &ctx);
  req->send();

  return ctx.wait();
}

} // namespace librbd

template class librbd::Operations<librbd::ImageCtx>;
