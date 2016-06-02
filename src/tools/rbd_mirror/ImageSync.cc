// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageSync.h"
#include "InstanceWatcher.h"
#include "ProgressContext.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSync: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {

using namespace image_sync;
using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
class ImageSync<I>::ImageCopyProgressContext : public librbd::ProgressContext {
public:
  ImageCopyProgressContext(ImageSync *image_sync) : image_sync(image_sync) {
  }

  int update_progress(uint64_t object_no, uint64_t object_count) override {
    image_sync->handle_copy_image_update_progress(object_no, object_count);
    return 0;
  }

  ImageSync *image_sync;
};

template <typename I>
ImageSync<I>::ImageSync(I *local_image_ctx, I *remote_image_ctx,
                        SafeTimer *timer, Mutex *timer_lock,
                        const std::string &mirror_uuid, Journaler *journaler,
                        MirrorPeerClientMeta *client_meta,
                        ContextWQ *work_queue,
                        InstanceWatcher<I> *instance_watcher,
                        Context *on_finish, ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::ImageSync", local_image_ctx->cct, on_finish),
    m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_timer(timer), m_timer_lock(timer_lock), m_mirror_uuid(mirror_uuid),
    m_journaler(journaler), m_client_meta(client_meta),
    m_work_queue(work_queue), m_instance_watcher(instance_watcher),
    m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("ImageSync::m_lock", this)),
    m_update_sync_point_interval(m_local_image_ctx->cct->_conf->template get_val<double>(
        "rbd_mirror_sync_point_update_age")), m_client_meta_copy(*client_meta) {
}

template <typename I>
ImageSync<I>::~ImageSync() {
  assert(m_image_copy_request == nullptr);
  assert(m_image_copy_prog_ctx == nullptr);
  assert(m_update_sync_ctx == nullptr);
}

template <typename I>
void ImageSync<I>::send() {
  send_notify_sync_request();
}

// called by
// ImageSyncThrottler<I>::cancel_sync, which called by BootstrapRequest<I>::cancel
template <typename I>
void ImageSync<I>::cancel() {
  Mutex::Locker locker(m_lock);

  dout(20) << dendl;

  m_canceled = true;

  if (m_instance_watcher->cancel_sync_request(m_local_image_ctx->id)) {
    return;
  }

  if (m_image_copy_request != nullptr) {
    m_image_copy_request->cancel();
  }
}

template <typename I>
void ImageSync<I>::send_notify_sync_request() {
  update_progress("NOTIFY_SYNC_REQUEST");

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_notify_sync_request>(this);
  m_instance_watcher->notify_sync_request(m_local_image_ctx->id, ctx);
}

template <typename I>
void ImageSync<I>::handle_notify_sync_request(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    BaseRequest::finish(r);
    return;
  }

  send_prune_catch_up_sync_point();
}

template <typename I>
void ImageSync<I>::send_prune_catch_up_sync_point() {
  update_progress("PRUNE_CATCH_UP_SYNC_POINT");

  // MirrorPeerClientMeta, got or registered by BootstrapRequest<I>::handle_get_client
  if (m_client_meta->sync_points.empty()) {

    // no sync points need to prune, create new sync point directly

    send_create_sync_point();
    return;
  }

  // have sync points, prune to a max of one, maybe zero

  dout(20) << dendl;

  // prune will remove sync points with missing snapshots and
  // ensure we have a maximum of one sync point (in case we
  // restarted)
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_catch_up_sync_point>(this);

  // ImageSync<I>::send_prune_sync_points calls create with the second
  // parameter set to true used to flag if the sync has completed
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, false, m_journaler, m_client_meta, ctx);

  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_catch_up_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune catch-up sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_sync_point();
}

template <typename I>
void ImageSync<I>::send_create_sync_point() {
  update_progress("CREATE_SYNC_POINT");

  // TODO: when support for disconnecting laggy clients is added,
  //       re-connect and create catch-up sync point
  if (m_client_meta->sync_points.size() > 0) {
    send_copy_image();
    return;
  }

  // currently no master sync point, so create one

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_create_sync_point>(this);
  SyncPointCreateRequest<I> *request = SyncPointCreateRequest<I>::create(
    m_remote_image_ctx, m_mirror_uuid, m_journaler, m_client_meta, ctx);

  request->send();
}

template <typename I>
void ImageSync<I>::handle_create_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create sync point: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_copy_image();
}

template <typename I>
void ImageSync<I>::send_copy_image() {
  librados::snap_t snap_id_start = 0;
  librados::snap_t snap_id_end;
  librbd::deep_copy::ObjectNumber object_number;
  int r = 0;
  {
    RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);
    assert(!m_client_meta->sync_points.empty());
    auto &sync_point = m_client_meta->sync_points.front();
    snap_id_end = m_remote_image_ctx->get_snap_id(
	cls::rbd::UserSnapshotNamespace(), sync_point.snap_name);
    if (snap_id_end == CEPH_NOSNAP) {
      derr << ": failed to locate snapshot: " << sync_point.snap_name << dendl;
      r = -ENOENT;
    } else if (!sync_point.from_snap_name.empty()) {
      snap_id_start = m_remote_image_ctx->get_snap_id(
        cls::rbd::UserSnapshotNamespace(), sync_point.from_snap_name);
      if (snap_id_start == CEPH_NOSNAP) {
        derr << ": failed to locate from snapshot: "
             << sync_point.from_snap_name << dendl;
        r = -ENOENT;
      }
    }
    object_number = sync_point.object_number;
  }
  if (r < 0) {
    finish(r);
    return;
  }

  m_lock.Lock();

  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_image>(this);
  m_image_copy_prog_ctx = new ImageCopyProgressContext(this);
  m_image_copy_request = librbd::DeepCopyRequest<I>::create(
      m_remote_image_ctx, m_local_image_ctx, snap_id_start, snap_id_end,
      object_number, m_work_queue, &m_client_meta->snap_seqs,
      m_image_copy_prog_ctx, ctx);
  m_image_copy_request->get();

  m_lock.Unlock();

  update_progress("COPY_IMAGE");

  m_image_copy_request->send();
}

template <typename I>
void ImageSync<I>::handle_copy_image(int r) {
  dout(20) << ": r=" << r << dendl;

  {
    Mutex::Locker timer_locker(*m_timer_lock);
    Mutex::Locker locker(m_lock);

    m_image_copy_request->put();
    m_image_copy_request = nullptr;
    delete m_image_copy_prog_ctx;
    m_image_copy_prog_ctx = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }

    if (m_update_sync_ctx != nullptr) {
      m_timer->cancel_event(m_update_sync_ctx);
      m_update_sync_ctx = nullptr;
    }

    if (m_updating_sync_point) {
      m_ret_val = r;
      return;
    }
  }

  if (r == -ECANCELED) {
    dout(10) << ": image copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to copy image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_flush_sync_point();
}

template <typename I>
void ImageSync<I>::handle_copy_image_update_progress(uint64_t object_no,
                                                     uint64_t object_count) {
  int percent = 100 * object_no / object_count;
  update_progress("COPY_IMAGE " + stringify(percent) + "%");

  Mutex::Locker locker(m_lock);
  m_image_copy_object_no = object_no;
  m_image_copy_object_count = object_count;

  if (m_update_sync_ctx == nullptr && !m_updating_sync_point) {
    send_update_sync_point();
  }
}

template <typename I>
void ImageSync<I>::send_update_sync_point() {
  assert(m_lock.is_locked());

  m_update_sync_ctx = nullptr;

  if (m_canceled) {
    return;
  }

  auto sync_point = &m_client_meta->sync_points.front();

  if (m_client_meta->sync_object_count == m_image_copy_object_count &&
      sync_point->object_number &&
      (m_image_copy_object_no - 1) == sync_point->object_number.get()) {
    // update sync point did not progress since last sync
    return;
  }

  m_updating_sync_point = true;

  m_client_meta_copy = *m_client_meta;
  m_client_meta->sync_object_count = m_image_copy_object_count;
  if (m_image_copy_object_no > 0) {
    sync_point->object_number = m_image_copy_object_no - 1;
  }

  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": sync_point=" << *sync_point << dendl;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(*m_client_meta);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_update_sync_point>(
      this);
  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void ImageSync<I>::handle_update_sync_point(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    *m_client_meta = m_client_meta_copy;
    lderr(cct) << ": failed to update client data: " << cpp_strerror(r)
               << dendl;
  }

  {
    Mutex::Locker timer_locker(*m_timer_lock);
    Mutex::Locker locker(m_lock);
    m_updating_sync_point = false;

    if (m_image_copy_request != nullptr) {
      m_update_sync_ctx = new FunctionContext(
        [this](int r) {
          Mutex::Locker locker(m_lock);
          this->send_update_sync_point();
        });
      m_timer->add_event_after(m_update_sync_point_interval,
                               m_update_sync_ctx);
      return;
    }
  }

  send_flush_sync_point();
}

template <typename I>
void ImageSync<I>::send_flush_sync_point() {
  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  update_progress("FLUSH_SYNC_POINT");

  m_client_meta_copy = *m_client_meta;
  m_client_meta->sync_object_count = m_image_copy_object_count;
  auto sync_point = &m_client_meta->sync_points.front();
  if (m_image_copy_object_no > 0) {
    sync_point->object_number = m_image_copy_object_no - 1;
  } else {
    sync_point->object_number = boost::none;
  }

  dout(20) << ": sync_point=" << *sync_point << dendl;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(*m_client_meta);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_flush_sync_point>(
      this);
  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void ImageSync<I>::handle_flush_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    *m_client_meta = m_client_meta_copy;

    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  // image sync finished
  send_prune_sync_points();
}

template <typename I>
void ImageSync<I>::send_prune_sync_points() {
  dout(20) << dendl;

  update_progress("PRUNE_SYNC_POINTS");

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_sync_points>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, true, m_journaler, m_client_meta, ctx);

  // sync has completed, prune the sync point

  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_sync_points(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_client_meta->sync_points.empty()) {

    // TODO: in ImageSync<I>::send_prune_catch_up_sync_point we may have
    // pruned to only one sync point, and ImageSync<I>::send_create_sync_point
    // never create new sync point when the existing sync points are not
    // empty, so why there is still another sync point ???

    send_copy_image();
    return;
  }

  finish(0);
}

template <typename I>
void ImageSync<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  // i.e., ImageReplayer::m_progress_cxt
  if (m_progress_ctx) {
    m_progress_ctx->update_progress("IMAGE_SYNC/" + description);
  }
}

template <typename I>
void ImageSync<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_instance_watcher->notify_sync_complete(m_local_image_ctx->id);
  BaseRequest::finish(r);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSync<librbd::ImageCtx>;
