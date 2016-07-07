// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "journal/Journaler.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ProgressContext.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::ImageCopyRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
ImageCopyRequest<I>::ImageCopyRequest(I *local_image_ctx, I *remote_image_ctx,
                                      SafeTimer *timer, Mutex *timer_lock,
                                      Journaler *journaler,
                                      MirrorPeerClientMeta *client_meta,
                                      MirrorPeerSyncPoint *sync_point,
                                      Context *on_finish,
				      ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::image_sync::ImageCopyRequest",
		local_image_ctx->cct, on_finish),
    m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_timer(timer), m_timer_lock(timer_lock), m_journaler(journaler),
    m_client_meta(client_meta), m_sync_point(sync_point),
    m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("ImageCopyRequest::m_lock", this)),
    m_updating_sync_point(false), m_update_sync_ctx(nullptr),
    m_update_sync_point_interval(g_ceph_context->_conf->rbd_mirror_sync_point_update_age),
    m_client_meta_copy(*client_meta) {
  assert(!m_client_meta_copy.sync_points.empty());
}

template <typename I>
void ImageCopyRequest<I>::send() {

  // calc map<remote snap id, vector<local snap id>> m_snap_map, which
  // is used by object copy, and used by ObjectCopyRequest<I>::send_write_object
  // eventually
  int r = compute_snap_map();
  if (r < 0) {
    finish(r);
    return;
  }

  send_update_max_object_count();
}

template <typename I>
void ImageCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  dout(20) << dendl;

  m_canceled = true;
}

template <typename I>
void ImageCopyRequest<I>::send_update_max_object_count() {
  uint64_t max_objects = m_client_meta->sync_object_count;

  {
    RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);

    max_objects = std::max(max_objects,
                           m_remote_image_ctx->get_object_count(CEPH_NOSNAP));
    for (auto snap_id : m_remote_image_ctx->snaps) {
      max_objects = std::max(max_objects,
                             m_remote_image_ctx->get_object_count(snap_id));
    }
  }

  if (max_objects <= m_client_meta->sync_object_count) {
    send_object_copies();

    return;
  }

  // the max object count to sync this time is lager than the ever
  // synced, so update this count

  update_progress("UPDATE_MAX_OBJECT_COUNT");

  dout(20) << ": sync_object_count=" << max_objects << dendl;

  m_client_meta_copy = *m_client_meta;
  m_client_meta_copy.sync_object_count = max_objects;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(m_client_meta_copy);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_update_max_object_count>(
      this);

  // update the max ever synced object count
  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void ImageCopyRequest<I>::handle_update_max_object_count(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {
    Mutex::Locker locker(m_lock);
    if (m_canceled) {
      dout(10) << ": image copy canceled" << dendl;
      r = -ECANCELED;
    }
  }

  if (r < 0) {
    if (r != -ECANCELED) {
      derr << ": failed to update client data: " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  // update provided meta structure to reflect reality
  m_client_meta->sync_object_count = m_client_meta_copy.sync_object_count;

  send_object_copies();
}

template <typename I>
void ImageCopyRequest<I>::send_object_copies() {
  CephContext *cct = m_local_image_ctx->cct;

  m_object_no = 0;

  if (m_sync_point->object_number) {

    // re-sync from the last sync'ed object

    m_object_no = *m_sync_point->object_number + 1;
  }

  m_end_object_no = m_client_meta->sync_object_count;

  dout(20) << ": start_object=" << m_object_no << ", "
           << "end_object=" << m_end_object_no << dendl;

  update_progress("COPY_OBJECT");

  bool complete;

  {
    Mutex::Locker locker(m_lock);

    // default is 10
    for (int i = 0; i < cct->_conf->rbd_concurrent_management_ops; ++i) {

      // try to start 10 object copies concurrently

      send_next_object_copy();

      if (m_ret_val < 0 && m_current_ops == 0) {

        // already failed or started nothing

        break;
      }
    }

    complete = (m_current_ops == 0);

    if (!complete) {

      // started object copies, need to add a timer event to update the sync point

      m_update_sync_ctx = new FunctionContext([this](int r) {
          this->send_update_sync_point();
      });
    }
  }

  {
    Mutex::Locker timer_locker(*m_timer_lock);

    if (m_update_sync_ctx) {

      // add the timer event to update the sync point

      m_timer->add_event_after(m_update_sync_point_interval,
                               m_update_sync_ctx);
    }
  }

  if (complete) {

    // ok, started nothing,

    send_flush_sync_point();
  }
}

template <typename I>
void ImageCopyRequest<I>::send_next_object_copy() {
  assert(m_lock.is_locked());

  if (m_canceled && m_ret_val == 0) {
    dout(10) << ": image copy canceled" << dendl;

    m_ret_val = -ECANCELED;
  }

  if (m_ret_val < 0 || m_object_no >= m_end_object_no) {

    // failed or reached the last object, do not start a new object copy

    return;
  }

  // start a new object copy

  uint64_t ono = m_object_no++;

  dout(20) << ": object_num=" << ono << dendl;

  ++m_current_ops;

  Context *ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_object_copy>(this);

  // m_snap_map calc'ed in ImageCopyRequest<I>::compute_snap_map
  ObjectCopyRequest<I> *req = ObjectCopyRequest<I>::create(
    m_local_image_ctx, m_remote_image_ctx, &m_snap_map, ono, ctx);

  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_object_copy(int r) {
  dout(20) << ": r=" << r << dendl;

  int percent;
  bool complete;

  {
    Mutex::Locker locker(m_lock);

    assert(m_current_ops > 0);
    --m_current_ops;

    percent = 100 * m_object_no / m_end_object_no;

    if (r < 0) {
      derr << ": object copy failed: " << cpp_strerror(r) << dendl;

      if (m_ret_val == 0) {
        m_ret_val = r;
      }
    }

    // finished an object, try to start a next object to copy
    send_next_object_copy();

    complete = (m_current_ops == 0);
  }

  update_progress("COPY_OBJECT " + stringify(percent) + "%", false);

  if (complete) {

    // started nothing and the in-flight object copies have finished

    bool do_flush = true;

    {
      Mutex::Locker timer_locker(*m_timer_lock);
      Mutex::Locker locker(m_lock);

      if (!m_updating_sync_point) {

        // no sync point updating in progress

        if (m_update_sync_ctx != nullptr) {
          m_timer->cancel_event(m_update_sync_ctx);
          m_update_sync_ctx = nullptr;
        }
      } else {

        // ImageCopyRequest<I>::send_update_sync_point is updating the
        // sync point, wait until ImageCopyRequest<I>::handle_update_sync_point
        // to do the flush

        do_flush = false;
      }
    }

    if (do_flush) {
      send_flush_sync_point();
    }
  }
}

template <typename I>
void ImageCopyRequest<I>::send_update_sync_point() {
  Mutex::Locker l(m_lock);

  m_update_sync_ctx = nullptr;

  if (m_canceled || m_ret_val < 0 || m_current_ops == 0) {

    // no need to send sync point update, ImageCopyRequest<I>::handle_object_copy
    // will do the flush

    return;
  }

  if (m_sync_point->object_number &&
      (m_object_no-1) == m_sync_point->object_number.get()) {

    // update sync point did not progress since last sync

    return;
  }

  // need to update sync point

  m_updating_sync_point = true;

  // used for reverting if failed
  m_client_meta_copy = *m_client_meta;

  // we have finished copy object number: m_object_no - 1
  m_sync_point->object_number = m_object_no - 1;

  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": sync_point=" << *m_sync_point << dendl;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(*m_client_meta);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_update_sync_point>(
      this);

  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void ImageCopyRequest<I>::handle_update_sync_point(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    // revert the change
    *m_client_meta = m_client_meta_copy;

    lderr(cct) << ": failed to update client data: " << cpp_strerror(r)
               << dendl;
  }

  bool complete;

  {
    Mutex::Locker l(m_lock);

    m_updating_sync_point = false;

    complete = m_current_ops == 0 || m_canceled || m_ret_val < 0;

    if (!complete) {

      // prepare for the next sync point update

      m_update_sync_ctx = new FunctionContext([this](int r) {
          this->send_update_sync_point();
      });
    }
  }

  if (!complete) {
    Mutex::Locker timer_lock(*m_timer_lock);

    if (m_update_sync_ctx) {
      m_timer->add_event_after(m_update_sync_point_interval,
                               m_update_sync_ctx);
    }
  } else {

    // finished or canceled or failed

    send_flush_sync_point();
  }
}

template <typename I>
void ImageCopyRequest<I>::send_flush_sync_point() {
  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  update_progress("FLUSH_SYNC_POINT");

  m_client_meta_copy = *m_client_meta;

  // in case ImageCopyRequest<I>::handle_update_sync_point reverted the
  // update of m_client_meta, we need to re-update the m_client_meta
  if (m_object_no > 0) {
    m_sync_point->object_number = m_object_no - 1;
  } else {
    m_sync_point->object_number = boost::none;
  }

  dout(20) << ": sync_point=" << *m_sync_point << dendl;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(m_client_meta_copy);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_flush_sync_point>(
      this);

  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void ImageCopyRequest<I>::handle_flush_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    *m_client_meta = m_client_meta_copy;

    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
int ImageCopyRequest<I>::compute_snap_map() {

  librados::snap_t snap_id_start = 0;
  librados::snap_t snap_id_end;

  {
    RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);

    snap_id_end = m_remote_image_ctx->get_snap_id(m_sync_point->snap_name);

    if (snap_id_end == CEPH_NOSNAP) {
      derr << ": failed to locate snapshot: "
           << m_sync_point->snap_name << dendl;
      return -ENOENT;
    }

    if (!m_sync_point->from_snap_name.empty()) {
      snap_id_start = m_remote_image_ctx->get_snap_id(
        m_sync_point->from_snap_name);

      if (snap_id_start == CEPH_NOSNAP) {
        derr << ": failed to locate from snapshot: "
             << m_sync_point->from_snap_name << dendl;
        return -ENOENT;
      }
    }
  }

  // [from_snap_name, snap_name] -> [snap_id_start, snap_id_end]

  SnapIds snap_ids;

  // snap_seqs is calc'ed by SnapshotCopyRequest<I>::handle_snap_create to
  // denote the <remote snap id, local snap id> mapping
  for (auto it = m_client_meta->snap_seqs.begin();
       it != m_client_meta->snap_seqs.end(); ++it) {

    // push front, so from new -> old
    snap_ids.insert(snap_ids.begin(), it->second);

    if (it->first < snap_id_start) {
      continue;
    } else if (it->first > snap_id_end) {
      break;
    }

    // map<remote snap id, vector<local snap id>>, which is used by
    // ObjectCopyRequest<I>::compute_diffs
    m_snap_map[it->first] = snap_ids;
  }

  if (m_snap_map.empty()) {
    derr << ": failed to map snapshots within boundary" << dendl;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
void ImageCopyRequest<I>::update_progress(const std::string &description,
					  bool flush) {
  dout(20) << ": " << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress("IMAGE_COPY/" + description, flush);
  }
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::ImageCopyRequest<librbd::ImageCtx>;
