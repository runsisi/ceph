// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/debug.h"
#include "common/errno.h"

#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd_types.h"
#include "librbd/internal.h"

#include "PoolWatcher.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::PoolWatcher: " << this << " " \
                           << __func__ << ": "

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::cls_client::mirror_image_list;

namespace rbd {
namespace mirror {

// will be created by Replayer::init
PoolWatcher::PoolWatcher(librados::IoCtx &remote_io_ctx,
                         double interval_seconds,
			 Mutex &lock, Cond &cond) :
  m_lock(lock),
  m_refresh_cond(cond),
  m_timer(g_ceph_context, m_lock, false),
  m_interval(interval_seconds)
{
  m_remote_io_ctx.dup(remote_io_ctx);
  m_timer.init();
}

PoolWatcher::~PoolWatcher()
{
  Mutex::Locker l(m_lock);

  m_stopping = true;
  m_timer.shutdown();
}

// called by
// Replayer::run
bool PoolWatcher::is_blacklisted() const {
  assert(m_lock.is_locked());

  // was set by PoolWatcher::refresh_images
  return m_blacklisted;
}

const PoolWatcher::ImageIds& PoolWatcher::get_images() const
{
  assert(m_lock.is_locked());

  return m_images;
}

// called by
// Replayer::init
// reschedule default to true, and never be false except for test code
void PoolWatcher::refresh_images(bool reschedule)
{
  ImageIds image_ids;

  // to get mirroring enabled images from remote peer, i.e., pool at remote cluster
  int r = refresh(&image_ids);

  Mutex::Locker l(m_lock);

  if (r >= 0) {
    m_images = std::move(image_ids);
  } else if (r == -EBLACKLISTED) {
    derr << "blacklisted during image refresh" << dendl;

    m_blacklisted = true;
  }

  if (!m_stopping && reschedule) {

    // refresh local pool to

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&PoolWatcher::refresh_images, this, true));

    m_timer.add_event_after(m_interval, ctx);
  }

  // signal replayer thread
  m_refresh_cond.Signal();

  // TODO: perhaps use a workqueue instead, once we get notifications
  // about new/removed mirrored images
}

// called by
// PoolWatcher::refresh_images
int PoolWatcher::refresh(ImageIds *image_ids) {
  dout(20) << "enter" << dendl;

  std::string pool_name = m_remote_io_ctx.get_pool_name();

  rbd_mirror_mode_t mirror_mode;
  int r = librbd::mirror_mode_get(m_remote_io_ctx, &mirror_mode);
  if (r < 0) {
    derr << "could not tell whether mirroring was enabled for "
         << pool_name << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
    dout(20) << "pool " << pool_name << " has mirroring disabled" << dendl;
    return 0;
  }

  // the remote pool has mirror enabled, either pool mode or image mode,
  // now we are to get all the mirror enabled images

  // get all images of the remote pool from RBD_DIRECTORY
  // <image name, image id>
  std::map<std::string, std::string> images_map;
  r = librbd::list_images_v2(m_remote_io_ctx, images_map);
  if (r < 0) {
    derr << "error retrieving image names from pool " << pool_name << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  // <image id, image name>
  std::map<std::string, std::string> image_id_to_name;

  for (const auto& img_pair : images_map) {
    image_id_to_name.insert(std::make_pair(img_pair.second, img_pair.first));
  }

  std::string last_read = "";
  int max_read = 1024;

  // to get all mirror registered images of the remote pool
  do {
    // <image id, image global id>
    std::map<std::string, std::string> mirror_images;

    // get from omap of RBD_MIRRORING
    r =  mirror_image_list(&m_remote_io_ctx, last_read, max_read,
                           &mirror_images);
    if (r < 0) {
      derr << "error listing mirrored image directory: "
           << cpp_strerror(r) << dendl;
      return r;
    }

    for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {

      // mirror enabled images only has <image id, image global id> info,
      // we need to get the name of the mirror enabled image

      // RBD_DIRECTORY may not agree with RBD_MIRRORING
      boost::optional<std::string> image_name(boost::none);

      auto it2 = image_id_to_name.find(it->first);
      if (it2 != image_id_to_name.end()) {
        image_name = it2->second;
      }

      // set<image id, image name, image global id>, we did not check if
      // this image is primary or not, coz if may be too completed
      // for us to do it here, we delegate this check later to
      // BootstrapRequest<I>::handle_open_remote_image
      image_ids->insert(ImageId(it->first, image_name, it->second));
    }

    if (!mirror_images.empty()) {
      last_read = mirror_images.rbegin()->first;
    }

    r = mirror_images.size();
  } while (r == max_read);

  return 0;
}

} // namespace mirror
} // namespace rbd
