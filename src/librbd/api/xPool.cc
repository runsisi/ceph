// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/xPool.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "cls/rbd/cls_rbd_client.h"
#include "osd/osd_types.h"
#include "librbd/Utils.h"
#include "librbd/api/xChild.h"
#include "librbd/api/xImage.h"
#include "librbd/api/xTrash.h"

#define dout_subsys ceph_subsys_rbd

namespace librbd {
namespace api {

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::zPool: " << __func__ << ": "

template <typename I>
int xPool<I>::get_stats(librados::IoCtx& io_ctx) {
  auto cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 10) << dendl;

  std::map<std::string, trash_image_info_t> trash_entries;
  int r = xTrash<I>::list(io_ctx, &trash_entries);
  if (r < 0 && r != -EOPNOTSUPP) {
    return r;
  }

  // images
  std::map<std::string, std::string> images;
  r = xImage<I>::list(io_ctx, &images);
  if (r < 0) {
    return r;
  }

  for (auto& it : trash_entries) {
    (void)it; // disable unused warning
    // images are moved to trash when removing, since Nautilus
    /*if (it.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
      images.insert({it.id, it.name});
    }*/
  }

  ldout(cct, 7) << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:" << dendl;

  utime_t latency;

  // list_info_v2
  latency = ceph_clock_now();

  std::map<std::string, std::pair<xImageInfo_v2, int>> entries;
  r = xImage<I>::list_info_v2(io_ctx, images, &entries);
  if (r < 0) {
    return r;
  }

  latency = ceph_clock_now() - latency;
  ldout(cct, 3) << "xImage<I>::list_info_v2 latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  // get_du
  {
    latency = ceph_clock_now();

    for (auto &i : entries) {
      auto &info = i.second.first;
      int r = i.second.second;
      if (r < 0) {
        lderr(cct) << "failed to get info for "
                   << info.id << " - " << info.name << ": "
                   << cpp_strerror(r)
                   << dendl;
        continue;
      }

      uint64_t size;
      r = xImage<I>::get_du(io_ctx, i.first, CEPH_NOSNAP, &size);
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::get_du latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  // get_du_sync
  {
    latency = ceph_clock_now();

    for (auto &i : entries) {
      auto &info = i.second.first;
      int r = i.second.second;
      if (r < 0) {
        lderr(cct) << "failed to get info for "
                   << info.id << " - " << info.name << ": "
                   << cpp_strerror(r)
                   << dendl;
        continue;
      }

      uint64_t size;
      r = xImage<I>::get_du_sync(io_ctx, i.first, CEPH_NOSNAP, &size);
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::get_du_sync latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  // get_du_v2
  {
    latency = ceph_clock_now();

    for (auto &i : entries) {
      auto &info = i.second.first;
      int r = i.second.second;
      if (r < 0) {
        lderr(cct) << "failed to get info for "
                   << info.id << " - " << info.name << ": "
                   << cpp_strerror(r)
                   << dendl;
        continue;
      }

      std::map<snapid_t, uint64_t> dus;
      r = xImage<I>::get_du_v2(io_ctx, i.first, &dus);
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::get_du_v2 latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  ldout(cct, 7) << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:" << dendl;
  {
    latency = ceph_clock_now();

    std::map<std::string, std::pair<xImageInfo, int>> entries;
    r = xImage<I>::list_info(io_ctx, images, &entries);
    if (r < 0) {
      return r;
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::list_info latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  ldout(cct, 7) << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:" << dendl;
  {
    latency = ceph_clock_now();

    std::map<std::string, std::pair<uint64_t, int>> entries;
    r = xImage<I>::list_du(io_ctx, &entries);
    if (r < 0) {
      return r;
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::list_du latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  ldout(cct, 7) << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:" << dendl;
  {
    latency = ceph_clock_now();

    std::map<std::string, std::pair<std::map<snapid_t, uint64_t>, int>> entries;
    r = xImage<I>::list_du_v2(io_ctx, &entries);
    if (r < 0) {
      return r;
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "xImage<I>::list_du_v2 latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  // children
  {
    latency = ceph_clock_now();

    std::map<ParentSpec, std::set<std::string>> children;
    int r = xChild<I>::list(io_ctx, &children);
    if (r < 0) {
      return r;
    }

    latency = ceph_clock_now() - latency;
    ldout(cct, 3) << "zChild<I>::list latency: "
                  << latency.sec() << "s/"
                  << latency.usec() << "us" << dendl;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xPool<librbd::ImageCtx>;
