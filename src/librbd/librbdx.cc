/*
 * librbdx.cc
 *
 *  Created on: Jul 31, 2019
 *      Author: runsisi
 */

#include "include/rbd/librbdx.hpp"

#include <cstdlib>

#include "include/utime.h"
#include "librbd/api/xChild.h"
#include "librbd/api/xImage.h"
#include "librbd/api/xTrash.h"

namespace {
  constexpr const char* conf_qos_iops_str = "conf_rbd_client_qos_limit";
  constexpr const char* conf_qos_bps_str = "conf_rbd_client_qos_bandwidth";

  void cvt_size_info(librbd::xSizeInfo& in, librbdx::size_info_t* out) {
    out->image_id = std::move(in.image_id);
    out->snap_id = in.snap_id;
    out->order = in.order;
    out->size = in.size;
    out->stripe_unit = in.stripe_unit;
    out->stripe_count = in.stripe_count;
    out->features = in.features;
    out->flags = in.flags;
  }

  void cvt_image_info(librbd::xImageInfo& in, librbdx::image_info_t* out) {
    out->id = std::move(in.id);
    out->name = std::move(in.name);
    out->order = in.order;
    out->size = in.size;
    out->stripe_unit = in.stripe_unit;
    out->stripe_count = in.stripe_count;
    out->features = in.features;
    out->flags = in.flags;
    out->snapc.seq = in.snapc.seq;
    for (auto& s : in.snapc.snaps) {
      out->snapc.snaps.push_back(s);
    }
    out->parent.spec.pool_id = in.parent.spec.pool_id;
    out->parent.spec.image_id = std::move(in.parent.spec.image_id);
    out->parent.spec.snap_id = in.parent.spec.snap_id;
    out->parent.overlap = in.parent.overlap;
    in.timestamp.to_timespec(&out->timestamp);
    out->data_pool_id = in.data_pool_id;
    for (auto& w : in.watchers) {
      out->watchers.emplace_back(std::move(w.addr));
    }
    for (auto& kv : in.kvs) {
      if (kv.first == conf_qos_iops_str) {
        out->qos.iops = std::atoll(kv.second.c_str());
      } else if (kv.first == conf_qos_bps_str) {
        out->qos.bps = std::atoll(kv.second.c_str());
      }
    }
  }

  void cvt_image_info_v2(librbd::xImageInfo_v2& in, librbdx::image_info_v2_t* out) {
    out->id = std::move(in.id);
    out->name = std::move(in.name);
    out->order = in.order;
    out->size = in.size;
    out->stripe_unit = in.stripe_unit;
    out->stripe_count = in.stripe_count;
    out->features = in.features;
    out->flags = in.flags;
    out->snapc.seq = in.snapc.seq;
    for (auto& s : in.snapc.snaps) {
      out->snapc.snaps.push_back(s);
    }
    out->parent.spec.pool_id = in.parent.spec.pool_id;
    out->parent.spec.image_id = std::move(in.parent.spec.image_id);
    out->parent.spec.snap_id = in.parent.spec.snap_id;
    out->parent.overlap = in.parent.overlap;
    in.timestamp.to_timespec(&out->timestamp);
    out->data_pool_id = in.data_pool_id;
    for (auto& w : in.watchers) {
      out->watchers.emplace_back(std::move(w.addr));
    }
    for (auto& kv : in.kvs) {
      if (kv.first == conf_qos_iops_str) {
        out->qos.iops = std::atoll(kv.second.c_str());
      } else if (kv.first == conf_qos_bps_str) {
        out->qos.bps = std::atoll(kv.second.c_str());
      }
    }
    for (auto& it : in.snaps) {
      auto& snap = out->snaps[it.first];
      auto& tsnap = in.snaps[it.first];

      snap.id = tsnap.id;
      snap.name = std::move(tsnap.name);
      snap.snap_ns_type = static_cast<librbdx::snap_ns_type_t>(tsnap.snap_ns_type);
      snap.size = tsnap.size;
      snap.features = tsnap.features;
      snap.flags = tsnap.flags;
      snap.protection_status = static_cast<librbdx::snap_protection_status_t>(tsnap.protection_status);
      tsnap.timestamp.to_timespec(&snap.timestamp);
    }
  }

  void cvt_trash_info(librbd::xTrashInfo& in, librbdx::trash_info_t* out) {
    out->id = std::move(in.id);
    out->name = std::move(in.name);
    out->source = static_cast<librbdx::trash_source_t>(in.source);
    in.deletion_time.to_timespec(&out->deletion_time);
    in.deferment_end_time.to_timespec(&out->deferment_end_time);
  }
}

namespace librbdx {

//
// xImage
//
int xRBD::get_size(librados::IoCtx& io_ctx,
    const std::string& image_id, uint64_t snap_id, size_info_t* info) {
  int r = 0;
  librbd::xSizeInfo tinfo;
  r = librbd::api::xImage<>::get_size(io_ctx, image_id, snap_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_size_info(tinfo, info);
  return r;
}

int xRBD::get_info(librados::IoCtx& io_ctx,
    const std::string& image_id, image_info_t* info) {
  int r = 0;
  librbd::xImageInfo tinfo;
  r = librbd::api::xImage<>::get_info(io_ctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info(tinfo, info);
  return r;
}

int xRBD::get_info_v2(librados::IoCtx& io_ctx,
    const std::string& image_id, image_info_v2_t* info) {
  int r = 0;
  librbd::xImageInfo_v2 tinfo;
  r = librbd::api::xImage<>::get_info_v2(io_ctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info_v2(tinfo, info);
  return r;
}

int xRBD::get_du(librados::IoCtx& io_ctx,
    const std::string& image_id, uint64_t snap_id,
    uint64_t* du) {
  int r = 0;
  *du = 0;
  r = librbd::api::xImage<>::get_du(io_ctx, image_id, snap_id, du);
  return r;
}

int xRBD::get_du_v2(librados::IoCtx& io_ctx,
    const std::string& image_id,
    std::map<uint64_t, uint64_t>* dus) {
  int r = 0;
  dus->clear();
  r = librbd::api::xImage<>::get_du_v2(io_ctx, image_id, dus);
  return r;
}

int xRBD::get_du_sync(librados::IoCtx& io_ctx,
    const std::string& image_id, uint64_t snap_id,
    uint64_t* du) {
  int r = 0;
  *du = 0;
  r = librbd::api::xImage<>::get_du_sync(io_ctx, image_id, snap_id, du);
  return r;
}

int xRBD::list(librados::IoCtx& io_ctx,
    std::map<std::string, std::string>* images) {
  int r = 0;
  images->clear();
  r = librbd::api::xImage<>::list(io_ctx, images);
  return r;
}

int xRBD::list_info(librados::IoCtx& io_ctx,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_info(io_ctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info(librados::IoCtx& io_ctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_info(io_ctx, images, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v2(librados::IoCtx& io_ctx,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v2, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v2(io_ctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v2(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v2(librados::IoCtx& io_ctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v2, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v2(io_ctx, images, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v2(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_du(librados::IoCtx& io_ctx,
    std::map<std::string, std::pair<uint64_t, int>>* dus) {
  int r = 0;
  dus->clear();
  r = librbd::api::xImage<>::list_du(io_ctx, dus);
  return r;
}

int xRBD::list_du(librados::IoCtx& io_ctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<uint64_t, int>>* dus) {
  int r = 0;
  dus->clear();
  r = librbd::api::xImage<>::list_du(io_ctx, images, dus);
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& io_ctx,
    std::map<std::string, std::pair<std::map<uint64_t, uint64_t>, int>>* dus) {
  int r = 0;
  dus->clear();
  r = librbd::api::xImage<>::list_du_v2(io_ctx, dus);
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& io_ctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<std::map<uint64_t, uint64_t>, int>>* dus) {
  int r = 0;
  dus->clear();
  r = librbd::api::xImage<>::list_du_v2(io_ctx, images, dus);
  return r;
}

//
// xChild
//
int xRBD::child_list(librados::IoCtx& io_ctx,
    std::map<parent_spec_t, std::vector<std::string>>* children) {
  int r = 0;
  children->clear();
  std::map<librbd::ParentSpec, std::set<std::string>> tchildren;
  r = librbd::api::xChild<>::list(io_ctx, &tchildren);
  if (r < 0) {
    return r;
  }
  for (auto& it : tchildren) {
    parent_spec_t parent;
    auto& tparent = it.first;

    parent.pool_id = tparent.pool_id;
    parent.image_id = std::move(tparent.image_id);
    parent.snap_id = std::move(tparent.snap_id);

    auto& children_ = (*children)[parent];
    auto& tchildren_ = tchildren[tparent];

    for (auto& c : tchildren_) {
      children_.push_back(c);
    }
  }
  return r;
}

//
// xTrash
//
int xRBD::trash_list(librados::IoCtx& io_ctx,
    std::map<std::string, trash_info_t>* trashes) {
  int r = 0;
  trashes->clear();
  std::map<std::string, librbd::xTrashInfo> ttrashes;
  r = librbd::api::xTrash<>::list(io_ctx, &ttrashes);
  if (r < 0) {
    return r;
  }
  for (auto& it : ttrashes) {
    auto& trash = (*trashes)[it.first];
    auto& ttrash = ttrashes[it.first];

    cvt_trash_info(ttrash, &trash);
  }
  return r;
}

}
