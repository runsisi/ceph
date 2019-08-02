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

  void cvt_du_info(librbd::xDuInfo& in, librbdx::du_info_t* out) {
    out->size = in.size;
    out->du = in.du;
  }

  void cvt_snap_info(librbd::xSnapInfo& in, librbdx::snap_info_t* out) {
    out->id = in.id;
    out->name = std::move(in.name);
    out->snap_ns_type = static_cast<librbdx::snap_ns_type_t>(in.snap_ns_type);
    out->size = in.size;
    out->features = in.features;
    out->flags = in.flags;
    out->protection_status = static_cast<librbdx::snap_protection_status_t>(in.protection_status);
    in.timestamp.to_timespec(&out->timestamp);
  }

  void cvt_snap_info_v2(librbd::xSnapInfo_v2& in, librbdx::snap_info_v2_t* out) {
    out->id = in.id;
    out->name = std::move(in.name);
    out->snap_ns_type = static_cast<librbdx::snap_ns_type_t>(in.snap_ns_type);
    out->size = in.size;
    out->features = in.features;
    out->flags = in.flags;
    out->protection_status = static_cast<librbdx::snap_protection_status_t>(in.protection_status);
    in.timestamp.to_timespec(&out->timestamp);
    out->du = in.du;
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

      cvt_snap_info(tsnap, &snap);
    }
  }

  void cvt_image_info_v3(librbd::xImageInfo_v3& in, librbdx::image_info_v3_t* out) {
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
    out->du = in.du;
  }

  void cvt_image_info_v4(librbd::xImageInfo_v4& in, librbdx::image_info_v4_t* out) {
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
    out->du = in.du;
    for (auto& it : in.snaps) {
      auto& snap = out->snaps[it.first];
      auto& tsnap = in.snaps[it.first];

      cvt_snap_info(tsnap, &snap);
    }
  }

  void cvt_image_info_v5(librbd::xImageInfo_v5& in, librbdx::image_info_v5_t* out) {
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
    out->du = in.du;
    for (auto& it : in.snaps) {
      auto& snap = out->snaps[it.first];
      auto& tsnap = in.snaps[it.first];

      cvt_snap_info_v2(tsnap, &snap);
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
int xRBD::get_name(librados::IoCtx& ioctx,
    const std::string& image_id, std::string* name) {
  int r = 0;
  r = librbd::api::xImage<>::get_name(ioctx, image_id, name);
  return r;
}

int xRBD::get_id(librados::IoCtx& ioctx,
    const std::string& image_name, std::string* id) {
  int r = 0;
  r = librbd::api::xImage<>::get_id(ioctx, image_name, id);
  return r;
}

int xRBD::get_size(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id, size_info_t* info) {
  int r = 0;
  librbd::xSizeInfo tinfo; // t prefix means temp
  r = librbd::api::xImage<>::get_size(ioctx, image_id, snap_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_size_info(tinfo, info);
  return r;
}

int xRBD::get_du(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id,
    du_info_t* info) {
  int r = 0;
  librbd::xDuInfo tinfo;
  r = librbd::api::xImage<>::get_du(ioctx, image_id, snap_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_du_info(tinfo, info);
  return r;
}

int xRBD::get_du_v2(librados::IoCtx& ioctx,
    const std::string& image_id,
    std::map<uint64_t, du_info_t>* infos) {
  int r = 0;
  infos->clear();
  std::map<uint64_t, librbd::xDuInfo> tinfos;
  r = librbd::api::xImage<>::get_du_v2(ioctx, image_id, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first];
    auto& tinfo = tinfos[it.first];

    cvt_du_info(tinfo, &info);
  }
  return r;
}

int xRBD::get_du_sync(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id,
    du_info_t* info) {
  int r = 0;
  librbd::xDuInfo tinfo;
  r = librbd::api::xImage<>::get_du_sync(ioctx, image_id, snap_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_du_info(tinfo, info);
  return r;
}

int xRBD::get_info(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_t* info) {
  int r = 0;
  librbd::xImageInfo tinfo;
  r = librbd::api::xImage<>::get_info(ioctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info(tinfo, info);
  return r;
}

int xRBD::get_info_v2(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v2_t* info) {
  int r = 0;
  librbd::xImageInfo_v2 tinfo;
  r = librbd::api::xImage<>::get_info_v2(ioctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info_v2(tinfo, info);
  return r;
}

int xRBD::get_info_v3(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v3_t* info) {
  int r = 0;
  librbd::xImageInfo_v3 tinfo;
  r = librbd::api::xImage<>::get_info_v3(ioctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info_v3(tinfo, info);
  return r;
}

int xRBD::get_info_v4(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v4_t* info) {
  int r = 0;
  librbd::xImageInfo_v4 tinfo;
  r = librbd::api::xImage<>::get_info_v4(ioctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info_v4(tinfo, info);
  return r;
}

int xRBD::get_info_v5(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v5_t* info) {
  int r = 0;
  librbd::xImageInfo_v5 tinfo;
  r = librbd::api::xImage<>::get_info_v5(ioctx, image_id, &tinfo);
  if (r < 0) {
    return r;
  }
  cvt_image_info_v5(tinfo, info);
  return r;
}

int xRBD::list_du(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<du_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xDuInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_du(ioctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_du_info(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_du(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<du_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xDuInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_du(ioctx, image_ids, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_du_info(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<std::map<uint64_t, librbd::xDuInfo>, int>> tinfos;
  r = librbd::api::xImage<>::list_du_v2(ioctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& snap_infos = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tsnap_infos = it.second.first;
    auto& tr = it.second.second;

    // info
    for (auto& it : tsnap_infos) {
      auto& info = snap_infos[it.first];
      auto& tinfo = tsnap_infos[it.first];

      cvt_du_info(tinfo, &info);
    }
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<std::map<uint64_t, librbd::xDuInfo>, int>> tinfos;
  r = librbd::api::xImage<>::list_du_v2(ioctx, image_ids, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& snap_infos = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tsnap_infos = it.second.first;
    auto& tr = it.second.second;

    // info
    for (auto& it : tsnap_infos) {
      auto& info = snap_infos[it.first];
      auto& tinfo = tsnap_infos[it.first];

      cvt_du_info(tinfo, &info);
    }
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list(librados::IoCtx& ioctx,
    std::map<std::string, std::string>* images) {
  int r = 0;
  images->clear();
  r = librbd::api::xImage<>::list(ioctx, images);
  return r;
}

int xRBD::list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_info(ioctx, &tinfos);
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

int xRBD::list_info(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo, int>> tinfos;
  r = librbd::api::xImage<>::list_info(ioctx, image_ids, &tinfos);
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

int xRBD::list_info_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v2, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v2(ioctx, &tinfos);
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

int xRBD::list_info_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v2, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v2(ioctx, image_ids, &tinfos);
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

int xRBD::list_info_v3(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v3_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v3, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v3(ioctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v3(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v3(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v3_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v3, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v3(ioctx, image_ids, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v3(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v4(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v4_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v4, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v4(ioctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v4(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v4(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v4_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v4, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v4(ioctx, image_ids, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v4(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v5(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v5_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v5, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v5(ioctx, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v5(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

int xRBD::list_info_v5(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v5_t, int>>* infos) {
  int r = 0;
  infos->clear();
  std::map<std::string, std::pair<librbd::xImageInfo_v5, int>> tinfos;
  r = librbd::api::xImage<>::list_info_v5(ioctx, image_ids, &tinfos);
  if (r < 0) {
    return r;
  }
  for (auto& it : tinfos) {
    auto& info = (*infos)[it.first].first;
    auto& r = (*infos)[it.first].second;

    auto& tinfo = it.second.first;
    auto& tr = it.second.second;

    // info
    cvt_image_info_v5(tinfo, &info);
    // error code
    r = tr;
  }
  return r;
}

//
// xChild
//
int xRBD::child_list(librados::IoCtx& ioctx,
    std::map<parent_spec_t, std::vector<std::string>>* children) {
  int r = 0;
  children->clear();
  std::map<librbd::ParentSpec, std::set<std::string>> tchildren;
  r = librbd::api::xChild<>::list(ioctx, &tchildren);
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
int xRBD::trash_list(librados::IoCtx& ioctx,
    std::map<std::string, trash_info_t>* trashes) {
  int r = 0;
  trashes->clear();
  std::map<std::string, librbd::xTrashInfo> ttrashes;
  r = librbd::api::xTrash<>::list(ioctx, &ttrashes);
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
