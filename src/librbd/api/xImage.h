// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_xIMAGE_H
#define LIBRBD_API_xIMAGE_H

#include "include/rbd/librbd.hpp"
#include "librbd/Types.h"

#include <map>
#include <vector>

namespace librados { struct IoCtx; }

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct xImage {

  static int get_name(librados::IoCtx& ioctx,
      const std::string& image_id, std::string* name);
  static int get_id(librados::IoCtx& ioctx,
      const std::string& image_name, std::string* id);

  static int get_size(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id, xSizeInfo* info);

  static int get_du(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id, xDuInfo* info);
  static int get_du_v2(librados::IoCtx& ioctx,
      const std::string& image_id,
      std::map<uint64_t, xDuInfo>* infos);
  static int get_du_sync(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id,
      xDuInfo* info);

  static int get_info(librados::IoCtx& ioctx,
      const std::string& image_id, xImageInfo* info);
  static int get_info_v2(librados::IoCtx& ioctx,
      const std::string& image_id, xImageInfo_v2* info);
  static int get_info_v3(librados::IoCtx& ioctx,
      const std::string& image_id, xImageInfo_v3* info);
  static int get_info_v4(librados::IoCtx& ioctx,
      const std::string& image_id, xImageInfo_v4* info);
  static int get_info_v5(librados::IoCtx& ioctx,
      const std::string& image_id, xImageInfo_v5* info);

  static int list_du(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xDuInfo, int>>* infos);
  static int list_du(librados::IoCtx& ioctx,
      const std::vector<std::string>& images_ids,
      std::map<std::string, std::pair<xDuInfo, int>>* infos);

  static int list_du_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<std::map<uint64_t, xDuInfo>, int>>* infos);
  static int list_du_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<std::map<uint64_t, xDuInfo>, int>>* infos);

  static int list(librados::IoCtx& ioctx,
      std::map<std::string, std::string>* images);

  static int list_info(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xImageInfo, int>>* infos);
  static int list_info(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<xImageInfo, int>>* infos);

  static int list_info_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xImageInfo_v2, int>>* infos);
  static int list_info_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<xImageInfo_v2, int>>* infos);

  static int list_info_v3(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xImageInfo_v3, int>>* infos);
  static int list_info_v3(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<xImageInfo_v3, int>>* infos);

  static int list_info_v4(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xImageInfo_v4, int>>* infos);
  static int list_info_v4(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<xImageInfo_v4, int>>* infos);

  static int list_info_v5(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<xImageInfo_v5, int>>* infos);
  static int list_info_v5(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<xImageInfo_v5, int>>* infos);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xImage<librbd::ImageCtx>;

#endif // LIBRBD_API_xIMAGE_H
