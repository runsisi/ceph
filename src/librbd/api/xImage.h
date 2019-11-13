// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_xIMAGE_H
#define LIBRBD_API_xIMAGE_H

#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.hpp"
#include "include/rbd/librbdx.hpp"
#include "librbd/Types.h"

#include <map>
#include <vector>

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
      const std::string& image_id, uint64_t snap_id, librbdx::size_info_t* info);

  static int get_du(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id, librbdx::du_info_t* info);
  static int get_du_v2(librados::IoCtx& ioctx,
      const std::string& image_id,
      std::map<uint64_t, librbdx::du_info_t>* infos);
  static int get_du_sync(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id,
      librbdx::du_info_t* info);

  static int get_info(librados::IoCtx& ioctx,
      const std::string& image_id, librbdx::image_info_t* info);
  static int get_info_v2(librados::IoCtx& ioctx,
      const std::string& image_id, librbdx::image_info_v2_t* info);
  static int get_info_v3(librados::IoCtx& ioctx,
      const std::string& image_id, librbdx::image_info_v3_t* info);

  static int list_du(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<librbdx::du_info_t, int>>* infos);
  static int list_du(librados::IoCtx& ioctx,
      const std::vector<std::string>& images_ids,
      std::map<std::string, std::pair<librbdx::du_info_t, int>>* infos);

  static int list_du_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<std::map<uint64_t, librbdx::du_info_t>, int>>* infos);
  static int list_du_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<std::map<uint64_t, librbdx::du_info_t>, int>>* infos);

  static int list(librados::IoCtx& ioctx,
      std::map<std::string, std::string>* images);

  static int list_info(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos);
  static int list_info(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos);

  static int list_info_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<librbdx::image_info_v2_t, int>>* infos);
  static int list_info_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<librbdx::image_info_v2_t, int>>* infos);

  static int list_info_v3(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<librbdx::image_info_v3_t, int>>* infos);
  static int list_info_v3(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<librbdx::image_info_v3_t, int>>* infos);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xImage<librbd::ImageCtx>;

#endif // LIBRBD_API_xIMAGE_H
