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

  static int get_info(librados::IoCtx& ioctx,
      const std::string& image_id, librbdx::image_info_t* info);
  static int get_info_v2(librados::IoCtx& ioctx,
      const std::string& image_id, librbdx::image_info_v2_t* info);

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

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xImage<librbd::ImageCtx>;

#endif // LIBRBD_API_xIMAGE_H
