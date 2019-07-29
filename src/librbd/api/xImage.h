// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_xIMAGE_H
#define LIBRBD_API_xIMAGE_H

#include "include/rbd/librbd.hpp"
#include "librbd/Types.h"
#include <map>

namespace librados { struct IoCtx; }

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct xImage {

  static int get_size(librados::IoCtx &io_ctx,
      const std::string &id, snapid_t snap_id, xSizeInfo *info);

  static int get_info(librados::IoCtx &io_ctx,
      const std::string &id, xImageInfo *info);
  static int get_info_v2(librados::IoCtx &io_ctx,
      const std::string &id, xImageInfo_v2 *info);

  static int get_du(librados::IoCtx &io_ctx,
      const std::string &image_id, snapid_t snap_id, uint64_t *du);
  static int get_du_v2(librados::IoCtx &io_ctx,
      const std::string &image_id,
      std::map<snapid_t, uint64_t> *dus);
  static int get_du_sync(librados::IoCtx &io_ctx,
      const std::string &image_id, snapid_t snap_id,
      uint64_t *du);

  static int list(librados::IoCtx &io_ctx,
      std::map<std::string, std::string> *images);

  static int list_info(librados::IoCtx &io_ctx,
      std::map<std::string, std::pair<xImageInfo, int>> *infos);
  static int list_info(librados::IoCtx &io_ctx,
      const std::map<std::string, std::string> &images,
      std::map<std::string, std::pair<xImageInfo, int>> *infos);

  static int list_info_v2(librados::IoCtx &io_ctx,
      std::map<std::string, std::pair<xImageInfo_v2, int>> *infos);
  static int list_info_v2(librados::IoCtx &io_ctx,
      const std::map<std::string, std::string> &images,
      std::map<std::string, std::pair<xImageInfo_v2, int>> *infos);

  static int list_du(librados::IoCtx &io_ctx,
      std::map<std::string, std::pair<uint64_t, int>> *dus);
  static int list_du(librados::IoCtx &io_ctx,
      const std::map<std::string, std::string> &images,
      std::map<std::string, std::pair<uint64_t, int>> *dus);

  static int list_du_v2(librados::IoCtx &io_ctx,
      std::map<std::string, std::pair<std::map<snapid_t, uint64_t>, int>> *dus);
  static int list_du_v2(librados::IoCtx &io_ctx,
      const std::map<std::string, std::string> &images,
      std::map<std::string, std::pair<std::map<snapid_t, uint64_t>, int>> *dus);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xImage<librbd::ImageCtx>;

#endif // LIBRBD_API_xIMAGE_H
