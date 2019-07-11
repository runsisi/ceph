// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_zTRASH_H
#define LIBRBD_API_zTRASH_H

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include <map>

namespace librados { struct IoCtx; }

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct zTrash {

  static int list(librados::IoCtx &io_ctx,
      std::map<std::string, trash_image_info_t> *trashes);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::zTrash<librbd::ImageCtx>;

#endif // LIBRBD_API_zTRASH_H
