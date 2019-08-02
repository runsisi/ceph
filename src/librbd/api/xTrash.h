// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_xTRASH_H
#define LIBRBD_API_xTRASH_H

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/Types.h"

#include <map>

namespace librados { struct IoCtx; }

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct xTrash {

  static int list(librados::IoCtx& ioctx,
      std::map<std::string, xTrashInfo>* trashes);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xTrash<librbd::ImageCtx>;

#endif // LIBRBD_API_xTRASH_H
