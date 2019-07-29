// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_xPOOL_H
#define CEPH_LIBRBD_API_xPOOL_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include <map>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct xPool {

  static int get_stats(librados::IoCtx& io_ctx);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xPool<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_xPOOL_H
