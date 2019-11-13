// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_API_xCHILD_H
#define LIBRBD_API_xCHILD_H

#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.hpp"
#include "include/rbd/librbdx.hpp"
#include "librbd/Types.h"

#include <map>
#include <set>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct xChild {

  static int list(librados::IoCtx& ioctx,
      std::map<librbdx::parent_spec_t, std::vector<std::string>>* children);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::xChild<librbd::ImageCtx>;

#endif // LIBRBD_API_xCHILD_H
