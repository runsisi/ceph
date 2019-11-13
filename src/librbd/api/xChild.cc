// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/xChild.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xChild: " << __func__ << ": "

namespace librbd {
namespace api {

namespace {
  // for legacy v1 clone relationship only
  // this relationship stores in RBD_CHILDREN object from child pool
  struct xParentSpec {
    int64_t pool_id;
    std::string image_id;
    snapid_t snap_id;

    xParentSpec() : pool_id(-1), snap_id(CEPH_NOSNAP) {
    }
    xParentSpec(int64_t pool_id, std::string image_id, snapid_t snap_id)
      : pool_id(pool_id), image_id(image_id), snap_id(snap_id) {
    }

    bool operator==(const xParentSpec &other) {
      return ((this->pool_id == other.pool_id) &&
              (this->image_id == other.image_id) &&
              (this->snap_id == other.snap_id));
    }
    bool operator!=(const xParentSpec &other) {
      return !(*this == other);
    }
    bool operator<(const xParentSpec &other) const {
      return ((pool_id < other.pool_id) ||
              (image_id < other.image_id) ||
              (snap_id < other.snap_id));
    }
  };

  // see parent_key from cls_rbd.cc
  xParentSpec parent_from_key(string key) {
    xParentSpec parent;
    bufferlist bl;
    bl.push_back(buffer::copy(key.c_str(), key.length()));
    auto it = bl.cbegin();
    decode(parent.pool_id, it);
    decode(parent.image_id, it);
    decode(parent.snap_id, it);
    return parent;
  }
}

template <typename I>
int xChild<I>::list(librados::IoCtx& ioctx,
    std::map<librbdx::parent_spec_t, std::vector<std::string>>* children) {
  CephContext* cct((CephContext*)ioctx.cct());
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  bool more_entries;
  uint32_t max_read = 1024;
  std::string last_read = "";
  do {
    map<std::string, std::set<std::string>> page;
    int r = cls_client::x_child_list(&ioctx,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing rbd child entries: " << cpp_strerror(r)
                 << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    if (page.empty()) {
      break;
    }

    for (const auto& entry : page) {
      // decode to parent
      auto x_parent = parent_from_key(entry.first);

      librbdx::parent_spec_t parent;
      parent.pool_id = x_parent.pool_id;
      parent.image_id = std::move(x_parent.image_id);
      parent.snap_id = std::move(x_parent.snap_id);

      auto& children_ = (*children)[parent];
      for (auto& c : entry.second) {
        children_.push_back(std::move(c));
      }
    }
    last_read = page.rbegin()->first;
    more_entries = (page.size() >= max_read);
  } while (more_entries);

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xChild<librbd::ImageCtx>;
