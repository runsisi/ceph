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
  std::string parent_key(uint64_t pool_id, std::string image_id,
      snapid_t snap_id) {
    bufferlist key_bl;
    ::encode(pool_id, key_bl);
    ::encode(image_id, key_bl);
    ::encode(snap_id, key_bl);
    return std::string(key_bl.c_str(), key_bl.length());
  }

  ParentSpec parent_from_key(string key) {
    ParentSpec parent;
    bufferlist bl;
    bl.push_back(buffer::copy(key.c_str(), key.length()));
    auto it = bl.begin();
    ::decode(parent.pool_id, it);
    ::decode(parent.image_id, it);
    ::decode(parent.snap_id, it);
    return parent;
  }
}

template <typename I>
int xChild<I>::list(librados::IoCtx& ioctx,
    std::map<ParentSpec, std::set<std::string>>* children) {
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
      auto parent = parent_from_key(entry.first);

      children->insert({parent, entry.second});
    }
    last_read = page.rbegin()->first;
    more_entries = (page.size() >= max_read);
  } while (more_entries);

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xChild<librbd::ImageCtx>;
