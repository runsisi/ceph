// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/xTrash.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"

namespace {

void cvt_trash_info(librbd::xTrashInfo& in, librbdx::trash_info_t* out) {
  out->id = std::move(in.id);
  out->name = std::move(in.name);
  out->source = static_cast<librbdx::trash_source_t>(in.source);
  in.deletion_time.to_timespec(&out->deletion_time);
  in.deferment_end_time.to_timespec(&out->deferment_end_time);
}

}

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xTrash: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int xTrash<I>::list(librados::IoCtx& ioctx,
    std::map<std::string, librbdx::trash_info_t>* trashes) {
  CephContext* cct((CephContext*)ioctx.cct());
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  bool more_entries;
  uint32_t max_read = 1024;
  std::string last_read = "";
  do {
    map<std::string, cls::rbd::TrashImageSpec> page;
    int r = cls_client::trash_list(&ioctx,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing rbd trash entries: " << cpp_strerror(r)
                 << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    if (page.empty()) {
      break;
    }

    for (const auto& entry : page) {
      librbd::xTrashInfo x_info = {
        .id = entry.first,
        .name = entry.second.name,
        .source = entry.second.source,
        .deletion_time = entry.second.deletion_time,
        .deferment_end_time = entry.second.deferment_end_time
      };

      librbdx::trash_info_t info;
      cvt_trash_info(x_info, &info);

      trashes->insert({info.id, info});
    }
    last_read = page.rbegin()->first;
    more_entries = (page.size() >= max_read);
  } while (more_entries);

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xTrash<librbd::ImageCtx>;
