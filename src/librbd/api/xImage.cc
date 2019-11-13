// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/xImage.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/ObjectMap.h"
#include "cls/rbd/cls_rbd_client.h"

#include <stdexcept>

#define dout_subsys ceph_subsys_rbd

namespace {

const uint64_t MAX_METADATA_ITEMS = 128;

using children_t = std::map<librbdx::parent_t, std::set<librbdx::child_t>>;

std::pair<int64_t, int64_t> calc_du(BitVector<2>& object_map,
    uint64_t size, uint8_t order) {
  int64_t used = 0;
  int64_t dirty = 0;

  int64_t left = size;
  int64_t object_size = (1ull << order);

  auto it = object_map.begin();
  auto end_it = object_map.end();
  while (it != end_it) {
    int64_t len = min(object_size, left);
    if (*it == OBJECT_EXISTS) { // if fast-diff is disabled then `used` equals `dirty`
      used += len;
      dirty += len;
    } else if (*it == OBJECT_EXISTS_CLEAN) {
      used += len;
    }

    ++it;
    left -= len;
  }
  return std::make_pair(used, dirty);
}

// refer to parent_key in cls_rbd.cc
librbdx::parent_t parent_from_key(string key) {
  librbdx::parent_t parent;
  bufferlist bl;
  bl.push_back(buffer::copy(key.c_str(), key.length()));
  auto it = bl.cbegin();
  decode(parent.pool_id, it);
  // cross namespace clone is disabled for clone v1
  // so parent.pool_namespace has to be derived from its children
  decode(parent.image_id, it);
  decode(parent.snap_id, it);
  return parent;
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::children: " \
    << __func__ << ": "

int list_children_per_pool(librados::IoCtx& ioctx, children_t* out_children) {
  CephContext* cct((CephContext*)ioctx.cct());
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  auto pool_id = ioctx.get_id();
  auto pool_namespace = ioctx.get_namespace();

  bool more_entries;
  uint64_t max_read = 512;
  std::string last_read = "";
  do {
    std::map<std::string, bufferlist> entries;
    int r = ioctx.omap_get_vals(RBD_CHILDREN, last_read, max_read, &entries);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing " << ioctx.get_id()
          << "/RBD_CHILDREN omap entries: "
          << cpp_strerror(r)
          << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }
    if (entries.empty()) {
      break;
    }

    for (const auto& entry : entries) {
      // decode to parent
      auto parent = parent_from_key(entry.first);
      // cross namespace clone is disabled for clone v1
      // set parent namespace from its children
      parent.pool_namespace = pool_namespace;

      auto& children = (*out_children)[parent];

      std::set<std::string> children_ids;
      decode(children_ids, entry.second);
      for (auto& cid : children_ids) {
        children.insert({
          .pool_id = pool_id,
          .pool_namespace = pool_namespace,
          .image_id = std::move(cid),
        });
      }
    }

    last_read = entries.rbegin()->first;
    more_entries = (entries.size() >= max_read);
  } while (more_entries);
  return 0;
}

//
// list legacy clone v1 children for all pools
//
int list_children(librados::IoCtx& ioctx, children_t* out_children) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  librados::Rados rados(ioctx);

  // search all pools to list clone v1 children
  std::list<std::pair<int64_t, std::string>> pools;
  int r = rados.pool_list2(pools);
  if (r < 0) {
    lderr(cct) << "error listing pools: " << cpp_strerror(r) << dendl;
    return r;
  }

  // pool_id -> pool specific children, for merging
  std::map<int64_t, children_t> all_children;

  for (auto& it : pools) {
    int64_t base_tier;
    r = rados.pool_get_base_tier(it.first, &base_tier);
    if (r == -ENOENT) {
      ldout(cct, 1) << "pool " << it.second << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      lderr(cct) << "error retrieving base tier for pool " << it.second
          << dendl;
      return r;
    }
    if (it.first != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    librados::IoCtx c_ioctx;
    // pool namespace inherited, clone v1 child has the same pool_namespace
    // with its parent
    r = librbd::util::create_ioctx(ioctx, it.second, it.first, {}, &c_ioctx);
    if (r == -ENOENT) {
      continue;
    } else if (r < 0) {
      return r;
    }

    r = list_children_per_pool(c_ioctx, &all_children[c_ioctx.get_id()]);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error reading list of children from pool "
          << it.second
          << dendl;
      return r;
    }
  }

  // merge per pool children
  for (auto& it : all_children) {
    auto& pool_children = it.second; // per pool children

    for (auto& it2 : pool_children) {
      auto& parent = it2.first;
      auto& children = it2.second;

      (*out_children)[parent].insert(children.begin(), children.end());
    }
  }
  return 0;
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest: " \
    << __func__ << " " << this << ": " \
    << "(id=" << m_info->id << ", snap_id=" << m_snap_id << "): "

/*
 * get `du` and `dirty` for a given head image/snap with explicitly
 * provided size info
 */
template <typename I>
class DuRequest {
public:
  DuRequest(librados::IoCtx& ioctx, Context* on_finish,
      const librbdx::image_info_t* x_info, snapid_t snap_id,
      int64_t* du, int64_t* dirty)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_ioctx(ioctx), m_on_finish(on_finish),
      m_info(x_info), m_snap_id(snap_id),
      m_du(du), m_dirty(dirty) {
    *m_du = 0;
    if (m_dirty != nullptr) {
      *m_dirty = 0;
    }
  }

  void send() {
    get_du();
  }

private:
  void complete(int r) {
    m_on_finish->complete(r);
    delete this;
  }

  void get_du() {
    if ((m_info->features & RBD_FEATURE_OBJECT_MAP) &&
        !(m_info->flags & RBD_FLAG_OBJECT_MAP_INVALID)) {
      load_object_map();
    } else {
      // todo: fallback to iterate image objects
      *m_du = -1;
      if (m_dirty != nullptr) {
        *m_dirty = -1;
      }

      complete(0);
    }
  }

  void load_object_map() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::object_map_load_start(&op);

    using klass = DuRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_load_object_map>(this);
    m_out_bl.clear();
    std::string oid(librbd::ObjectMap<>::object_map_name(
        m_info->id, m_snap_id));
    int r = m_ioctx.aio_operate(oid,
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_load_object_map(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to load object map: "
            << cpp_strerror(r)
            << dendl;
      }
      complete(r);
      return;
    }

    BitVector<2> object_map;
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::object_map_load_finish(&it, &object_map);
    if (r < 0) {
      lderr(m_cct) << "failed to decode object map: "
          << cpp_strerror(r)
          << dendl;
      complete(r);
      return;
    }

    auto du = calc_du(object_map, m_info->size, m_info->order);

    *m_du = du.first;
    if (m_dirty != nullptr) {
      *m_dirty = du.second;
    }

    complete(0);
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_ioctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const librbdx::image_info_t* m_info;
  const snapid_t m_snap_id;

  // [out]
  int64_t* m_du;
  int64_t* m_dirty;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest: " \
    << __func__ << " " << this << ": " \
    << "(id=" << m_image_id << "): "

template <typename I>
class InfoRequest : public std::enable_shared_from_this<InfoRequest<I>> {
public:
  template<typename... Args>
  static std::shared_ptr<InfoRequest> create(Args&&... args) {
    // https://github.com/isocpp/CppCoreGuidelines/issues/1205
    // https://embeddedartistry.com/blog/2017/01/11/stdshared_ptr-and-shared_from_this/
    auto ptr = std::shared_ptr<InfoRequest>(new InfoRequest(std::forward<Args>(args)...));
    ptr->on_complete();
    return ptr;
  }

public:
  void send() {
    get_info();
  }

private:
  InfoRequest(librados::IoCtx& ioctx, std::function<void(int)> on_finish,
      const std::string& image_id,
      children_t& children,
      librbdx::image_info_t* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_ioctx(ioctx),
      m_on_finish(on_finish),
      m_image_id(image_id),
      m_children(children),
      m_lock(image_id),
      m_pending_count(0),
      m_info(info),
      m_r(0) {
    // NOTE: image name is updated outside of InfoRequest
    m_info->id = m_image_id;
  }

private:
  std::function<void(int)> m_on_complete;

  void on_complete() {
    // https://forum.libcinder.org/topic/solution-calling-shared-from-this-in-the-constructor
    // https://stackoverflow.com/questions/17853212/using-shared-from-this-in-templated-classes
    m_on_complete = [lifetime = this->shared_from_this(), this](int r) mutable {
      // user callback
      m_on_finish(r);
      // release the last reference
      lifetime.reset();
    };
  }

private:
  void complete(int r) {
    m_on_complete(r);
  }

  void complete_request(int r) {
    m_lock.Lock();
    if (m_r >= 0) {
      if (r < 0 && r != -ENOENT) {
        m_r = r;
      }
    }

    ceph_assert(m_pending_count > 0);
    int count = --m_pending_count;
    m_lock.Unlock();

    if (count == 0) {
      complete(m_r);
    }
  }

  void get_info() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, "", MAX_METADATA_ITEMS);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_info>(this);
    m_out_bl.clear();
    int r = m_ioctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_info(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image info: "
            << cpp_strerror(r)
            << dendl;
      }
      complete(r);
      return;
    }

    auto order = &m_info->order;
    auto size = &m_info->size;
    auto features = &m_info->features;
    auto op_features = &m_info->op_features;
    auto flags = &m_info->flags;
    auto create_timestamp = &m_info->create_timestamp;
    auto access_timestamp = &m_info->access_timestamp;
    auto modify_timestamp = &m_info->modify_timestamp;
    auto data_pool_id = &m_info->data_pool_id;
    auto watchers = &m_info->watchers;

    std::map<snapid_t, cls::rbd::xclsSnapInfo> cls_snaps;
    cls::rbd::ParentImageSpec cls_parent;

    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::x_image_get_finish(&it, order, size,
        features, op_features, flags,
        &cls_snaps,
        &cls_parent,
        create_timestamp,
        access_timestamp,
        modify_timestamp,
        data_pool_id,
        watchers);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image info: "
          << cpp_strerror(r)
          << dendl;
      complete(r);
      return;
    }

    auto& parent = m_info->parent;
    parent.pool_id = cls_parent.pool_id;
    parent.pool_namespace = std::move(cls_parent.pool_namespace);
    parent.image_id = std::move(cls_parent.image_id);
    parent.snap_id = cls_parent.snap_id;

    auto pool_id = m_ioctx.get_id();
    auto pool_namespace = m_ioctx.get_namespace();

    auto& snaps = m_info->snaps;
    for (auto& it : cls_snaps) {
      std::set<librbdx::child_t> children;

      // clone v1
      librbdx::parent_t parent_ = {
        .pool_id = pool_id,
        .pool_namespace = pool_namespace,
        .image_id = m_image_id,
        .snap_id = uint64_t(it.first),
      };

      try {
        auto children_ = m_children.at(parent_);
        children.swap(children_);
      } catch (const std::out_of_range&) {
        // pass
      }

      // clone v2
      for (auto& c : it.second.children) {
        children.insert({
          .pool_id = c.pool_id,
          .pool_namespace = std::move(c.pool_namespace),
          .image_id = std::move(c.image_id),
        });
      }

      snaps.emplace(uint64_t(it.first), librbdx::snap_info_t{
          .id = it.second.id,
          .name = it.second.name,
          .snap_type = static_cast<librbdx::snap_type_t>(
              cls::rbd::get_snap_namespace_type(it.second.snapshot_namespace)),
          .size = it.second.image_size,
          .flags = it.second.flags,
          .protection_status = static_cast<librbdx::snap_protection_status_t>(
              it.second.protection_status),
          .timestamp = it.second.timestamp,
          .children = std::move(children),
          // .du and .dirty will be populated by get_dus()
      });
    }

    std::map<std::string, bufferlist> raw_metas;
    r = librbd::cls_client::metadata_list_finish(&it, &raw_metas);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image metas: "
          << cpp_strerror(r)
          << dendl;
      complete(r);
      return;
    }

    auto metas = &m_info->metas;

    for (auto& it : raw_metas) {
      std::string val(it.second.c_str(), it.second.length());
      metas->insert({it.first, val});
    }

    if (!raw_metas.empty()) {
      m_last_meta_key = raw_metas.rbegin()->first;
      get_metas();
      return;
    }

    get_dus();
  }

  void get_metas() {
    ldout(m_cct, 10) << "start_key=" << m_last_meta_key << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::metadata_list_start(&op, m_last_meta_key, MAX_METADATA_ITEMS);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_metas>(this);
    m_out_bl.clear();
    int r = m_ioctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_metas(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image metas: "
            << cpp_strerror(r)
            << dendl;
      }
      complete(r);
      return;
    }

    auto metas = &m_info->metas;

    std::map<std::string, bufferlist> raw_metas;
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::metadata_list_finish(&it, &raw_metas);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image metas: "
          << cpp_strerror(r)
          << dendl;
      complete(r);
      return;
    }
    for (auto& it : raw_metas) {
      std::string val(it.second.c_str(), it.second.length());
      metas->insert({it.first, val});
    }

    if (!raw_metas.empty()) {
      m_last_meta_key = raw_metas.rbegin()->first;
      get_metas();
      return;
    }

    get_dus();
  }

  void get_dus() {
    ldout(m_cct, 10) << dendl;

    m_pending_count = 1 + m_info->snaps.size();

    std::vector<uint64_t> snaps{CEPH_NOSNAP};
    snaps.reserve(m_pending_count);
    for (const auto& s : m_info->snaps) {
      snaps.push_back(s.first);
    }

    using klass = InfoRequest<I>;
    for (auto snap : snaps) {
      Context *on_finish = librbd::util::create_context_callback<klass,
          &klass::complete_request>(this);
      auto du = &m_info->du;
      int64_t* dirty = nullptr;
      if (snap != CEPH_NOSNAP) {
        du = &m_info->snaps[snap].du;
        dirty = &m_info->snaps[snap].dirty;
      }

      auto request = new DuRequest<I>(m_ioctx, on_finish,
          m_info, snap, du, dirty);
      request->send();
    }
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_ioctx;
  std::function<void(int)> m_on_finish; // user callback
  bufferlist m_out_bl;
  std::string m_last_meta_key;

  // [in]
  const std::string m_image_id;
  // https://timsong-cpp.github.io/cppwp/n4861/container.requirements.dataraces#2
  children_t& m_children;

  // put after m_image_id to prevent compiler warning
  Mutex m_lock;
  int m_pending_count;

  // [out]
  librbdx::image_info_t* m_info;
  int m_r;
};

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int xImage<I>::get_info(librados::IoCtx& ioctx,
    const std::string& image_id, librbdx::image_info_t* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  children_t children;
  int r = list_children(ioctx, &children);
  if (r < 0) {
    return r;
  }

  C_SaferCond cond;
  auto on_finish = [&cond](int r) {
    cond.complete(r);
  };

  auto req = InfoRequest<I>::create(ioctx, on_finish, image_id, children, info);
  req->send();

  r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 8) << "latency: "
      << latency.sec() << "s/"
      << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list(librados::IoCtx& ioctx,
    std::map<std::string, std::string>* images) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  bool more_entries;
  uint32_t max_read = 1024;
  std::string last_read = "";
  do {
    std::map<std::string, std::string> page;
    int r = cls_client::dir_list(&ioctx, RBD_DIRECTORY,
        last_read, max_read, &page);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error listing rbd image entries: "
          << cpp_strerror(r)
          << dendl;
      return r;
    } else if (r == -ENOENT) {
      break;
    }

    if (page.empty()) {
      break;
    }

    for (const auto& entry : page) {
      // map<id, name>
      images->insert({entry.second, entry.first});
    }
    last_read = page.rbegin()->first;
    more_entries = (page.size() >= max_read);
  } while (more_entries);

  latency = ceph_clock_now() - latency;
  ldout(cct, 8) << "latency: "
      << latency.sec() << "s/"
      << latency.usec() << "us" << dendl;

  return 0;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  r = list_info(ioctx, images, infos);
  return r;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // std::map<librbdx::parent_t, std::set<librbdx::child_t>>
  children_t children;
  int r = list_children(ioctx, &children);
  if (r < 0) {
    return r;
  }

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_query_ops");
  SimpleThrottle throttle(ops, false); // we never error out in throttle
  for (const auto& image : images) {
    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;

    // update image name outside of InfoRequest
    info.name = image.second;

    auto on_finish = [&throttle, &r](int r_) {
      r = r_;
      throttle.end_op(0);
    };
    auto req = InfoRequest<I>::create(ioctx, on_finish, id, children, &info);
    throttle.start_op();

    req->send();
  }

  // return code should always be 0, we have error code for each op
  throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 8) << "latency: "
      << latency.sec() << "s/"
      << latency.usec() << "us" << dendl;

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xImage<librbd::ImageCtx>;
