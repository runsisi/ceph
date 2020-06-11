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

constexpr uint64_t MAX_METADATA_ITEMS = 128;
constexpr int64_t INVALID_DU = -1;

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
      auto it = entry.second.cbegin();
      decode(children_ids, it);
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
    // as its parent
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
    << "(image_id=" << m_image_id << ", snap_id=" << m_snap_id << "): "

/*
 * get `du` and `dirty` for a given head image/snap with explicitly
 * provided size info
 */
template <typename I>
class DuRequest {
public:
  DuRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id, snapid_t snap_id,
      uint64_t features, uint64_t flags,
      uint64_t size, uint8_t order,
      int64_t* du, int64_t* dirty)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_ioctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id), m_snap_id(snap_id),
      m_features(features), m_flags(flags),
      m_size(size), m_order(order),
      m_du(du), m_dirty(dirty) {
    *m_du = INVALID_DU;
    *m_dirty = INVALID_DU;
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
    if ((m_features & RBD_FEATURE_OBJECT_MAP) &&
        !(m_flags & RBD_FLAG_OBJECT_MAP_INVALID)) {
      load_object_map();
    } else {
      // todo: fallback to iterate image objects?
      *m_du = INVALID_DU;
      *m_dirty = INVALID_DU;

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
        m_image_id, m_snap_id));
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

    auto du = calc_du(object_map, m_size, m_order);

    *m_du = du.first;
    *m_dirty = du.second;

    complete(0);
  }

private:
  CephContext* m_cct = nullptr;
  librados::IoCtx& m_ioctx;
  Context* m_on_finish = nullptr;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;
  const snapid_t m_snap_id;
  uint64_t m_features = 0;
  uint64_t m_flags = 0;
  uint64_t m_size = 0;
  uint8_t m_order = 0;

  // [out]
  int64_t* m_du = nullptr;
  int64_t* m_dirty = nullptr;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest: " \
    << __func__ << " " << this << ": " \
    << "(name=" << m_image_name << ", id=" << m_image_id << "): "

template <typename I>
class InfoRequest : public std::enable_shared_from_this<InfoRequest<I>> {
public:
  template<typename... Args>
  static std::shared_ptr<InfoRequest> create(Args&&... args) {
    // https://github.com/isocpp/CppCoreGuidelines/issues/1205
    // https://embeddedartistry.com/blog/2017/01/11/stdshared_ptr-and-shared_from_this/
    auto ptr = std::shared_ptr<InfoRequest>(new InfoRequest(std::forward<Args>(args)...));
    ptr->init();
    return ptr;
  }

public:
  void send() {
    if (m_flags & (~librbdx::INFO_F_ALL)) {
      complete(-EINVAL);
      return;
    }

    if (m_image_name.empty() && m_image_id.empty()) {
      complete(-EINVAL);
    } else if (!m_image_name.empty() && !m_image_id.empty()) {
      m_info->name = m_image_name;
      m_info->id = m_image_id;

      get_info();
    } else if (m_image_name.empty()) {
      m_info->id = m_image_id;

      get_name();
    } else if (m_image_id.empty()) {
      m_info->name = m_image_name;

      get_id();
    } else {
      complete(-EINVAL);
    }
  }

private:
  InfoRequest(librados::IoCtx& ioctx, std::function<void(int)> on_finish,
      const std::string& image_name,
      const std::string& image_id,
      children_t& children,
      librbdx::image_info_t* info, uint64_t flags)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_ioctx(ioctx),
      m_on_finish(on_finish),
      m_image_name(image_name),
      m_image_id(image_id),
      m_children(children),
      m_flags(flags),
      m_lock(std::move(image_name + "/" + image_id)),
      m_info(info) {
    // reset content
    m_info->snaps.clear();
    m_info->watchers.clear();
    m_info->metas.clear();
  }

private:
  std::function<void(int)> m_on_complete;

  void init() {
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

  void get_name() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::dir_get_name_start(&op, m_image_id);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_name>(this);
    m_out_bl.clear();
    int r = m_ioctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_name(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r == 0) {
      auto it = m_out_bl.cbegin();
      r = librbd::cls_client::dir_get_name_finish(&it, &m_image_name);
      m_info->name = m_image_name;
    }
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to retrieve image name: "
          << cpp_strerror(r) << dendl;
      complete(r);
    } else if (r == -ENOENT) {
      // image does not exist in directory, look in the trash bin
      ldout(m_cct, 10) << "image id " << m_image_id << " does not exist in "
          << "rbd directory, searching in rbd trash..." << dendl;
      get_name_from_trash();
    } else {
      get_info();
    }
  }

  void get_name_from_trash() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::trash_get_start(&op, m_image_id);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_name_from_trash>(this);
    m_out_bl.clear();
    int r = m_ioctx.aio_operate(RBD_TRASH, comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_name_from_trash(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    cls::rbd::TrashImageSpec trash_spec;
    if (r == 0) {
      auto it = m_out_bl.cbegin();
      r = librbd::cls_client::trash_get_finish(&it, &trash_spec);
      m_image_name = trash_spec.name;
      m_info->name = m_image_name;
    }
    if (r < 0) {
      if (r == -EOPNOTSUPP) {
        r = -ENOENT;
      }
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to retrieve name from trash: "
            << cpp_strerror(r) << dendl;
      }
      complete(r);
    } else {
      get_info();
    }
  }

  void get_id() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::get_id_start(&op);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_id>(this);
    m_out_bl.clear();
    int r = m_ioctx.aio_operate(librbd::util::id_obj_name(m_image_name),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_id(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r == 0) {
      auto it = m_out_bl.cbegin();
      r = librbd::cls_client::get_id_finish(&it, &m_image_id);
      m_info->id = m_image_id;
    }
    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to retrieve image id: " << cpp_strerror(r)
            << dendl;
      }
      complete(r);
    } else {
      get_info();
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
    // image du/dirty will be populated later
    m_info->du = INVALID_DU;
    m_info->dirty = INVALID_DU;

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

      if (m_flags & librbdx::INFO_F_CHILDREN_V1) {
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
        .name = it.second.name,
        .id = it.second.id,
        .snap_type = static_cast<librbdx::snap_type_t>(it.second.snap_type),
        .size = it.second.size,
        .flags = it.second.flags,
        .timestamp = it.second.timestamp,
        .children = std::move(children),
        // .du and .dirty will be populated by get_dus()
        .du = INVALID_DU,
        .dirty = INVALID_DU,
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

    if (!(m_flags & (librbdx::INFO_F_IMAGE_DU | librbdx::INFO_F_SNAP_DU))) {
      complete(0);
      return;
    }

    auto send_du_req = [this](librados::IoCtx& ioctx,
        const std::string& image_id, uint64_t snap_id,
        uint64_t features, uint64_t flags, uint64_t size, uint8_t order,
        int64_t* du, int64_t* dirty) {
      using klass = InfoRequest<I>;
      Context *on_finish = librbd::util::create_context_callback<klass,
          &klass::complete_request>(this);
      auto request = new DuRequest<I>(m_ioctx, on_finish, image_id,
          snap_id, features, flags, size, order, du, dirty);
      request->send();
    };

    m_pending_count = 1;
    if (m_flags & librbdx::INFO_F_SNAP_DU) {
      m_pending_count += m_info->snaps.size();
    }

    send_du_req(m_ioctx, m_image_id, CEPH_NOSNAP,
        m_info->features, m_info->flags,
        m_info->size, m_info->order, &m_info->du, &m_info->dirty);

    if (m_flags & librbdx::INFO_F_SNAP_DU) {
      for (auto it : m_info->snaps) {
        auto snap_id = it.first;
        auto snap = it.second;

        auto du = &m_info->snaps[snap_id].du;
        auto dirty = &m_info->snaps[snap_id].dirty;

        send_du_req(m_ioctx, m_image_id, snap_id,
            RBD_FEATURE_OBJECT_MAP, snap.flags,
            snap.size, m_info->order, du, dirty);
      }
    }
  }

private:
  CephContext* m_cct = nullptr;
  librados::IoCtx& m_ioctx;
  std::function<void(int)> m_on_finish; // user callback
  bufferlist m_out_bl;
  std::string m_last_meta_key;

  // [in]
  std::string m_image_name;
  std::string m_image_id;
  // https://timsong-cpp.github.io/cppwp/n4861/container.requirements.dataraces#2
  children_t& m_children;
  uint64_t m_flags = 0;

  Mutex m_lock; // put after m_image_id to prevent compiler warning
  int m_pending_count = 0;

  // [out]
  librbdx::image_info_t* m_info = nullptr;
  int m_r = 0;
};

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int xImage<I>::get_info(librados::IoCtx& ioctx,
    const std::string& image_name,
    const std::string& image_id,
    librbdx::image_info_t* info,
    uint64_t flags) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  children_t children;

  if (flags & librbdx::INFO_F_CHILDREN_V1) {
    int r = list_children(ioctx, &children);
    if (r < 0) {
      return r;
    }
  }

  C_SaferCond cond;
  auto on_finish = [&cond](int r) {
    cond.complete(r);
  };

  auto req = InfoRequest<I>::create(ioctx, on_finish, image_name, image_id,
      children, info, flags);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 5) << "latency: "
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
  ldout(cct, 5) << "latency: "
      << latency.sec() << "s/"
      << latency.usec() << "us" << dendl;

  return 0;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos,
    uint64_t flags) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  r = list_info(ioctx, images, infos, flags);
  return r;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos,
    uint64_t flags) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // std::map<librbdx::parent_t, std::set<librbdx::child_t>>
  children_t children;

  if (flags & librbdx::INFO_F_CHILDREN_V1) {
    int r = list_children(ioctx, &children);
    if (r < 0) {
      return r;
    }
  }

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_query_ops");
  SimpleThrottle throttle(ops, false); // we never error out in throttle
  for (const auto& image : images) {
    auto& id = image.first;
    auto& name = image.second;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;

    auto on_finish = [&throttle, &r](int r_) {
      r = r_;
      throttle.end_op(0);
    };
    auto req = InfoRequest<I>::create(ioctx, on_finish, name, id,
        children, &info, flags);
    throttle.start_op();

    req->send();
  }

  // return code should always be 0, we have error code for each op
  throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 5) << "latency: "
      << latency.sec() << "s/"
      << latency.usec() << "us" << dendl;

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::xImage<librbd::ImageCtx>;
