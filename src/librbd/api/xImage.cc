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
#include "librbd/api/xTrash.h"

#define dout_subsys ceph_subsys_rbd

namespace {

const std::string RBD_QOS_PREFIX = "conf_rbd_client_qos_";
const uint64_t MAX_METADATA_ITEMS = 128;

uint64_t calc_du(uint64_t snap_id, BitVector<2>& object_map,
    uint64_t size, uint8_t order) {
  uint64_t used = 0;
  uint64_t left = size;
  uint64_t object_size = (1ull << order);

  auto it = object_map.begin();
  auto end_it = object_map.end();
  if (snap_id == CEPH_NOSNAP) {
    while (it != end_it) {
      uint64_t len = min(object_size, left);
      if (*it == OBJECT_EXISTS || *it == OBJECT_EXISTS_CLEAN) {
        used += len;
      }

      ++it;
      left -= len;
    }
  } else {
    while (it != end_it) {
      uint64_t len = min(object_size, left);
      if (*it == OBJECT_EXISTS) {
        used += len;
      }

      ++it;
      left -= len;
    }
  }
  return used;
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::NameRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class NameRequest {
public:
  NameRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      std::string* name)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_name(name) {
  }

  void send() {
    get_name();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;

  // [out]
  std::string* m_name;

  void get_name() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::dir_get_name_start(&op, m_image_id);

    using klass = NameRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_name>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(RBD_DIRECTORY,
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_name(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to get image name: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    if (r == -ENOENT) {
      get_name_from_trash();
    } else {
      auto it = m_out_bl.begin();
      r = librbd::cls_client::dir_get_name_finish(&it, m_name);
      if (r < 0) {
        lderr(m_cct) << "failed to decode image name: "
                     << cpp_strerror(r)
                     << dendl;
        finish(r);
        return;
      }

      finish(0);
    }
  }

  void get_name_from_trash() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::trash_get_start(&op, m_image_id);

    using klass = NameRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_name_from_trash>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(RBD_TRASH, comp, &op, &m_out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_name_from_trash(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image name from trash: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    cls::rbd::TrashImageSpec trash_spec;
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::trash_get_finish(&it, &trash_spec);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image name: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::IdRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_name << "): "

template <typename I>
class IdRequest {
public:
  IdRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_name,
      std::string* id)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_name(image_name),
      m_id(id) {
  }

  void send() {
    get_id();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_name;

  // [out]
  std::string* m_id;

  void get_id() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::get_id_start(&op);

    using klass = IdRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_id>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::id_obj_name(m_image_name),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_id(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image name: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto it = m_out_bl.begin();
    r = librbd::cls_client::get_id_finish(&it, m_id);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image id: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::SizeRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class SizeRequest {
public:
  SizeRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      uint64_t snap_id,
      librbd::xSizeInfo* size_info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_snap_id(snap_id),
      m_size_info(size_info) {
    // NOTE: image name is not set
    m_size_info->image_id = m_image_id;
    m_size_info->snap_id = m_snap_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;
  uint64_t m_snap_id;

  // [out]
  librbd::xSizeInfo* m_size_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_size_get_start(&op, m_snap_id);

    using klass = SizeRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_head>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image head: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto order = &m_size_info->order;
    auto size = &m_size_info->size;
    auto stripe_unit = &m_size_info->stripe_unit;
    auto stripe_count = &m_size_info->stripe_count;
    auto features = &m_size_info->features;
    auto flags = &m_size_info->flags;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_size_get_finish(&it, order, size,
        stripe_unit, stripe_count, features, flags);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image size: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class DuRequest {
public:
  DuRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      uint64_t snap_id,
      librbd::xDuInfo* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_snap_id(snap_id),
      m_info(info) {
    m_size_info.image_id = m_image_id;
    m_size_info.snap_id = m_snap_id;

    m_info->size = 0;
    m_info->du = 0;
  }

  void send() {
    get_size();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;
  uint64_t m_snap_id;

  // [out]
  librbd::xSizeInfo m_size_info;
  librbd::xDuInfo* m_info;

  void get_size() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_size_get_start(&op, m_snap_id);

    using klass = DuRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_size>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_size(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image size info: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto order = &m_size_info.order;
    auto size = &m_size_info.size;
    auto stripe_unit = &m_size_info.stripe_unit;
    auto stripe_count = &m_size_info.stripe_count;
    auto features = &m_size_info.features;
    auto flags = &m_size_info.flags;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_size_get_finish(&it, order, size,
        stripe_unit, stripe_count, features, flags);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image size: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    get_du();
  }

  void get_du() {
    load_object_map(m_snap_id);
  }

  void load_object_map(uint64_t snap_id) {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::object_map_load_start(&op);

    using klass = DuRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_load_object_map>(this);
    m_out_bl.clear();
    std::string oid(librbd::ObjectMap<>::object_map_name(m_image_id, snap_id));
    int r = m_io_ctx.aio_operate(oid,
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
      finish(r);
      return;
    }

    BitVector<2> object_map;
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::object_map_load_finish(&it, &object_map);
    if (r < 0) {
      lderr(m_cct) << "failed to decode object map: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    if ((m_size_info.features & RBD_FEATURE_FAST_DIFF) &&
        !(m_size_info.flags & RBD_FLAG_FAST_DIFF_INVALID)) {
      m_info->size = m_size_info.size;
      m_info->du = calc_du(m_snap_id, object_map, m_size_info.size, m_size_info.order);
    } else {
      // todo: fallback to iterate image objects
      m_info->size = m_size_info.size;
      m_info->du = 0;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class DuRequest_v2 {
public:
  DuRequest_v2(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      std::map<uint64_t, librbd::xDuInfo>* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_lock(image_id),
      m_pending_count(0),
      m_info(info),
      m_r(0){
    m_info->clear();
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

  void complete_request(int r) {
    m_lock.Lock();
    if (m_r >= 0) {
      if (r < 0 && r != -ENOENT) {
        m_r = r;
      }
    }

    assert(m_pending_count > 0);
    int count = --m_pending_count;
    m_lock.Unlock();

    if (count == 0) {
      finish(m_r);
    }
  }

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_snapc_get_start(&op);

    using klass = DuRequest_v2<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_head>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image snapc: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto snapc = &m_snapc;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_snapc_get_finish(&it, snapc);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image snapc: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    if (!snapc->is_valid()) {
      lderr(m_cct) << "snap context is invalid" << dendl;
      finish(-EINVAL);
      return;
    }

    get_dus();
  }

  void get_dus() {
    ldout(m_cct, 10) << dendl;

    m_pending_count = 1 + m_snapc.snaps.size();

    std::vector<uint64_t> snaps{CEPH_NOSNAP};
    snaps.reserve(m_pending_count);
    snaps.insert(snaps.end(), m_snapc.snaps.begin(), m_snapc.snaps.end());

    for (auto snap : snaps) {
      Context *on_complete = librbd::util::create_context_callback<DuRequest_v2<I>,
          &DuRequest_v2<I>::complete_request>(this);
      auto& info = (*m_info)[snap];
      auto request = new DuRequest<I>(m_io_ctx, on_complete,
          m_image_id, snap, &info);
      request->send();
    }
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;

  // [out]
  SnapContext m_snapc;
  Mutex m_lock;
  int m_pending_count;
  std::map<uint64_t, librbd::xDuInfo>* m_info;
  int m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest_v3: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_size_info.image_id << "): "

template <typename I>
class DuRequest_v3 {
public:
  DuRequest_v3(librados::IoCtx& ioctx, Context* on_finish,
      librbd::xSizeInfo& size_info,
      uint64_t* du)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_size_info(size_info),
      m_du(du) {
    std::swap(m_size_info, size_info);
    *m_du = 0;
  }

  void send() {
    get_du();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;

  // [in]
  librbd::xSizeInfo m_size_info;

  // [out]
  uint64_t* m_du;

  void get_du() {
    load_object_map();
  }

  void load_object_map() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::object_map_load_start(&op);

    using klass = DuRequest_v3<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_load_object_map>(this);
    m_out_bl.clear();
    std::string oid(librbd::ObjectMap<>::object_map_name(
        m_size_info.image_id, m_size_info.snap_id));
    int r = m_io_ctx.aio_operate(oid,
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
      finish(r);
      return;
    }

    BitVector<2> object_map;
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::object_map_load_finish(&it, &object_map);
    if (r < 0) {
      lderr(m_cct) << "failed to decode object map: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    if ((m_size_info.features & RBD_FEATURE_FAST_DIFF) &&
        !(m_size_info.flags & RBD_FLAG_FAST_DIFF_INVALID)) {
      *m_du = calc_du(m_size_info.snap_id, object_map,
          m_size_info.size, m_size_info.order);
    } else {
      // todo: fallback to iterate image objects
      *m_du = 0;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class InfoRequest {
public:
  InfoRequest(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      librbd::xImageInfo* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_info(info) {
    // NOTE: image name is not set
    m_info->id = m_image_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;
  std::map<snapid_t, cls::rbd::xSnapInfo> m_cls_snaps;

  // [in]
  const std::string m_image_id;

  // [out]
  librbd::xImageInfo* m_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, RBD_QOS_PREFIX,
        MAX_METADATA_ITEMS);

    using klass = InfoRequest<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_head>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image head: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto order = &m_info->order;
    auto size = &m_info->size;
    auto stripe_unit = &m_info->stripe_unit;
    auto stripe_count = &m_info->stripe_count;
    auto features = &m_info->features;
    auto flags = &m_info->flags;
    auto snapc = &m_info->snapc;
    auto parent = &m_info->parent;
    auto timestamp = &m_info->timestamp;
    auto data_pool_id = &m_info->data_pool_id;
    auto watchers = &m_info->watchers;
    auto kvs = &m_info->kvs;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_image_get_finish(&it, order, size,
        stripe_unit, stripe_count,
        features, flags,
        snapc, &m_cls_snaps,
        parent, timestamp,
        data_pool_id,
        watchers);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image metadata: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }
    r = librbd::cls_client::x_metadata_list_finish(&it, kvs);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image qos kvs: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    auto& snaps = m_info->snaps;
    for (auto& it : m_cls_snaps) {
      snaps.emplace(it.first, {
          .id = it.second.id,
          .name = it.second.name,
          .snap_ns_type = it.second.snapshot_namespace.get_namespace_type(),
          .size = it.second.image_size,
          .features = it.second.features,
          .flags = it.second.flags,
          .protection_status = it.second.protection_status,
          .timestamp = it.second.timestamp,
      });
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class InfoRequest_v2 {
public:
  InfoRequest_v2(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      librbd::xImageInfo_v2* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_info(info) {
    // NOTE: image name is not set
    m_info->id = m_image_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;
  std::map<snapid_t, cls::rbd::xSnapInfo> m_cls_snaps;

  // [in]
  const std::string m_image_id;

  // [out]
  librbd::xImageInfo_v2* m_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, RBD_QOS_PREFIX,
        MAX_METADATA_ITEMS);

    using klass = InfoRequest_v2<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_head>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image head: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto order = &m_info->order;
    auto size = &m_info->size;
    auto stripe_unit = &m_info->stripe_unit;
    auto stripe_count = &m_info->stripe_count;
    auto features = &m_info->features;
    auto flags = &m_info->flags;
    auto snapc = &m_info->snapc;
    auto parent = &m_info->parent;
    auto timestamp = &m_info->timestamp;
    auto data_pool_id = &m_info->data_pool_id;
    auto watchers = &m_info->watchers;
    auto kvs = &m_info->kvs;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_image_get_finish(&it, order, size,
        stripe_unit, stripe_count,
        features, flags,
        snapc, parent, timestamp,
        data_pool_id,
        watchers);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image metadata: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }
    r = librbd::cls_client::x_metadata_list_finish(&it, kvs);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image qos kvs: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    auto& snaps = m_info->snaps;
    for (auto& it : m_cls_snaps) {
      snaps.emplace(it.first, {
          .id = it.second.id,
          .name = it.second.name,
          .snap_ns_type = it.second.snapshot_namespace.get_namespace_type(),
          .size = it.second.image_size,
          .features = it.second.features,
          .flags = it.second.flags,
          .protection_status = it.second.protection_status,
          .timestamp = it.second.timestamp,
      });
    }

    get_du();
  }

  void get_du() {
    Context *on_finish = librbd::util::create_context_callback<InfoRequest_v2<I>,
        &InfoRequest_v2<I>::handle_get_du>(this);
    auto& du = m_info->du;
    librbd::xSizeInfo size_info = {
      .image_id = m_image_id,
      .snap_id = CEPH_NOSNAP,
      .order = m_info->order,
      .size = m_info->size,
      .stripe_unit = m_info->stripe_unit,
      .stripe_count = m_info->stripe_count,
      .features = m_info->features,
      .flags = m_info->flags,
    };
    auto request = new DuRequest_v3<I>(m_io_ctx, on_finish,
        size_info, &du);
    request->send();
  }

  void handle_get_du(int r) {
    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image du: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    finish(0);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest_v3: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class InfoRequest_v3 {
public:
  InfoRequest_v3(librados::IoCtx& ioctx, Context* on_finish,
      const std::string& image_id,
      librbd::xImageInfo_v3* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_lock(image_id),
      m_pending_count(0),
      m_r(0),
      m_info(info) {
    // NOTE: image name is not set
    m_info->id = m_image_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

  void complete_request(int r) {
    m_lock.Lock();
    if (m_r >= 0) {
      if (r < 0 && r != -ENOENT) {
        m_r = r;
      }
    }

    assert(m_pending_count > 0);
    int count = --m_pending_count;
    m_lock.Unlock();

    if (count == 0) {
      finish(m_r);
    }
  }

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, RBD_QOS_PREFIX,
        MAX_METADATA_ITEMS);

    using klass = InfoRequest_v3<I>;
    auto comp = librbd::util::create_rados_callback<klass,
        &klass::handle_get_head>(this);
    m_out_bl.clear();
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_get_head(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      if (r != -ENOENT) {
        lderr(m_cct) << "failed to get image head: "
                     << cpp_strerror(r)
                     << dendl;
      }
      finish(r);
      return;
    }

    auto order = &m_info->order;
    auto size = &m_info->size;
    auto stripe_unit = &m_info->stripe_unit;
    auto stripe_count = &m_info->stripe_count;
    auto features = &m_info->features;
    auto flags = &m_info->flags;
    auto snapc = &m_info->snapc;
    auto parent = &m_info->parent;
    auto timestamp = &m_info->timestamp;
    auto data_pool_id = &m_info->data_pool_id;
    auto watchers = &m_info->watchers;
    auto kvs = &m_info->kvs;

    auto it = m_out_bl.begin();
    r = librbd::cls_client::x_image_get_finish(&it, order, size,
        stripe_unit, stripe_count,
        features, flags,
        snapc, parent, timestamp,
        data_pool_id,
        watchers);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image metadata: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }
    r = librbd::cls_client::x_metadata_list_finish(&it, kvs);
    if (r < 0) {
      lderr(m_cct) << "failed to decode image qos kvs: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    auto& snaps = m_info->snaps;
    for (auto& it : m_cls_snaps) {
      snaps.emplace(it.first, {
          .id = it.second.id,
          .name = it.second.name,
          .snap_ns_type = it.second.snapshot_namespace.get_namespace_type(),
          .size = it.second.image_size,
          .features = it.second.features,
          .flags = it.second.flags,
          .protection_status = it.second.protection_status,
          .timestamp = it.second.timestamp,
      });
    }

    if (!snapc->is_valid()) {
      lderr(m_cct) << "snap context is invalid" << dendl;
      finish(-EINVAL);
      return;
    }

    get_dus();
  }

  void get_dus() {
    ldout(m_cct, 10) << dendl;

    m_pending_count = 1 + m_info->snapc.snaps.size();

    std::vector<uint64_t> snaps{CEPH_NOSNAP};
    snaps.reserve(m_pending_count);
    snaps.insert(snaps.end(), m_info->snapc.snaps.begin(), m_info->snapc.snaps.end());

    for (auto snap : snaps) {
      Context *on_finish = librbd::util::create_context_callback<InfoRequest_v3<I>,
          &InfoRequest_v5<I>::complete_request>(this);
      auto du = &m_info->du;
      if (snap != CEPH_NOSNAP) {
        du = &m_info->snaps[snap].du;
      }
      librbd::xSizeInfo size_info = {
        .image_id = m_image_id,
        .snap_id = snap,
        .order = m_info->order,
        .size = m_info->size,
        .stripe_unit = m_info->stripe_unit,
        .stripe_count = m_info->stripe_count,
        .features = m_info->features,
        .flags = m_info->flags,
      };
      auto request = new DuRequest_v3<I>(m_io_ctx, on_finish,
          size_info, du);
      request->send();
    }
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;
  std::map<snapid_t, cls::rbd::xSnapInfo> m_cls_snaps;

  // [in]
  const std::string m_image_id;

  // [out]
  Mutex m_lock;
  int m_pending_count;
  int m_r;
  librbd::xImageInfo_v3* m_info;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledDuRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ThrottledDuRequest {
public:
  ThrottledDuRequest(librados::IoCtx& ioctx, SimpleThrottle& throttle,
      const std::string& image_id,
      librbd::xDuInfo* info, int* r)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id),
      m_r(r) {
    Context* on_finish = librbd::util::create_context_callback<ThrottledDuRequest<I>,
        &ThrottledDuRequest<I>::finish>(this);
    m_request = new DuRequest<I>(ioctx, on_finish, image_id, CEPH_NOSNAP, info);
    m_throttle.start_op();
  }

  void send() {
    m_request->send();
  }

private:
  void finish(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    *m_r = r;
    // ignore errors so throttle can continue
    r = 0;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  DuRequest<I>* m_request;
  SimpleThrottle& m_throttle;

  // [in]
  const std::string m_image_id;

  // [out]
  int* m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledDuRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ThrottledDuRequest_v2 {
public:
  ThrottledDuRequest_v2(librados::IoCtx& ioctx, SimpleThrottle& throttle,
      const std::string& image_id,
      std::map<uint64_t, librbd::xDuInfo>* info, int* r)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id),
      m_r(r) {
    Context* on_finish = librbd::util::create_context_callback<ThrottledDuRequest_v2<I>,
        &ThrottledDuRequest_v2<I>::finish>(this);
    m_request = new DuRequest_v2<I>(ioctx, on_finish, image_id, info);
    m_throttle.start_op();
  }

  void send() {
    m_request->send();
  }

private:
  void finish(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    *m_r = r;
    // ignore errors so throttle can continue
    r = 0;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  DuRequest_v2<I>* m_request;
  SimpleThrottle& m_throttle;

  // [in]
  const std::string m_image_id;

  // [out]
  int* m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledInfoRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ThrottledInfoRequest {
public:
  ThrottledInfoRequest(librados::IoCtx& ioctx, SimpleThrottle& throttle,
      const std::string& image_id,
      librbd::xImageInfo* info, int *r)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id),
      m_r(r) {
    Context* on_finish = librbd::util::create_context_callback<ThrottledInfoRequest<I>,
        &ThrottledInfoRequest<I>::finish>(this);
    m_request = new InfoRequest<I>(ioctx, on_finish, image_id, info);
    m_throttle.start_op();
  }

  void send() {
    m_request->send();
  }

private:
  void finish(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    *m_r = r;
    // ignore errors so throttle can continue
    r = 0;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  InfoRequest<I>* m_request;
  SimpleThrottle& m_throttle;

  // [in]
  const std::string m_image_id;

  // [out]
  int* m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledInfoRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ThrottledInfoRequest_v2 {
public:
  ThrottledInfoRequest_v2(librados::IoCtx& ioctx, SimpleThrottle& throttle,
      const std::string& image_id,
      librbd::xImageInfo_v2* info, int* r)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id),
      m_r(r) {
    Context* on_finish = librbd::util::create_context_callback<ThrottledInfoRequest_v2<I>,
        &ThrottledInfoRequest_v2<I>::finish>(this);
    m_request = new InfoRequest_v2<I>(ioctx, on_finish, image_id, info);
    m_throttle.start_op();
  }

  void send() {
    m_request->send();
  }

private:
  void finish(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    *m_r = r;
    // ignore errors so throttle can continue
    r = 0;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  SimpleThrottle& m_throttle;
  InfoRequest_v2<I>* m_request;

  // [in]
  const std::string m_image_id;

  // [out]
  int* m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledInfoRequest_v3: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class ThrottledInfoRequest_v3 {
public:
  ThrottledInfoRequest_v3(librados::IoCtx& ioctx, SimpleThrottle& throttle,
      const std::string& image_id,
      librbd::xImageInfo_v3* info, int* r)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id),
      m_r(r) {
    Context* on_finish = librbd::util::create_context_callback<ThrottledInfoRequest_v3<I>,
        &ThrottledInfoRequest_v3<I>::finish>(this);
    m_request = new InfoRequest_v3<I>(ioctx, on_finish, image_id, info);
    m_throttle.start_op();
  }

  void send() {
    m_request->send();
  }

private:
  void finish(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    *m_r = r;
    // ignore errors so throttle can continue
    r = 0;
    m_throttle.end_op(r);

    delete this;
  }

private:
  CephContext* m_cct;
  SimpleThrottle& m_throttle;
  InfoRequest_v3<I>* m_request;

  // [in]
  const std::string m_image_id;

  // [out]
  int* m_r;
};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int xImage<I>::get_name(librados::IoCtx& ioctx,
    const std::string& image_id, std::string* name) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  C_SaferCond cond;

  auto req = new NameRequest<I>(ioctx, &cond, image_id, name);
  req->send();

  int r = cond.wait();
  return r;
}

template <typename I>
int xImage<I>::get_id(librados::IoCtx& ioctx,
    const std::string& image_name, std::string* id) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  C_SaferCond cond;

  auto req = new IdRequest<I>(ioctx, &cond, image_name, id);
  req->send();

  int r = cond.wait();
  return r;
}

template <typename I>
int xImage<I>::get_size(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id, xSizeInfo* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  C_SaferCond cond;

  auto req = new SizeRequest<I>(ioctx, &cond, image_id, snap_id, info);
  req->send();

  int r = cond.wait();
  return r;
}

template <typename I>
int xImage<I>::get_du(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id,
    xDuInfo* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new DuRequest<I>(ioctx, &cond, image_id, snap_id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_du_v2(librados::IoCtx& ioctx,
    const std::string& image_id,
    std::map<uint64_t, xDuInfo>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new DuRequest_v2<I>(ioctx, &cond, image_id, infos);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_du_sync(librados::IoCtx& ioctx,
    const std::string& image_id, uint64_t snap_id,
    xDuInfo *info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  xSizeInfo size_info;
  int r = xImage<I>::get_size(ioctx, image_id, snap_id, &size_info);
  if (r < 0) {
    lderr(cct) << "failed to get size: " << image_id << "@" << snap_id << ", "
               << cpp_strerror(r)
               << dendl;
    return r;
  }

  if ((size_info.features & RBD_FEATURE_FAST_DIFF) &&
      !(size_info.flags & RBD_FLAG_FAST_DIFF_INVALID)) {
    BitVector<2> object_map;
    std::string oid(ObjectMap<>::object_map_name(image_id, snap_id));
    int r = librbd::cls_client::object_map_load(&ioctx, oid, &object_map);
    if (r < 0) {
      lderr(cct) << "failed to load object map: " << oid << ", "
                 << cpp_strerror(r)
                 << dendl;
      return r;
    }

    info->size = size_info.size;
    info->du = calc_du(snap_id, object_map, size_info.size, size_info.order);
  } else {
    // todo: fallback to iterate image objects
    info->size = size_info.size;
    info->du = 0;
  }

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return 0;
}

template <typename I>
int xImage<I>::get_info(librados::IoCtx& ioctx,
    const std::string& image_id, xImageInfo* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new InfoRequest<I>(ioctx, &cond, image_id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_info_v2(librados::IoCtx& ioctx,
    const std::string& image_id, xImageInfo_v2* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new InfoRequest_v2<I>(ioctx, &cond, image_id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_info_v3(librados::IoCtx& ioctx,
    const std::string& image_id, xImageInfo_v3* info) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new InfoRequest_v3<I>(ioctx, &cond, image_id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_du(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<xDuInfo, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, xTrashInfo> trashes;
//  int r = xTrash<I>::list(ioctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto& it : trashes) {
//    if (it.second.source == cls::rbd::TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledDuRequest<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_du(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<xDuInfo, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledDuRequest<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  int r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_du_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<std::map<uint64_t, xDuInfo>, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, xTrashInfo> trashes;
//  int r = xTrash<I>::list(ioctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto& it : trashes) {
//    if (it.second.source == cls::rbd::TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledDuRequest_v2<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_du_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<std::map<uint64_t, xDuInfo>, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledDuRequest_v2<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  int r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
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
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return 0;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<xImageInfo, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, xTrashInfo> trashes;
//  int r = xTrash<I>::list(ioctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto& it : trashes) {
//    if (it.second.source == cls::rbd::TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_info(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<xImageInfo, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  int r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_info_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<xImageInfo_v2, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, xTrashInfo> trashes;
//  int r = xTrash<I>::list(ioctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto& it : trashes) {
//    if (it.second.source == cls::rbd::TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v2<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_info_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<xImageInfo_v2, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v2<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  int r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_info_v3(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<xImageInfo_v3, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(ioctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, xTrashInfo> trashes;
//  int r = xTrash<I>::list(ioctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto& it : trashes) {
//    if (it.second.source == cls::rbd::TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto& id = image.first;

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v3<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::list_info_v3(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<xImageInfo_v3, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto& id : image_ids) {
    if (throttle.pending_error()) {
      break;
    }

    auto& info = (*infos)[id].first;
    auto& r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v3<I>(ioctx, throttle, id, &info, &r);
    req->send();
  }

  int r = throttle.wait_for_ret();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

} // namespace api
} // namespace librbd

template class librbd::api::xImage<librbd::ImageCtx>;
