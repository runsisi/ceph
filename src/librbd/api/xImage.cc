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

constexpr const char* conf_qos_iops_str = "conf_rbd_client_qos_limit";
constexpr const char* conf_qos_bps_str = "conf_rbd_client_qos_bandwidth";

void cvt_snap_info(librbd::xSnapInfo& in, librbdx::snap_info_t* out) {
  out->id = in.id;
  out->name = std::move(in.name);
  out->snap_ns_type = static_cast<librbdx::snap_ns_type_t>(in.snap_ns_type);
  out->size = in.size;
  out->flags = in.flags;
  out->protection_status = static_cast<librbdx::snap_protection_status_t>(in.protection_status);
  in.timestamp.to_timespec(&out->timestamp);
}

void cvt_snap_info_v2(librbd::xSnapInfo_v2& in, librbdx::snap_info_v2_t* out) {
  out->id = in.id;
  out->name = std::move(in.name);
  out->snap_ns_type = static_cast<librbdx::snap_ns_type_t>(in.snap_ns_type);
  out->size = in.size;
  out->flags = in.flags;
  out->protection_status = static_cast<librbdx::snap_protection_status_t>(in.protection_status);
  in.timestamp.to_timespec(&out->timestamp);
  out->du = in.du;
  out->dirty = in.dirty;
}

void cvt_image_info(librbd::xImageInfo& in, librbdx::image_info_t* out) {
  out->snapc.snaps.clear();
  out->snaps.clear();
  out->watchers.clear();

  out->id = std::move(in.id);
  out->name = std::move(in.name);
  out->order = in.order;
  out->size = in.size;
  out->stripe_unit = in.stripe_unit;
  out->stripe_count = in.stripe_count;
  out->features = in.features;
  out->flags = in.flags;
  out->snapc.seq = in.snapc.seq;
  for (auto& s : in.snapc.snaps) {
    out->snapc.snaps.push_back(s);
  }
  for (auto& it : in.snaps) {
    auto& snap = out->snaps[it.first];
    auto& tsnap = in.snaps[it.first];

    cvt_snap_info(tsnap, &snap);
  }
  out->parent.spec.pool_id = in.parent.spec.pool_id;
  out->parent.spec.pool_namespace = std::move(in.parent.spec.pool_namespace);
  out->parent.spec.image_id = std::move(in.parent.spec.image_id);
  out->parent.spec.snap_id = in.parent.spec.snap_id;
  out->parent.overlap = in.parent.overlap;
  in.timestamp.to_timespec(&out->timestamp);
  out->data_pool_id = in.data_pool_id;
  for (auto& w : in.watchers) {
    out->watchers.emplace_back(std::move(w.addr));
  }
  out->qos.iops = -1;
  out->qos.bps = -1;
  for (auto& kv : in.kvs) {
    if (kv.first == conf_qos_iops_str) {
      out->qos.iops = std::atoll(kv.second.c_str());
    } else if (kv.first == conf_qos_bps_str) {
      out->qos.bps = std::atoll(kv.second.c_str());
    }
  }
  out->du = in.du;
}

void cvt_image_info_v2(librbd::xImageInfo_v2& in, librbdx::image_info_v2_t* out) {
  out->snapc.snaps.clear();
  out->snaps.clear();
  out->watchers.clear();

  out->id = std::move(in.id);
  out->name = std::move(in.name);
  out->order = in.order;
  out->size = in.size;
  out->stripe_unit = in.stripe_unit;
  out->stripe_count = in.stripe_count;
  out->features = in.features;
  out->flags = in.flags;
  out->snapc.seq = in.snapc.seq;
  for (auto& s : in.snapc.snaps) {
    out->snapc.snaps.push_back(s);
  }
  for (auto& it : in.snaps) {
    auto& snap = out->snaps[it.first];
    auto& tsnap = in.snaps[it.first];

    cvt_snap_info_v2(tsnap, &snap);
  }
  out->parent.spec.pool_id = in.parent.spec.pool_id;
  out->parent.spec.pool_namespace = std::move(in.parent.spec.pool_namespace);
  out->parent.spec.image_id = std::move(in.parent.spec.image_id);
  out->parent.spec.snap_id = in.parent.spec.snap_id;
  out->parent.overlap = in.parent.overlap;
  in.timestamp.to_timespec(&out->timestamp);
  out->data_pool_id = in.data_pool_id;
  for (auto& w : in.watchers) {
    out->watchers.emplace_back(std::move(w.addr));
  }
  out->qos.iops = -1;
  out->qos.bps = -1;
  for (auto& kv : in.kvs) {
    if (kv.first == conf_qos_iops_str) {
      out->qos.iops = std::atoll(kv.second.c_str());
    } else if (kv.first == conf_qos_bps_str) {
      out->qos.bps = std::atoll(kv.second.c_str());
    }
  }
  out->du = in.du;
}

}

namespace {

const std::string RBD_QOS_PREFIX = "conf_rbd_";
const uint64_t MAX_METADATA_ITEMS = 128;

std::pair<uint64_t, uint64_t> calc_du(BitVector<2>& object_map,
    uint64_t size, uint8_t order) {
  uint64_t used = 0;
  uint64_t dirty = 0;

  uint64_t left = size;
  uint64_t object_size = (1ull << order);

  auto it = object_map.begin();
  auto end_it = object_map.end();
  while (it != end_it) {
    uint64_t len = min(object_size, left);
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

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_size_info.image_id << "): "

/*
 * get `du` and `dirty` for a given head image/snap with explicitly
 * provided size info
 */
template <typename I>
class DuRequest {
public:
  DuRequest(librados::IoCtx& ioctx, Context* on_finish,
      librbd::xSizeInfo& size_info,
      uint64_t* du, uint64_t* dirty)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_size_info(size_info),
      m_du(du), m_dirty(dirty) {
    std::swap(m_size_info, size_info);
    *m_du = 0;
    if (m_dirty != nullptr) {
      *m_dirty = 0;
    }
  }

  void send() {
    get_du();
  }

private:
  void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

  void get_du() {
    if ((m_size_info.features & RBD_FEATURE_OBJECT_MAP) &&
        !(m_size_info.flags & RBD_FLAG_OBJECT_MAP_INVALID)) {
      load_object_map();
    } else {
      // todo: fallback to iterate image objects
      *m_du = 0;
      if (m_dirty != nullptr) {
        *m_dirty = 0;
      }

      finish(0);
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
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::object_map_load_finish(&it, &object_map);
    if (r < 0) {
      lderr(m_cct) << "failed to decode object map: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    auto du = calc_du(object_map, m_size_info.size, m_size_info.order);

    *m_du = du.first;
    if (m_dirty != nullptr) {
      *m_dirty = du.second;
    }

    finish(0);
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
  uint64_t* m_dirty;
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
      librbdx::image_info_t* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_info(info) {
    // NOTE: image name is not set
    m_x_info.id = m_image_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    if (r == 0) {
      cvt_image_info(m_x_info, m_info);
    }

    m_on_finish->complete(r);
    delete this;
  }

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

    auto order = &m_x_info.order;
    auto size = &m_x_info.size;
    auto stripe_unit = &m_x_info.stripe_unit;
    auto stripe_count = &m_x_info.stripe_count;
    auto features = &m_x_info.features;
    auto flags = &m_x_info.flags;
    auto snapc = &m_x_info.snapc;
    auto parent = &m_x_info.parent;
    auto timestamp = &m_x_info.timestamp;
    auto data_pool_id = &m_x_info.data_pool_id;
    auto watchers = &m_x_info.watchers;
    auto kvs = &m_x_info.kvs;

    auto it = m_out_bl.cbegin();
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

    auto& snaps = m_x_info.snaps;
    for (auto& it : m_cls_snaps) {
      snaps.insert({it.first, {
          .id = it.second.id,
          .name = it.second.name,
          .snap_ns_type = cls::rbd::get_snap_namespace_type(it.second.snapshot_namespace),
          .size = it.second.image_size,
          .flags = it.second.flags,
          .protection_status = it.second.protection_status,
          .timestamp = it.second.timestamp,
      }});
    }

    get_du();
  }

  void get_du() {
    Context *on_finish = librbd::util::create_context_callback<InfoRequest<I>,
        &InfoRequest<I>::handle_get_du>(this);
    auto& du = m_x_info.du;
    librbd::xSizeInfo size_info = {
      .image_id = m_image_id,
      .snap_id = CEPH_NOSNAP,
      .order = m_x_info.order,
      .size = m_x_info.size,
      .stripe_unit = m_x_info.stripe_unit,
      .stripe_count = m_x_info.stripe_count,
      .features = m_x_info.features,
      .flags = m_x_info.flags,
    };
    auto request = new DuRequest<I>(m_io_ctx, on_finish,
        size_info, &du, nullptr);
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

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;
  std::map<snapid_t, cls::rbd::xclsSnapInfo> m_cls_snaps;
  librbd::xImageInfo m_x_info;

  // [in]
  const std::string m_image_id;

  // [out]
  librbdx::image_info_t* m_info;
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
      librbdx::image_info_v2_t* info)
    : m_cct(reinterpret_cast<CephContext*>(ioctx.cct())),
      m_io_ctx(ioctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_lock(image_id),
      m_pending_count(0),
      m_info(info),
      m_r(0) {
    // NOTE: image name is not set
    m_x_info.id = m_image_id;
  }

  void send() {
    get_head();
  }

private:
  void finish(int r) {
    if (r == 0) {
      cvt_image_info_v2(m_x_info, m_info);
    }

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

    ceph_assert(m_pending_count > 0);
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

    auto order = &m_x_info.order;
    auto size = &m_x_info.size;
    auto stripe_unit = &m_x_info.stripe_unit;
    auto stripe_count = &m_x_info.stripe_count;
    auto features = &m_x_info.features;
    auto flags = &m_x_info.flags;
    auto snapc = &m_x_info.snapc;
    auto parent = &m_x_info.parent;
    auto timestamp = &m_x_info.timestamp;
    auto data_pool_id = &m_x_info.data_pool_id;
    auto watchers = &m_x_info.watchers;
    auto kvs = &m_x_info.kvs;

    auto it = m_out_bl.cbegin();
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

    auto& snaps = m_x_info.snaps;
    for (auto& it : m_cls_snaps) {
      snaps.insert({it.first, {
          .id = it.second.id,
          .name = it.second.name,
          .snap_ns_type = cls::rbd::get_snap_namespace_type(it.second.snapshot_namespace),
          .size = it.second.image_size,
          .flags = it.second.flags,
          .protection_status = it.second.protection_status,
          .timestamp = it.second.timestamp,
      }});
    }

    if (!snapc->is_valid()) {
      lderr(m_cct) << "snap context is invalid" << dendl;
      finish(-ESTALE);
      return;
    }

    get_dus();
  }

  void get_dus() {
    ldout(m_cct, 10) << dendl;

    m_pending_count = 1 + m_x_info.snapc.snaps.size();

    std::vector<uint64_t> snaps{CEPH_NOSNAP};
    snaps.reserve(m_pending_count);
    snaps.insert(snaps.end(), m_x_info.snapc.snaps.begin(), m_x_info.snapc.snaps.end());

    using klass = InfoRequest_v2<I>;
    for (auto snap : snaps) {
      Context *on_finish = librbd::util::create_context_callback<klass,
          &klass::complete_request>(this);
      auto du = &m_x_info.du;
      uint64_t* dirty = nullptr;
      if (snap != CEPH_NOSNAP) {
        du = &m_x_info.snaps[snap].du;
        dirty = &m_x_info.snaps[snap].dirty;
      }
      librbd::xSizeInfo size_info = {
        .image_id = m_image_id,
        .snap_id = snap,
        .order = m_x_info.order,
        .size = m_x_info.size,
        .stripe_unit = m_x_info.stripe_unit,
        .stripe_count = m_x_info.stripe_count,
        .features = m_x_info.features,
        .flags = m_x_info.flags,
      };
      auto request = new DuRequest<I>(m_io_ctx, on_finish,
          size_info, du, dirty);
      request->send();
    }
  }

private:
  CephContext* m_cct;
  librados::IoCtx& m_io_ctx;
  Context* m_on_finish;
  bufferlist m_out_bl;
  std::map<snapid_t, cls::rbd::xclsSnapInfo> m_cls_snaps;
  librbd::xImageInfo_v2 m_x_info;

  // [in]
  const std::string m_image_id;

  Mutex m_lock;
  int m_pending_count;

  // [out]
  librbdx::image_info_v2_t* m_info;
  int m_r;
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
      librbdx::image_info_t* info, int* r)
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
  SimpleThrottle& m_throttle;
  InfoRequest<I>* m_request;

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
      librbdx::image_info_v2_t* info, int* r)
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

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
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
    const std::string& image_id, librbdx::image_info_v2_t* info) {
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
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos) {
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

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_management_ops");
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
    std::map<std::string, std::pair<librbdx::image_info_t, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_management_ops");
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
    std::map<std::string, std::pair<librbdx::image_info_v2_t, int>>* infos) {
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

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_management_ops");
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
    std::map<std::string, std::pair<librbdx::image_info_v2_t, int>>* infos) {
  CephContext* cct = (CephContext*)ioctx.cct();
  ldout(cct, 20) << "ioctx=" << &ioctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf.get_val<uint64_t>("rbd_concurrent_management_ops");
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

} // namespace api
} // namespace librbd

template class librbd::api::xImage<librbd::ImageCtx>;
