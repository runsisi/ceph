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

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::SizeRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

uint64_t calc_du(BitVector<2> &object_map, uint64_t size, uint8_t order) {
  uint64_t used = 0;
  uint64_t left = size;
  uint64_t object_size = (1ull << order);

  auto it = object_map.begin();
  auto end_it = object_map.end();
  while (it != end_it) {
    uint64_t len = min(object_size, left);
    if (*it == OBJECT_EXISTS) {
      used += len;
    }

    ++it;
    left -= len;
  }
  return used;
}

template <typename I>
class SizeRequest {
public:
  SizeRequest(librados::IoCtx &io_ctx, Context *on_finish,
      const std::string &image_id,
      snapid_t snap_id,
      librbd::xSizeInfo *size_info)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_on_finish(on_finish),
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
  CephContext *m_cct;
  librados::IoCtx &m_io_ctx;
  Context * m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;
  snapid_t m_snap_id;

  // [out]
  librbd::xSizeInfo *m_size_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_size_get_start(&op, m_snap_id);

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<SizeRequest<I>,
        &SizeRequest<I>::handle_get_head>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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
#define dout_prefix *_dout << "librbd::api::xImage::InfoRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class InfoRequest {
public:
  InfoRequest(librados::IoCtx &io_ctx, Context *on_finish,
      const std::string &image_id,
      librbd::xImageInfo *image_info)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_image_info(image_info) {
    // NOTE: image name is not set
    m_image_info->id = m_image_id;
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
  CephContext *m_cct;
  librados::IoCtx &m_io_ctx;
  Context * m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;

  // [out]
  librbd::xImageInfo *m_image_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, RBD_QOS_PREFIX,
        MAX_METADATA_ITEMS);

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<InfoRequest<I>,
        &InfoRequest<I>::handle_get_head>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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

    auto order = &m_image_info->order;
    auto size = &m_image_info->size;
    auto stripe_unit = &m_image_info->stripe_unit;
    auto stripe_count = &m_image_info->stripe_count;
    auto features = &m_image_info->features;
    auto flags = &m_image_info->flags;
    auto snapc = &m_image_info->snapc;
    auto parent = &m_image_info->parent;
    auto timestamp = &m_image_info->timestamp;
    auto data_pool_id = &m_image_info->data_pool_id;
    auto watchers = &m_image_info->watchers;
    auto kvs = &m_image_info->kvs;

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
  InfoRequest_v2(librados::IoCtx &io_ctx, Context *on_finish,
      const std::string &image_id,
      librbd::xImageInfo_v2 *image_info)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_image_info(image_info) {
    // NOTE: image name is not set
    m_image_info->id = m_image_id;
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
  CephContext *m_cct;
  librados::IoCtx &m_io_ctx;
  Context * m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;

  // [out]
  librbd::xImageInfo_v2 *m_image_info;

  void get_head() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_image_get_start(&op);
    librbd::cls_client::metadata_list_start(&op, RBD_QOS_PREFIX,
        MAX_METADATA_ITEMS);

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<InfoRequest_v2<I>,
        &InfoRequest_v2<I>::handle_get_head>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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

    auto order = &m_image_info->order;
    auto size = &m_image_info->size;
    auto stripe_unit = &m_image_info->stripe_unit;
    auto stripe_count = &m_image_info->stripe_count;
    auto features = &m_image_info->features;
    auto flags = &m_image_info->flags;
    auto snapc = &m_image_info->snapc;
    auto parent = &m_image_info->parent;
    auto timestamp = &m_image_info->timestamp;
    auto data_pool_id = &m_image_info->data_pool_id;
    auto watchers = &m_image_info->watchers;
    auto kvs = &m_image_info->kvs;

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

    if (!snapc->is_valid()) {
      lderr(m_cct) << "snap context is invalid" << dendl;
      finish(-EINVAL);
      return;
    }

    get_snaps();
  }

  void get_snaps() {
    if (m_image_info->snapc.snaps.empty()) {
      finish(0);
      return;
    }

    ldout(m_cct, 10) << dendl;
    librados::ObjectReadOperation op;
    for (auto snap_id : m_image_info->snapc.snaps) {
      librbd::cls_client::x_snap_get_start(&op, snap_id);
    }

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<
        InfoRequest_v2<I>, &InfoRequest_v2<I>::handle_get_snaps>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
  }

  void handle_get_snaps(int r) {
    ldout(m_cct, 10) << "r=" << r << dendl;

    if (r == -ENOENT) {
      ldout(m_cct, 5) << "out-of-sync snapshots" << dendl;
      get_head();
    } else if (r < 0) {
      lderr(m_cct) << "failed to retrieve snapshots: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    auto &snaps = m_image_info->snaps;

    auto it = m_out_bl.begin();
    for (auto snap_id : m_image_info->snapc.snaps) {
      cls::rbd::x_SnapshotInfo cls_snap_info;
      r = librbd::cls_client::x_snap_get_finish(&it, &cls_snap_info);
      if (r < 0) {
        break;
      }

      librbd::xSnapInfo snap = {
        .id = snap_id,
        .name = cls_snap_info.name,
        .snap_namespace = cls_snap_info.snapshot_namespace.snapshot_namespace,
        .size = cls_snap_info.image_size,
        .features = cls_snap_info.features,
        .flags = cls_snap_info.flags,
        .protection_status = cls_snap_info.protection_status,
        .timestamp = cls_snap_info.timestamp,
      };
      snaps.insert({snap_id, snap});
    }

    if (r < 0) {
      lderr(m_cct) << "failed to decode snapshots: "
                   << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }

    finish(0);
    return;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::DuRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << "): "

template <typename I>
class DuRequest {
public:
  DuRequest(librados::IoCtx &io_ctx, Context *on_finish,
      const std::string &image_id,
      snapid_t snap_id,
      uint64_t *du)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_snap_id(snap_id),
      m_du(du) {
    // NOTE: image name is not set
    m_size_info.image_id = m_image_id;
    m_size_info.snap_id = m_snap_id;
    *m_du = 0;
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
  CephContext *m_cct;
  librados::IoCtx &m_io_ctx;
  Context * m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;
  snapid_t m_snap_id;

  // [out]
  librbd::xSizeInfo m_size_info;
  uint64_t *m_du;

  void get_size() {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::x_size_get_start(&op, m_snap_id);

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<DuRequest<I>,
        &DuRequest<I>::handle_get_size>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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

    load_object_map(m_snap_id);
  }

  void load_object_map(snapid_t snap_id) {
    ldout(m_cct, 10) << dendl;

    librados::ObjectReadOperation op;
    librbd::cls_client::object_map_load_start(&op);

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<DuRequest<I>,
        &DuRequest<I>::handle_load_object_map>(this);
    std::string oid(librbd::ObjectMap<>::object_map_name(m_image_id, snap_id));
    int r = m_io_ctx.aio_operate(oid,
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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
      *m_du = calc_du(object_map, m_size_info.size, m_size_info.order);
    } else {
      // todo: fallback to iterate image objects
      *m_du = 0;
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
  DuRequest_v2(librados::IoCtx &io_ctx, Context *on_finish,
      const std::string &image_id,
      std::map<snapid_t, uint64_t> *du)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_io_ctx(io_ctx), m_on_finish(on_finish),
      m_image_id(image_id),
      m_lock(image_id),
      m_pending_count(0),
      m_r(0),
      m_du(du) {
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

    m_out_bl.clear();
    auto aio_comp = librbd::util::create_rados_callback<DuRequest_v2<I>,
        &DuRequest_v2<I>::handle_get_head>(this);
    int r = m_io_ctx.aio_operate(librbd::util::header_name(m_image_id),
        aio_comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    aio_comp->release();
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

    std::vector<snapid_t> snaps{CEPH_NOSNAP};
    snaps.reserve(m_pending_count);
    snaps.insert(snaps.end(), m_snapc.snaps.begin(), m_snapc.snaps.end());

    for (auto snap : snaps) {
      Context *on_complete = librbd::util::create_context_callback<DuRequest_v2<I>,
          &DuRequest_v2<I>::complete_request>(this);
      auto &du = (*m_du)[snap];
      auto request = new DuRequest<I>(m_io_ctx, on_complete,
          m_image_id, snap, &du);
      request->send();
    }
  }

private:
  CephContext *m_cct;
  librados::IoCtx &m_io_ctx;
  Context * m_on_finish;
  bufferlist m_out_bl;

  // [in]
  const std::string m_image_id;

  // [out]
  SnapContext m_snapc;
  Mutex m_lock;
  int m_pending_count;
  int m_r;
  std::map<snapid_t, uint64_t> *m_du;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledInfoRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << ", name=" \
                           << m_image_name << "): "

template <typename I>
class ThrottledInfoRequest {
public:
  ThrottledInfoRequest(librados::IoCtx &io_ctx, SimpleThrottle &throttle,
      const std::string &image_id, const std::string &image_name,
      librbd::xImageInfo *image_info, int *r)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id), m_image_name(image_name),
      m_r(r) {
    image_info->id = image_id;
    image_info->name = image_name;

    Context *on_finish = librbd::util::create_context_callback<ThrottledInfoRequest<I>,
        &ThrottledInfoRequest<I>::finish>(this);
    m_request = new InfoRequest<I>(io_ctx, on_finish, image_id, image_info);
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
  CephContext *m_cct;
  InfoRequest<I> *m_request;
  SimpleThrottle &m_throttle;

  // [in]
  const std::string m_image_id;
  const std::string m_image_name;

  // [out]
  int *m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledInfoRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << ", name=" \
                           << m_image_name << "): "

template <typename I>
class ThrottledInfoRequest_v2 {
public:
  ThrottledInfoRequest_v2(librados::IoCtx &io_ctx, SimpleThrottle &throttle,
      const std::string &image_id, const std::string &image_name,
      librbd::xImageInfo_v2 *image_info, int *r)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id), m_image_name(image_name),
      m_r(r) {
    image_info->id = image_id;
    image_info->name = image_name;

    Context *on_finish = librbd::util::create_context_callback<ThrottledInfoRequest_v2<I>,
        &ThrottledInfoRequest_v2<I>::finish>(this);
    m_request = new InfoRequest_v2<I>(io_ctx, on_finish, image_id, image_info);
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
  CephContext *m_cct;
  SimpleThrottle &m_throttle;
  InfoRequest_v2<I> *m_request;

  // [in]
  const std::string m_image_id;
  const std::string m_image_name;

  // [out]
  int *m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledDuRequest: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << ", name=" \
                           << m_image_name << "): "

template <typename I>
class ThrottledDuRequest {
public:
  ThrottledDuRequest(librados::IoCtx &io_ctx, SimpleThrottle &throttle,
      const std::string &image_id, const std::string &image_name,
      uint64_t *du, int *r)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id), m_image_name(image_name),
      m_r(r) {
    Context *on_finish = librbd::util::create_context_callback<ThrottledDuRequest<I>,
        &ThrottledDuRequest<I>::finish>(this);
    m_request = new DuRequest<I>(io_ctx, on_finish, image_id, CEPH_NOSNAP, du);
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
  CephContext *m_cct;
  DuRequest<I> *m_request;
  SimpleThrottle &m_throttle;

  // [in]
  const std::string m_image_id;
  const std::string m_image_name;

  // [out]
  int *m_r;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage::ThrottledDuRequest_v2: " \
                           << __func__ << " " << this << ": " \
                           << "(id=" << m_image_id << ", name=" \
                           << m_image_name << "): "

template <typename I>
class ThrottledDuRequest_v2 {
public:
  ThrottledDuRequest_v2(librados::IoCtx &io_ctx, SimpleThrottle &throttle,
      const std::string &image_id, const std::string &image_name,
      std::map<snapid_t, uint64_t> *du, int *r)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_throttle(throttle),
      m_image_id(image_id), m_image_name(image_name),
      m_r(r) {
    Context *on_finish = librbd::util::create_context_callback<ThrottledDuRequest_v2<I>,
        &ThrottledDuRequest_v2<I>::finish>(this);
    m_request = new DuRequest_v2<I>(io_ctx, on_finish, image_id, du);
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
  CephContext *m_cct;
  DuRequest_v2<I> *m_request;
  SimpleThrottle &m_throttle;

  // [in]
  const std::string m_image_id;
  const std::string m_image_name;

  // [out]
  int *m_r;
};

} // anonymous namespace

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::xImage: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int xImage<I>::get_size(librados::IoCtx &io_ctx,
    const std::string &id, snapid_t snap_id, xSizeInfo *info) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  C_SaferCond cond;

  auto req = new SizeRequest<I>(io_ctx, &cond, id, snap_id, info);
  req->send();

  int r = cond.wait();
  return r;
}

template <typename I>
int xImage<I>::get_info(librados::IoCtx &io_ctx,
    const std::string &id, xImageInfo *info) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new InfoRequest<I>(io_ctx, &cond, id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_info_v2(librados::IoCtx &io_ctx,
    const std::string &id, xImageInfo_v2 *info) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new InfoRequest_v2<I>(io_ctx, &cond, id, info);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_du(librados::IoCtx &io_ctx,
    const std::string &image_id, snapid_t snap_id,
    uint64_t *du) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new DuRequest<I>(io_ctx, &cond, image_id, snap_id, du);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_du_v2(librados::IoCtx &io_ctx,
    const std::string &image_id,
    std::map<snapid_t, uint64_t> *dus) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  C_SaferCond cond;

  auto req = new DuRequest_v2<I>(io_ctx, &cond, image_id, dus);
  req->send();

  int r = cond.wait();

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return r;
}

template <typename I>
int xImage<I>::get_du_sync(librados::IoCtx &io_ctx,
    const std::string &image_id, snapid_t snap_id,
    uint64_t *du) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  xSizeInfo info;
  int r = xImage<I>::get_size(io_ctx, image_id, snap_id, &info);
  if (r < 0) {
    lderr(cct) << "failed to get size: " << image_id << "@" << snap_id << ", "
               << cpp_strerror(r)
               << dendl;
    return r;
  }

  if ((info.features & RBD_FEATURE_FAST_DIFF) &&
      !(info.flags & RBD_FLAG_FAST_DIFF_INVALID)) {
    BitVector<2> object_map;
    std::string oid(ObjectMap<>::object_map_name(image_id, snap_id));
    int r = librbd::cls_client::object_map_load(&io_ctx, oid, &object_map);
    if (r < 0) {
      lderr(cct) << "failed to load object map: " << oid << ", "
                 << cpp_strerror(r)
                 << dendl;
      return r;
    }

    *du = calc_du(object_map, info.size, info.order);
  } else {
    // todo: fallback to iterate image objects
    *du = 0;
  }

  latency = ceph_clock_now() - latency;
  ldout(cct, 7) << "latency: "
                << latency.sec() << "s/"
                << latency.usec() << "us" << dendl;

  return 0;
}

template <typename I>
int xImage<I>::list(librados::IoCtx &io_ctx,
    std::map<std::string, std::string> *images) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  bool more_entries;
  uint32_t max_read = 1024;
  std::string last_read = "";
  do {
    std::map<std::string, std::string> page;
    int r = cls_client::dir_list(&io_ctx, RBD_DIRECTORY,
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

    for (const auto &entry : page) {
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
int xImage<I>::list_info(librados::IoCtx &io_ctx,
    std::map<std::string, std::pair<xImageInfo, int>> *infos) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(io_ctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, trash_image_info_t> trashes;
//  int r = zTrash<I>::list(io_ctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto &it : trashes) {
//    if (it.second.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &info = (*infos)[id].first;
    auto &r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest<I>(io_ctx, throttle, id, name, &info, &r);
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
int xImage<I>::list_info(librados::IoCtx &io_ctx,
    const std::map<std::string, std::string> &images,
    std::map<std::string, std::pair<xImageInfo, int>> *infos) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &info = (*infos)[id].first;
    auto &r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest<I>(io_ctx, throttle, id, name, &info, &r);
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
int xImage<I>::list_info_v2(librados::IoCtx &io_ctx,
    std::map<std::string, std::pair<xImageInfo_v2, int>> *infos) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(io_ctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, trash_image_info_t> trashes;
//  int r = zTrash<I>::list(io_ctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto &it : trashes) {
//    if (it.second.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &info = (*infos)[id].first;
    auto &r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v2<I>(io_ctx, throttle, id, name, &info, &r);
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
int xImage<I>::list_info_v2(librados::IoCtx &io_ctx,
    const std::map<std::string, std::string> &images,
    std::map<std::string, std::pair<xImageInfo_v2, int>> *infos) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &info = (*infos)[id].first;
    auto &r = (*infos)[id].second;
    auto req = new ThrottledInfoRequest_v2<I>(io_ctx, throttle, id, name, &info, &r);
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
int xImage<I>::list_du(librados::IoCtx &io_ctx,
    std::map<std::string, std::pair<uint64_t, int>> *dus) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(io_ctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, trash_image_info_t> trashes;
//  int r = zTrash<I>::list(io_ctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto &it : trashes) {
//    if (it.second.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &du = (*dus)[id].first;
    auto &r = (*dus)[id].second;
    auto req = new ThrottledDuRequest<I>(io_ctx, throttle, id, name, &du, &r);
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
int xImage<I>::list_du(librados::IoCtx &io_ctx,
    const std::map<std::string, std::string> &images,
    std::map<std::string, std::pair<uint64_t, int>> *dus) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &du = (*dus)[id].first;
    auto &r = (*dus)[id].second;
    auto req = new ThrottledDuRequest<I>(io_ctx, throttle, id, name, &du, &r);
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
int xImage<I>::list_du_v2(librados::IoCtx &io_ctx,
    std::map<std::string, std::pair<std::map<snapid_t, uint64_t>, int>> *dus) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  // map<id, name>
  std::map<std::string, std::string> images;
  int r = xImage<I>::list(io_ctx, &images);
  if (r < 0) {
    return r;
  }

  // images are moved to trash when removing, since Nautilus
//  std::map<std::string, trash_image_info_t> trashes;
//  int r = zTrash<I>::list(io_ctx, &trashes);
//  if (r < 0 && r != -EOPNOTSUPP) {
//    return r;
//  }
//  for (auto &it : trashes) {
//    if (it.second.source == RBD_TRASH_IMAGE_SOURCE_REMOVING) {
//      images.insert({it.first, it.second.name});
//    }
//  }

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &du = (*dus)[id].first;
    auto &r = (*dus)[id].second;
    auto req = new ThrottledDuRequest_v2<I>(io_ctx, throttle, id, name, &du, &r);
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
int xImage<I>::list_du_v2(librados::IoCtx &io_ctx,
    const std::map<std::string, std::string> &images,
    std::map<std::string, std::pair<std::map<snapid_t, uint64_t>, int>> *dus) {
  CephContext *cct = (CephContext *)io_ctx.cct();
  ldout(cct, 20) << "io_ctx=" << &io_ctx << dendl;

  utime_t latency = ceph_clock_now();

  auto ops = cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
  SimpleThrottle throttle(ops, true);
  for (const auto &image : images) {
    if (throttle.pending_error()) {
      break;
    }

    auto id = image.first;
    auto name = image.second;

    auto &du = (*dus)[id].first;
    auto &r = (*dus)[id].second;
    auto req = new ThrottledDuRequest_v2<I>(io_ctx, throttle, id, name, &du, &r);
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
