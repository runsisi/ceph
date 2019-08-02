// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_TYPES_H
#define LIBRBD_TYPES_H

#include "include/types.h"
#include "include/rados/rados_types.hpp"
#include "common/snap_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

namespace librbd {

// Performance counters
enum {
  l_librbd_first = 26000,

  l_librbd_rd,               // read ops
  l_librbd_rd_bytes,         // bytes read
  l_librbd_rd_latency,       // average latency
  l_librbd_wr,
  l_librbd_wr_bytes,
  l_librbd_wr_latency,
  l_librbd_discard,
  l_librbd_discard_bytes,
  l_librbd_discard_latency,
  l_librbd_flush,

  l_librbd_aio_flush,
  l_librbd_aio_flush_latency,
  l_librbd_ws,
  l_librbd_ws_bytes,
  l_librbd_ws_latency,

  l_librbd_cmp,
  l_librbd_cmp_bytes,
  l_librbd_cmp_latency,

  l_librbd_snap_create,
  l_librbd_snap_remove,
  l_librbd_snap_rollback,
  l_librbd_snap_rename,

  l_librbd_notify,
  l_librbd_resize,

  l_librbd_readahead,
  l_librbd_readahead_bytes,

  l_librbd_invalidate_cache,

  l_librbd_opened_time,
  l_librbd_lock_acquired_time,

  l_librbd_last,
};

/** @brief Unique identification of a parent in clone relationship.
 * Cloning an image creates a child image that keeps a reference
 * to its parent. This allows copy-on-write images. */
struct ParentSpec {
  int64_t pool_id;
  std::string image_id;
  snapid_t snap_id;

  ParentSpec() : pool_id(-1), snap_id(CEPH_NOSNAP) {
  }
  ParentSpec(int64_t pool_id, std::string image_id, snapid_t snap_id)
    : pool_id(pool_id), image_id(image_id), snap_id(snap_id) {
  }

  bool operator==(const ParentSpec &other) {
    return ((this->pool_id == other.pool_id) &&
            (this->image_id == other.image_id) &&
            (this->snap_id == other.snap_id));
  }
  bool operator!=(const ParentSpec &other) {
    return !(*this == other);
  }
  bool operator<(const ParentSpec &other) const {
    return ((pool_id < other.pool_id) ||
            (image_id < other.image_id) ||
            (snap_id < other.snap_id));
  }
};

/// Full information about an image's parent.
struct ParentInfo {
  /// Identification of the parent.
  ParentSpec spec;

  /** @brief Where the portion of data shared with the child image ends.
   * Since images can be resized multiple times, the portion of data shared
   * with the child image is not necessarily min(parent size, child size).
   * If the child image is first shrunk and then enlarged, the common portion
   * will be shorter. */
  uint64_t overlap;

  ParentInfo() : overlap(0) {
  }
};

struct SnapInfo {
  std::string name;
  cls::rbd::SnapshotNamespace snap_namespace;
  uint64_t size;
  ParentInfo parent;
  uint8_t protection_status;
  uint64_t flags;
  utime_t timestamp;
  SnapInfo(std::string _name,
           const cls::rbd::SnapshotNamespace &_snap_namespace,
           uint64_t _size, const ParentInfo &_parent,
           uint8_t _protection_status, uint64_t _flags, utime_t _timestamp)
    : name(_name), snap_namespace(_snap_namespace), size(_size),
      parent(_parent), protection_status(_protection_status), flags(_flags),
      timestamp(_timestamp) {
  }
};

struct xSizeInfo {
  std::string image_id;
  snapid_t snap_id;
  uint8_t order = 0;
  uint64_t size = 0;
  uint64_t stripe_unit = 0;
  uint64_t stripe_count = 0;
  uint64_t features = 0;
  uint64_t flags = 0;
};

struct xImageInfo {
  std::string id;
  std::string name;
  uint8_t order = 0;
  uint64_t size = 0;
  uint64_t stripe_unit = 0;
  uint64_t stripe_count = 0;
  uint64_t features = 0;
  uint64_t flags = 0;
  SnapContext snapc;
  ParentInfo parent;
  utime_t timestamp;
  int64_t data_pool_id = -1;
  std::vector<obj_watch_t> watchers;
  std::map<std::string, std::string> kvs;
};

// do not default initialize the fields
// https://stackoverflow.com/questions/37776823/could-not-convert-from-brace-enclosed-initializer-list-to-struct
struct xSnapInfo {
  snapid_t id;
  std::string name;
  cls::rbd::SnapshotNamespaceType snap_ns_type;
  uint64_t size;
  uint64_t features;
  uint64_t flags;
  uint8_t protection_status;
  utime_t timestamp;
};

struct xImageInfo_v2 {
  std::string id;
  std::string name;
  uint8_t order = 0;
  uint64_t size = 0;
  uint64_t stripe_unit = 0;
  uint64_t stripe_count = 0;
  uint64_t features = 0;
  uint64_t flags = 0;
  SnapContext snapc;
  ParentInfo parent;
  utime_t timestamp;
  int64_t data_pool_id = -1;
  std::vector<obj_watch_t> watchers;
  std::map<std::string, std::string> kvs;
  std::map<snapid_t, xSnapInfo> snaps;
};

struct xTrashInfo {
  std::string id;
  std::string name;
  cls::rbd::TrashImageSource source;
  utime_t deletion_time;
  utime_t deferment_end_time;
};

} // namespace librbd

#endif // LIBRBD_TYPES_H
