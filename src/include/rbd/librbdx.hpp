/*
 * librbdx.hpp
 *
 *  Created on: Jul 31, 2019
 *      Author: runsisi
 */

#ifndef SRC_INCLUDE_RBD_LIBRBDX_HPP_
#define SRC_INCLUDE_RBD_LIBRBDX_HPP_

#include <map>
#include <string>
#include <vector>

#include "../rados/librados.hpp"
#include "../rbd/librbd.h"
#include "../rbd/librbd.hpp"

namespace librbdx {

enum class snap_ns_type_t {
  SNAPSHOT_NAMESPACE_TYPE_USER = 0,
  SNAPSHOT_NAMESPACE_TYPE_GROUP = 1,
  SNAPSHOT_NAMESPACE_TYPE_TRASH = 2
};

inline std::string to_str(const snap_ns_type_t& o) {
  switch (o) {
  case snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_USER:
    return "user";
  case snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_GROUP:
    return "group";
  case snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_TRASH:
    return "trash";
  default:
    return "unknown";
  }
}

enum class snap_protection_status_t {
  PROTECTION_STATUS_UNPROTECTED  = 0,
  PROTECTION_STATUS_UNPROTECTING = 1,
  PROTECTION_STATUS_PROTECTED    = 2,
  PROTECTION_STATUS_LAST         = 3
};

inline std::string to_str(const snap_protection_status_t& o) {
  switch (o) {
  case snap_protection_status_t::PROTECTION_STATUS_UNPROTECTED:
    return "unprotected";
  case snap_protection_status_t::PROTECTION_STATUS_UNPROTECTING:
    return "unprotecting";
  case snap_protection_status_t::PROTECTION_STATUS_PROTECTED:
    return "protected";
  default:
    return "unknown";
  }
}

enum class trash_source_t {
  TRASH_IMAGE_SOURCE_USER = 0,
  TRASH_IMAGE_SOURCE_MIRRORING = 1,
  TRASH_IMAGE_SOURCE_MIGRATION = 2,
  TRASH_IMAGE_SOURCE_REMOVING = 3,
};

inline std::string to_str(const trash_source_t& o) {
  switch (o) {
  case trash_source_t::TRASH_IMAGE_SOURCE_USER:
    return "user";
  case trash_source_t::TRASH_IMAGE_SOURCE_MIRRORING:
    return "mirroring";
  case trash_source_t::TRASH_IMAGE_SOURCE_MIGRATION:
    return "migration";
  case trash_source_t::TRASH_IMAGE_SOURCE_REMOVING:
    return "removing";
  default:
    return "unknown";
  }
}

typedef struct {
  uint64_t size;
  // if fast-diff is disabled then `dirty` equals `du`
  uint64_t du;          // OBJECT_EXISTS + OBJECT_EXISTS_CLEAN
  uint64_t dirty;       // OBJECT_EXISTS
} du_info_t;

typedef struct {
  uint64_t seq;
  std::vector<uint64_t> snaps;
} snapc_t;

typedef struct {
  int64_t pool_id;
  std::string pool_namespace;
  std::string image_id;
  uint64_t snap_id;
} parent_spec_t;

inline bool operator<(const parent_spec_t& lhs, const parent_spec_t& rhs) {
  return ((lhs.pool_id < rhs.pool_id) ||
      (lhs.pool_namespace < rhs.pool_namespace) ||
      (lhs.image_id < rhs.image_id) ||
      (lhs.snap_id < rhs.snap_id));
}

inline std::string to_str(const parent_spec_t& o) {
  std::string str;
  str = std::to_string(o.pool_id);
  str += "/";
  str += o.pool_namespace;
  str += "/";
  str += o.image_id;
  str += "/";
  str += std::to_string(o.snap_id);
  return std::move(str);
}

typedef struct {
  parent_spec_t spec;
  uint64_t overlap;
} parent_info_t;

typedef struct {
  int64_t pool_id;
  std::string image_id;
} child_t;

typedef struct {
  int64_t iops;
  int64_t bps;
} qos_t;

typedef struct {
  uint64_t id;
  std::string name;
  snap_ns_type_t snap_ns_type;
  uint64_t size;
  uint64_t flags;
  snap_protection_status_t protection_status;
  timespec timestamp;
} snap_info_t;

typedef struct {
  uint64_t id;
  std::string name;
  snap_ns_type_t snap_ns_type;
  uint64_t size;
  uint64_t flags;
  snap_protection_status_t protection_status;
  timespec timestamp;
  // if fast-diff is disabled then `dirty` equals `du`
  uint64_t du;          // OBJECT_EXISTS + OBJECT_EXISTS_CLEAN
  uint64_t dirty;       // OBJECT_EXISTS
} snap_info_v2_t;

typedef struct {
  std::string id;
  std::string name;
  uint8_t order;
  uint64_t size;
  uint64_t stripe_unit;
  uint64_t stripe_count;
  uint64_t features;
  uint64_t flags;
  snapc_t snapc;
  std::map<uint64_t, snap_info_t> snaps;
  parent_info_t parent;
  timespec timestamp;
  int64_t data_pool_id;
  std::vector<std::string> watchers;
  qos_t qos;
  uint64_t du;
} image_info_t;

typedef struct {
  std::string id;
  std::string name;
  uint8_t order;
  uint64_t size;
  uint64_t stripe_unit;
  uint64_t stripe_count;
  uint64_t features;
  uint64_t flags;
  snapc_t snapc;
  std::map<uint64_t, snap_info_v2_t> snaps;
  parent_info_t parent;
  timespec timestamp;
  int64_t data_pool_id;
  std::vector<std::string> watchers;
  qos_t qos;
  uint64_t du;
} image_info_v3_t;

typedef struct {
  std::string id;
  std::string name;
  trash_source_t source;
  timespec deletion_time;
  timespec deferment_end_time;
} trash_info_t;

class CEPH_RBD_API xRBD {
public:
  //
  // xImage
  //
  int get_info(librados::IoCtx& ioctx,
      const std::string& image_id, image_info_t* info);
  int get_info_v2(librados::IoCtx& ioctx,
      const std::string& image_id, image_info_v2_t* info);
  int get_info_v3(librados::IoCtx& ioctx,
      const std::string& image_id, image_info_v3_t* info);

  int list_du(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<du_info_t, int>>* infos);
  int list_du(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<du_info_t, int>>* infos);
  int list_du_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos);
  int list_du_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos);

  int list(librados::IoCtx& ioctx,
      std::map<std::string, std::string>* images);

  int list_info(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<image_info_t, int>>* infos);
  int list_info(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<image_info_t, int>>* infos);

  int list_info_v2(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<image_info_v2_t, int>>* infos);
  int list_info_v2(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<image_info_v2_t, int>>* infos);

  int list_info_v3(librados::IoCtx& ioctx,
      std::map<std::string, std::pair<image_info_v3_t, int>>* infos);
  int list_info_v3(librados::IoCtx& ioctx,
      const std::vector<std::string>& image_ids,
      std::map<std::string, std::pair<image_info_v3_t, int>>* infos);

  //
  // xTrash
  //
  int trash_list(librados::IoCtx& ioctx,
      std::map<std::string, trash_info_t>* trashes);

  //
  // xChild, for legacy clone v1 only
  //
  int child_list(librados::IoCtx& ioctx,
      std::map<parent_spec_t, std::vector<std::string>>* children);

};

}

#endif /* SRC_INCLUDE_RBD_LIBRBDX_HPP_ */
