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
};

std::string to_str(const snap_ns_type_t& o) {
  switch (o) {
  case snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_USER:
    return "SNAPSHOT_NAMESPACE_TYPE_USER";
  default:
    return "???";
  }
}

enum class snap_protection_status_t {
  PROTECTION_STATUS_UNPROTECTED  = 0,
  PROTECTION_STATUS_UNPROTECTING = 1,
  PROTECTION_STATUS_PROTECTED    = 2,
  PROTECTION_STATUS_LAST         = 3
};

std::string to_str(const snap_protection_status_t& o) {
  switch (o) {
  case snap_protection_status_t::PROTECTION_STATUS_UNPROTECTED:
    return "PROTECTION_STATUS_UNPROTECTED";
  case snap_protection_status_t::PROTECTION_STATUS_UNPROTECTING:
    return "PROTECTION_STATUS_UNPROTECTING";
  case snap_protection_status_t::PROTECTION_STATUS_PROTECTED:
    return "PROTECTION_STATUS_PROTECTED";
  default:
    return "???";
  }
}

enum class trash_source_t {
  TRASH_IMAGE_SOURCE_USER = 0,
  TRASH_IMAGE_SOURCE_MIRRORING = 1,
};

std::string to_str(const trash_source_t& o) {
  switch (o) {
  case trash_source_t::TRASH_IMAGE_SOURCE_USER:
    return "TRASH_IMAGE_SOURCE_USER";
  case trash_source_t::TRASH_IMAGE_SOURCE_MIRRORING:
    return "TRASH_IMAGE_SOURCE_MIRRORING";
  default:
    return "???";
  }
}

typedef struct {
  std::string image_id;
  uint64_t snap_id;
  uint8_t order;
  uint64_t size;
  uint64_t stripe_unit;
  uint64_t stripe_count;
  uint64_t features;
  uint64_t flags;
} size_info_t;

typedef struct {
  uint64_t size;
  uint64_t du;
} du_info_t;

typedef struct {
  uint64_t seq;
  std::vector<uint64_t> snaps;
} snapc_t;

typedef struct {
  int64_t pool_id;
  std::string image_id;
  uint64_t snap_id;
} parent_spec_t;

bool operator<(const parent_spec_t& lhs, const parent_spec_t& rhs) {
  return ((lhs.pool_id < rhs.pool_id) ||
      (lhs.image_id < rhs.image_id) ||
      (lhs.snap_id < rhs.snap_id));
}

std::string to_str(const parent_spec_t& o) {
  std::string str;
  str = std::to_string(o.pool_id);
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
  uint64_t features;
  uint64_t flags;
  snap_protection_status_t protection_status;
  timespec timestamp;
} snap_info_t;

typedef struct {
  uint64_t id;
  std::string name;
  snap_ns_type_t snap_ns_type;
  uint64_t size;
  uint64_t features;
  uint64_t flags;
  snap_protection_status_t protection_status;
  timespec timestamp;
  uint64_t du;
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
  std::map<uint64_t, snap_info_t> snaps;
  parent_info_t parent;
  timespec timestamp;
  int64_t data_pool_id;
  std::vector<std::string> watchers;
  qos_t qos;
  uint64_t du;
} image_info_v2_t;

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
  int get_name(librados::IoCtx& ioctx,
      const std::string& image_id, std::string* name);
  int get_id(librados::IoCtx& ioctx,
      const std::string& image_name, std::string* id);

  int get_size(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id, size_info_t* info);

  int get_du(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id,
      du_info_t* info);
  int get_du_v2(librados::IoCtx& ioctx,
      const std::string& image_id,
      std::map<uint64_t, du_info_t>* infos);
  int get_du_sync(librados::IoCtx& ioctx,
      const std::string& image_id, uint64_t snap_id,
      du_info_t* info);

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
  // xChild
  //
  int child_list(librados::IoCtx& ioctx,
      std::map<parent_spec_t, std::vector<std::string>>* children);

  //
  // xTrash
  //
  int trash_list(librados::IoCtx& ioctx,
      std::map<std::string, trash_info_t>* trashes);
};

}

#endif /* SRC_INCLUDE_RBD_LIBRBDX_HPP_ */
