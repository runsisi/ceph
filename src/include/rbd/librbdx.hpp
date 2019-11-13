/*
 * librbdx.hpp
 *
 *  Created on: Jul 31, 2019
 *      Author: runsisi
 */

#ifndef SRC_INCLUDE_RBD_LIBRBDX_HPP_
#define SRC_INCLUDE_RBD_LIBRBDX_HPP_

#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "../rados/librados.hpp"
#include "../rbd/librbd.h"
#include "../rbd/librbd.hpp"

namespace librbdx {

template<typename E> struct Enable : std::false_type { };

template<typename E>
typename std::enable_if<Enable<typename std::decay<E>::type>::value, E>::type
operator&(E lhs, E rhs) {
  using T = typename std::underlying_type<E>::type;
  return static_cast<E>(static_cast<T>(lhs) & static_cast<T>(rhs));
}

template<typename E>
typename std::enable_if<Enable<typename std::decay<E>::type>::value, E>::type
operator|(E lhs, E rhs) {
  using T = typename std::underlying_type<E>::type;
  return static_cast<E>(static_cast<T>(lhs) | static_cast<T>(rhs));
}

template<typename E>
typename std::enable_if<Enable<typename std::decay<E>::type>::value, E>::type&
operator&=(E& lhs, E rhs) {
  using T = typename std::underlying_type<E>::type;
  lhs = static_cast<E>(static_cast<T>(lhs) & static_cast<T>(rhs));
  return lhs;
}

template<typename E>
typename std::enable_if<Enable<typename std::decay<E>::type>::value, E>::type&
operator|=(E& lhs, E rhs) {
  using T = typename std::underlying_type<E>::type;
  lhs = static_cast<E>(static_cast<T>(lhs) | static_cast<T>(rhs));
  return lhs;
}

}

namespace librbdx {

enum class info_filter_t : uint64_t {
  INFO_F_CHILDREN_V1    = 1ULL << 0,
  INFO_F_IMAGE_DU       = 1ULL << 1,
  INFO_F_SNAP_DU        = 1ULL << 2,
  INFO_F_ALL            = ~1ULL
};

template<> struct Enable<librbdx::info_filter_t> : std::true_type { };

enum class snap_type_t {
  SNAPSHOT_NAMESPACE_TYPE_USER = RBD_SNAP_NAMESPACE_TYPE_USER,
  SNAPSHOT_NAMESPACE_TYPE_GROUP = RBD_SNAP_NAMESPACE_TYPE_GROUP,
  SNAPSHOT_NAMESPACE_TYPE_TRASH = RBD_SNAP_NAMESPACE_TYPE_TRASH
};

inline std::string stringify(const snap_type_t& o) {
  switch (o) {
  case snap_type_t::SNAPSHOT_NAMESPACE_TYPE_USER:
    return "user";
  case snap_type_t::SNAPSHOT_NAMESPACE_TYPE_GROUP:
    return "group";
  case snap_type_t::SNAPSHOT_NAMESPACE_TYPE_TRASH:
    return "trash";
  default:
    return "unknown";
  }
}

// include/rbd_types.h
enum class snap_protection_status_t {
  PROTECTION_STATUS_UNPROTECTED  = 0, // RBD_PROTECTION_STATUS_UNPROTECTED
  PROTECTION_STATUS_UNPROTECTING = 1, // RBD_PROTECTION_STATUS_UNPROTECTING
  PROTECTION_STATUS_PROTECTED    = 2, // RBD_PROTECTION_STATUS_PROTECTED
  PROTECTION_STATUS_LAST         = 3, // RBD_PROTECTION_STATUS_LAST
};

inline std::string stringify(const snap_protection_status_t& o) {
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

struct parent_t {
  int64_t pool_id;
  std::string pool_namespace;
  std::string image_id;
  uint64_t snap_id;

  bool operator<(const parent_t& rhs) const {
    return std::tie(pool_id, pool_namespace, image_id, snap_id)
      < std::tie(rhs.pool_id, rhs.pool_namespace, rhs.image_id, rhs.snap_id);
  }
};

struct child_t {
  int64_t pool_id;
  std::string pool_namespace;
  std::string image_id;

  bool operator<(const child_t& rhs) const {
    return std::tie(pool_id, pool_namespace, image_id)
      < std::tie(rhs.pool_id, rhs.pool_namespace, rhs.image_id);
  }
};

struct snap_info_t {
  uint64_t id;
  std::string name;
  snap_type_t snap_type;
  uint64_t size;
  uint64_t flags;
  snap_protection_status_t protection_status;
  int64_t timestamp;
  std::set<child_t> children;
  // if fast-diff is disabled then `dirty` equals `du`
  int64_t du;           // OBJECT_EXISTS + OBJECT_EXISTS_CLEAN
  int64_t dirty;        // OBJECT_EXISTS
};

struct image_info_t {
  std::string id;
  std::string name;
  uint8_t order;
  uint64_t size;
  uint64_t features;
  uint64_t op_features;
  uint64_t flags;
  std::map<uint64_t, snap_info_t> snaps;
  parent_t parent;
  int64_t create_timestamp;
  int64_t access_timestamp;
  int64_t modify_timestamp;
  int64_t data_pool_id;
  std::vector<std::string> watchers;
  std::map<std::string, std::string> metas;
  int64_t du;
};

CEPH_RBD_API int get_info(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_t* info,
    uint64_t flags = 0);

CEPH_RBD_API int list(librados::IoCtx& ioctx,
    std::map<std::string, std::string>* images);

CEPH_RBD_API int list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_t, int>>* infos,
    uint64_t flags = 0);
CEPH_RBD_API int list_info(librados::IoCtx& ioctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<image_info_t, int>>* infos,
    uint64_t flags = 0);

}

#endif /* SRC_INCLUDE_RBD_LIBRBDX_HPP_ */
