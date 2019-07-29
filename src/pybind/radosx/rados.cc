/*
 * rados.cc
 *
 *  Created on: Jul 30, 2019
 *      Author: runsisi
 */

#include "rados.h"

#include <list>

namespace radosx {

//
// IoCtx
//
int64_t xIoCtx::get_id() {
  return ioctx.get_id();
}

//
// Rados
//
int xRados::init(const char* const id) {
  return rados.init(id);
}

int xRados::init2(const char* const name, const char* const clustername, uint64_t flags) {
  return rados.init2(name, clustername, flags);
}

int xRados::conf_read_file(const char* const path) const {
  return rados.conf_read_file(path);
}

int xRados::connect() {
  return rados.connect();
}

void xRados::shutdown() {
  rados.shutdown();
}

int xRados::pool_list(std::vector<std::string>& rv) {
  std::list<std::string> l;
  int r = rados.pool_list(l);
  if (r < 0) {
    return r;
  }

  std::vector<std::string> v{
    std::make_move_iterator(std::begin(l)),
    std::make_move_iterator(std::end(l))
  };
  rv.swap(v);
  return 0;
}

int xRados::pool_list2(std::vector<std::pair<int64_t, std::string>>& rv) {
  std::list<std::pair<int64_t, std::string>> l;
  int r = rados.pool_list2(l);
  if (r < 0) {
    return r;
  }

  std::vector<std::pair<int64_t, std::string>> v{
    std::make_move_iterator(std::begin(l)),
    std::make_move_iterator(std::end(l))
  };
  rv.swap(v);
  return 0;
}

int xRados::ioctx_create(const char* name, xIoCtx& rio) {
  librados::IoCtx io;
  int r = rados.ioctx_create(name, io);
  if (r < 0) {
    return r;
  }
  rio.ioctx = io;
  return 0;
}

}
