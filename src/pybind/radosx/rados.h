/*
 * rados.h
 *
 *  Created on: Jul 30, 2019
 *      Author: runsisi
 */

#ifndef SRC_PYBIND_RADOSX_RADOS_H_
#define SRC_PYBIND_RADOSX_RADOS_H_

#include "rados/librados.hpp"

#include <string>
#include <vector>

namespace radosx {

class xIoCtx {
public:
  librados::IoCtx ioctx;

public:
  int64_t get_id();
};

class xRados : public librados::Rados {
public:
  librados::Rados rados;

public:
  int init(const char* const id);
  int init2(const char* const name, const char* const clustername, uint64_t flags);
  int conf_read_file(const char* const path) const;
  int connect();
  void shutdown();
  int pool_list(std::vector<std::string>& rv);
  int pool_list2(std::vector<std::pair<int64_t, std::string>>& rv);
  int ioctx_create(const char* name, xIoCtx& io);
};

}

#endif /* SRC_PYBIND_RADOSX_RADOS_H_ */
