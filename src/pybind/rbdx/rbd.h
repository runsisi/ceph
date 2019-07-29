/*
 * rbd.h
 *
 *  Created on: Jul 30, 2019
 *      Author: runsisi
 */

#ifndef SRC_PYBIND_RBDX_RBD_H_
#define SRC_PYBIND_RBDX_RBD_H_

#include "rados/librados.hpp"
#include "rbd/librbd.hpp"

#include <map>
#include <string>

namespace rbdx {

class xRBD {
public:
  librbd::RBD rbd;

public:
  int x_list(librbd::IoCtx& ioctx, std::map<std::string, std::string>* images);
};

}

#endif /* SRC_PYBIND_RBDX_RBD_H_ */
