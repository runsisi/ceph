/*
 * rbd.cc
 *
 *  Created on: Jul 30, 2019
 *      Author: runsisi
 */

#include "rbd.h"

namespace rbdx {

//
// RBD
//

int xRBD::x_list(librados::IoCtx& ioctx, std::map<std::string,
    std::string>* images) {
  return rbd.x_list(ioctx, images);
}

}
