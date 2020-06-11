/*
 * librbdx.cc
 *
 *  Created on: Jul 31, 2019
 *      Author: runsisi
 */

#include "include/rbd/librbdx.hpp"
#include "librbd/api/xImage.h"

namespace librbdx {

int get_info(librados::IoCtx& ioctx,
    const std::string& image_name,
    const std::string& image_id,
    image_info_t* info,
    uint64_t flags) {
  int r = 0;
  r = librbd::api::xImage<>::get_info(ioctx, image_name, image_id, info, flags);
  return r;
}

int list(librados::IoCtx& ioctx,
    std::map<std::string, std::string>* images) {
  int r = 0;
  images->clear();
  r = librbd::api::xImage<>::list(ioctx, images);
  return r;
}

int list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_t, int>>* infos,
    uint64_t flags) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info(ioctx, infos, flags);
  return r;
}

int list_info(librados::IoCtx& ioctx,
    const std::map<std::string, std::string>& images,
    std::map<std::string, std::pair<image_info_t, int>>* infos,
    uint64_t flags) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info(ioctx, images, infos, flags);
  return r;
}

}
