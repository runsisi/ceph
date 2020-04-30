/*
 * librbdx.cc
 *
 *  Created on: Jul 31, 2019
 *      Author: runsisi
 */

#include "include/rbd/librbdx.hpp"

#include <cstdlib>

#include "include/utime.h"
#include "librbd/api/xChild.h"
#include "librbd/api/xImage.h"
#include "librbd/api/xTrash.h"

namespace librbdx {

int xRBD::get_info(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_t* info) {
  int r = 0;
  r = librbd::api::xImage<>::get_info(ioctx, image_id, info);
  return r;
}

int xRBD::get_info_v2(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v2_t* info) {
  int r = 0;
  r = librbd::api::xImage<>::get_info_v2(ioctx, image_id, info);
  return r;
}

int xRBD::get_info_v3(librados::IoCtx& ioctx,
    const std::string& image_id, image_info_v3_t* info) {
  int r = 0;
  r = librbd::api::xImage<>::get_info_v3(ioctx, image_id, info);
  return r;
}

int xRBD::list_du(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<du_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_du(ioctx, infos);
  return r;
}

int xRBD::list_du(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<du_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_du(ioctx, image_ids, infos);
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_du_v2(ioctx, infos);
  return r;
}

int xRBD::list_du_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<std::map<uint64_t, du_info_t>, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_du_v2(ioctx, image_ids, infos);
  return r;
}

int xRBD::list(librados::IoCtx& ioctx,
    std::map<std::string, std::string>* images) {
  int r = 0;
  images->clear();
  r = librbd::api::xImage<>::list(ioctx, images);
  return r;
}

int xRBD::list_info(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info(ioctx, infos);
  return r;
}

int xRBD::list_info(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info(ioctx, image_ids, infos);
  return r;
}

int xRBD::list_info_v2(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info_v2(ioctx, infos);
  return r;
}

int xRBD::list_info_v2(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v2_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info_v2(ioctx, image_ids, infos);
  return r;
}

int xRBD::list_info_v3(librados::IoCtx& ioctx,
    std::map<std::string, std::pair<image_info_v3_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info_v3(ioctx, infos);
  return r;
}

int xRBD::list_info_v3(librados::IoCtx& ioctx,
    const std::vector<std::string>& image_ids,
    std::map<std::string, std::pair<image_info_v3_t, int>>* infos) {
  int r = 0;
  infos->clear();
  r = librbd::api::xImage<>::list_info_v3(ioctx, image_ids, infos);
  return r;
}

//
// xTrash
//
int xRBD::trash_list(librados::IoCtx& ioctx,
    std::map<std::string, trash_info_t>* trashes) {
  int r = 0;
  trashes->clear();
  r = librbd::api::xTrash<>::list(ioctx, trashes);
  return r;
}

//
// xChild, for legacy clone v1 only
//
int xRBD::child_list(librados::IoCtx& ioctx,
    std::map<parent_spec_t, std::vector<std::string>>* children) {
  int r = 0;
  children->clear();
  r = librbd::api::xChild<>::list(ioctx, children);
  return r;
}

}
