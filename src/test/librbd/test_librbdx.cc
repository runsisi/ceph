// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/rbd/librbd.hpp"
#include "include/rbd/librbdx.hpp"

#include "gtest/gtest.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <poll.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>

#include "test/librados/test.h"
#include "test/librados/test_cxx.h"
#include "test/librbd/test_support.h"
#include "include/stringify.h"

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

using namespace std;

#define ASSERT_PASSED(x, args...) \
  do {                            \
    bool passed = false;          \
    x(args, &passed);             \
    ASSERT_TRUE(passed);          \
  } while(0)

void register_test_librbdx() {
}

static int get_features(bool *old_format, uint64_t *features)
{
  const char *c = getenv("RBD_FEATURES");
  if (c && strlen(c) > 0) {
    stringstream ss;
    ss << c;
    ss >> *features;
    if (ss.fail())
      return -EINVAL;
    *old_format = false;
    cout << "using new format!" << std::endl;
  } else {
    *old_format = true;
    *features = 0;
    cout << "using old format" << std::endl;
  }

  return 0;
}

static int create_image_pp(librbd::RBD &rbd,
			   librados::IoCtx &ioctx,
			   const char *name,
			   uint64_t size, int *order) {
  bool old_format;
  uint64_t features;
  int r = get_features(&old_format, &features);
  if (r < 0)
    return r;
  if (old_format) {
    librados::Rados rados(ioctx);
    int r = rados.conf_set("rbd_default_format", "1");
    if (r < 0) {
      return r;
    }
    return rbd.create(ioctx, name, size, order);
  } else {
    return rbd.create2(ioctx, name, size, features, order);
  }
}

class TestLibRBDX : public ::testing::Test {
public:

  TestLibRBDX() : m_pool_number() {
  }

  static void SetUpTestCase() {
    _pool_names.clear();
    _unique_pool_names.clear();
    _image_number = 0;
    ASSERT_EQ("", connect_cluster(&_cluster));
    ASSERT_EQ("", connect_cluster_pp(_rados));

    create_optional_data_pool();
  }

  static void TearDownTestCase() {
    rados_shutdown(_cluster);
    _rados.wait_for_latest_osdmap();
    _pool_names.insert(_pool_names.end(), _unique_pool_names.begin(),
		       _unique_pool_names.end());
    for (size_t i = 1; i < _pool_names.size(); ++i) {
      ASSERT_EQ(0, _rados.pool_delete(_pool_names[i].c_str()));
    }
    if (!_pool_names.empty()) {
      ASSERT_EQ(0, destroy_one_pool_pp(_pool_names[0], _rados));
    }
  }

  void SetUp() override {
    ASSERT_NE("", m_pool_name = create_pool());
  }

  bool is_skip_partial_discard_enabled() {
    std::string value;
    EXPECT_EQ(0, _rados.conf_get("rbd_skip_partial_discard", value));
    return value == "true";
  }

  void validate_object_map(rbd_image_t image, bool *passed) {
    uint64_t flags;
    ASSERT_EQ(0, rbd_get_flags(image, &flags));
    *passed = ((flags & RBD_FLAG_OBJECT_MAP_INVALID) == 0);
  }

  void validate_object_map(librbd::Image &image, bool *passed) {
    uint64_t flags;
    ASSERT_EQ(0, image.get_flags(&flags));
    *passed = ((flags & RBD_FLAG_OBJECT_MAP_INVALID) == 0);
  }

  static std::string get_temp_image_name() {
    ++_image_number;
    return "image" + stringify(_image_number);
  }

  static void create_optional_data_pool() {
    bool created = false;
    std::string data_pool;
    ASSERT_EQ(0, create_image_data_pool(_rados, data_pool, &created));
    if (!data_pool.empty()) {
      printf("using image data pool: %s\n", data_pool.c_str());
      if (created) {
        _unique_pool_names.push_back(data_pool);
      }
    }
  }

  std::string create_pool(bool unique = false) {
    librados::Rados rados;
    std::string pool_name;
    if (unique) {
      pool_name = get_temp_pool_name("test-librbd-");
      EXPECT_EQ("", create_one_pool_pp(pool_name, rados));
      _unique_pool_names.push_back(pool_name);
    } else if (m_pool_number < _pool_names.size()) {
      pool_name = _pool_names[m_pool_number];
    } else {
      pool_name = get_temp_pool_name("test-librbd-");
      EXPECT_EQ("", create_one_pool_pp(pool_name, rados));
      _pool_names.push_back(pool_name);
    }
    ++m_pool_number;
    return pool_name;
  }

  static std::vector<std::string> _pool_names;
  static std::vector<std::string> _unique_pool_names;
  static rados_t _cluster;
  static librados::Rados _rados;
  static uint64_t _image_number;

  std::string m_pool_name;
  uint32_t m_pool_number;

};

std::vector<std::string> TestLibRBDX::_pool_names;
std::vector<std::string> TestLibRBDX::_unique_pool_names;
rados_t TestLibRBDX::_cluster;
librados::Rados TestLibRBDX::_rados;
uint64_t TestLibRBDX::_image_number = 0;

static void simple_write_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << std::endl;
}

static void aio_write_test_data(librbd::Image& image, const char *test_data,
                         off_t off, uint32_t iohint, bool *passed)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");
  if (iohint)
    image.aio_write2(off, strlen(test_data), bl, comp, iohint);
  else
    image.aio_write(off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  ASSERT_EQ(0, r);
  printf("finished write\n");
  comp->release();
  *passed = true;
}

static void aio_discard_test_data(librbd::Image& image, off_t off, size_t len, bool *passed)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  image.aio_discard(off, len, comp);
  comp->wait_for_complete();
  int r = comp->get_return_value();
  ASSERT_EQ(0, r);
  comp->release();
  *passed = true;
}

TEST_F(TestLibRBDX, GetInfoPP)
{
  REQUIRE_FORMAT_V2();
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  librbd::Image image;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 4 << 20; // discard works on object boundary
  std::string image_id;
  librbdx::image_info_t image_info;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));
  ASSERT_EQ(0, rbd.open(ioctx, image, name.c_str(), nullptr));
  ASSERT_EQ(0, image.get_id(&image_id));
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info));
  ASSERT_EQ(-1, image_info.du);
  ASSERT_EQ(-1, image_info.dirty);
  ASSERT_EQ(0, librbdx::get_info(ioctx, "", image_id, &image_info, librbdx::INFO_F_CHILDREN_V1));
  ASSERT_EQ(-1, image_info.du);
  ASSERT_EQ(-1, image_info.dirty);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_IMAGE_DU));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_SNAP_DU));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_IMAGE_DU | librbdx::INFO_F_SNAP_DU));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);
  ASSERT_EQ(0, librbdx::get_info(ioctx, "", image_id, &image_info, librbdx::INFO_F_ALL));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);

  struct timespec timestamp;
  ASSERT_EQ(0, image.get_create_timestamp(&timestamp));
  ASSERT_EQ(timestamp.tv_sec, image_info.create_timestamp);

  librbd::image_info_t stat;
  ASSERT_EQ(0, image.stat(stat, sizeof(stat)));
  ASSERT_EQ(stat.order, image_info.order);
  ASSERT_EQ(stat.size, image_info.size);

  ASSERT_EQ((size_t)1, image_info.watchers.size());

  auto find_snap = [](const std::map<uint64_t, librbdx::snap_info_t>& snaps,
      const std::string& name, librbdx::snap_info_t* snap) -> int {
    for (auto& it : snaps) {
      auto& s = it.second;
      if (s.name == name) {
        *snap = s;
        return 0;
      }
    }
    return -ENOENT;
  };

  // snaps
  ASSERT_EQ(0, image.snap_create("snap1"));
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, "", &image_info, librbdx::INFO_F_SNAP_DU));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);
  ASSERT_EQ((size_t)1, image_info.snaps.size());
  librbdx::snap_info_t snap;
  ASSERT_EQ(0, find_snap(image_info.snaps, "snap1", &snap));
  ASSERT_EQ(librbdx::snap_type_t::SNAPSHOT_NAMESPACE_TYPE_USER, snap.snap_type);
  ASSERT_EQ(size, snap.size);
  ASSERT_EQ((size_t)0, snap.children.size());
  ASSERT_EQ(0, snap.du);
  ASSERT_EQ(0, snap.dirty);

  // children
  ASSERT_EQ(0, image.snap_protect("snap1"));
  string cname1 = get_temp_image_name();
  string cname2 = get_temp_image_name();
  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap1", ioctx,
      cname1.c_str(), features, &order));
  ASSERT_EQ(0, rbd.clone(ioctx, name.c_str(), "snap1", ioctx,
      cname2.c_str(), features, &order));

  ASSERT_EQ(0, librbdx::get_info(ioctx, name, "", &image_info, librbdx::INFO_F_CHILDREN_V1));
  ASSERT_EQ((size_t)1, image_info.snaps.size());
  ASSERT_EQ(0, find_snap(image_info.snaps, "snap1", &snap));
  ASSERT_EQ((size_t)2, snap.children.size());

  ASSERT_EQ(0, rbd.namespace_create(ioctx, "name1"));
  librados::IoCtx ns_ioctx;
  ns_ioctx.dup(ioctx);

  std::string cname3 = get_temp_image_name();
  ns_ioctx.set_namespace("name1");
  librbd::ImageOptions opts;
  opts.set(RBD_IMAGE_OPTION_FEATURES, features & ~RBD_FEATURES_IMPLICIT_ENABLE);
  opts.set(RBD_IMAGE_OPTION_ORDER, order);
  opts.set(RBD_IMAGE_OPTION_CLONE_FORMAT, 2);
  ASSERT_EQ(0, rbd.clone3(ioctx, name.c_str(), "snap1", ns_ioctx,
      cname3.c_str(), opts));

  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_ALL));
  ASSERT_EQ((size_t)1, image_info.snaps.size());
  ASSERT_EQ(0, find_snap(image_info.snaps, "snap1", &snap));
  ASSERT_EQ((size_t)3, snap.children.size());

  std::string cid3;
  librbd::Image cimage3;
  ASSERT_EQ(0, rbd.open(ns_ioctx, cimage3, cname3.c_str(), nullptr));
  ASSERT_EQ(0, cimage3.get_id(&cid3));
  ASSERT_EQ(0, cimage3.close());
  ASSERT_TRUE(snap.children.find(librbdx::child_t{ns_ioctx.get_id(), "name1", cid3}) != snap.children.end());

  ASSERT_EQ(0, rbd.remove(ioctx,  cname1.c_str()));
  ASSERT_EQ(0, rbd.remove(ioctx,  cname2.c_str()));
  ASSERT_EQ(0, rbd.remove(ns_ioctx,  cname3.c_str()));
  ASSERT_EQ(0, rbd.namespace_remove(ns_ioctx, "name1"));

  // dus
  char test_data[TEST_IO_SIZE];
  char zero_data[TEST_IO_SIZE];

  for (int i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE - 1] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  // write data
  ASSERT_PASSED(aio_write_test_data, image, test_data, 0, 0);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_IMAGE_DU));
  int64_t du = std::min<int64_t>(size, 1 << order);
  ASSERT_EQ(du, image_info.du); // assume TEST_IO_SIZE > object size
  ASSERT_EQ(du, image_info.dirty);

  // create snap
  ASSERT_EQ(0, image.snap_create("snap2"));
  ASSERT_EQ(0, librbdx::get_info(ioctx, "", image_id, &image_info, librbdx::INFO_F_SNAP_DU));
  du = std::min<int64_t>(size, 1 << order);
  ASSERT_EQ(du, image_info.du);
  ASSERT_EQ(0, image_info.dirty);
  ASSERT_EQ(0, find_snap(image_info.snaps, "snap2", &snap));
  ASSERT_EQ(du, snap.du);
  ASSERT_EQ(du, snap.dirty);
  ASSERT_EQ((size_t)2, image_info.snaps.size());

  // write data
  ASSERT_PASSED(aio_write_test_data, image, test_data, 0, 0);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_IMAGE_DU));
  du = std::min<int64_t>(size, 1 << order);
  ASSERT_EQ(du, image_info.du);
  ASSERT_EQ(du, image_info.dirty);

  // write zero data then discard
  ASSERT_PASSED(aio_write_test_data, image, zero_data, 0, 0);
  ASSERT_PASSED(aio_discard_test_data, image, 0, size);
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, "", &image_info, librbdx::INFO_F_IMAGE_DU));
  ASSERT_EQ(0, image_info.du);
  ASSERT_EQ(0, image_info.dirty);

  ASSERT_EQ(0, image.snap_unprotect("snap1"));
  ASSERT_EQ(0, image.snap_remove("snap1"));
  ASSERT_EQ(0, image.snap_remove("snap2"));
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, "", &image_info, librbdx::INFO_F_SNAP_DU));
  ASSERT_EQ((size_t)0, image_info.snaps.size());

  // metas
  ASSERT_EQ((size_t)0, image_info.metas.size());
  ASSERT_EQ(0, image.metadata_set("key1", "value1"));
  ASSERT_EQ(0, image.metadata_set("key2", "value2"));
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info, librbdx::INFO_F_ALL));
  ASSERT_EQ((size_t)2, image_info.metas.size());
  ASSERT_TRUE(image_info.metas["key1"] == "value1");
  ASSERT_TRUE(image_info.metas["key2"] == "value2");
  ASSERT_EQ(0, image.metadata_remove("key2"));
  ASSERT_EQ(0, librbdx::get_info(ioctx, name, image_id, &image_info));
  ASSERT_EQ((size_t)1, image_info.metas.size());
  ASSERT_TRUE(image_info.metas["key1"] == "value1");

  ioctx.close();
}

TEST_F(TestLibRBDX, ListPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  std::map<std::string, std::string> images;
  ASSERT_EQ(0, librbdx::list(ioctx, &images));

  ioctx.close();
}

TEST_F(TestLibRBDX, ListInfoPP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  std::map<std::string, std::pair<librbdx::image_info_t, int>> infos;
  ASSERT_EQ(0, librbdx::list_info(ioctx, &infos));
  ASSERT_EQ(0, librbdx::list_info(ioctx, &infos, librbdx::INFO_F_CHILDREN_V1));
  ASSERT_EQ(0, librbdx::list_info(ioctx, &infos, librbdx::INFO_F_IMAGE_DU));
  ASSERT_EQ(0, librbdx::list_info(ioctx, &infos, librbdx::INFO_F_SNAP_DU));
  ASSERT_EQ(0, librbdx::list_info(ioctx, &infos, librbdx::INFO_F_ALL));

  ioctx.close();
}

TEST_F(TestLibRBDX, ListInfo2PP)
{
  REQUIRE_FORMAT_V2();

  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(m_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  int order = 0;
  std::string name = get_temp_image_name();
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image_pp(rbd, ioctx, name.c_str(), size, &order));

  std::map<std::string, std::string> images;
  ASSERT_EQ(0, librbdx::list(ioctx, &images));
  std::map<std::string, std::pair<librbdx::image_info_t, int>> infos;
  ASSERT_EQ(0, librbdx::list_info(ioctx, images, &infos, librbdx::INFO_F_ALL));

  ioctx.close();
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"
