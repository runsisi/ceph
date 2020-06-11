// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"

#include "librbd/api/xImage.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace api {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockxImage : public TestMockFixture {

};

TEST_F(TestMockxImage, GetInfo) {
  REQUIRE_FORMAT_V2();

  librados::MockTestMemIoCtxImpl& mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  EXPECT_CALL(mock_io_ctx,
      exec(_, _, StrEq("rbd"), StrEq("x_image_get"), _, _, _))
      .WillOnce(Return(0));
  EXPECT_CALL(mock_io_ctx,
      exec(_, _, StrEq("rbd"), StrEq("metadata_list"), _, _, _))
      .WillOnce(Return(0));

  std::string image_name = "image name";
  std::string image_id = "image id";
  librbdx::image_info_t info;
  ASSERT_EQ(0, xImage<>::get_info(m_ioctx, image_name, image_id, &info));
}

}
}
