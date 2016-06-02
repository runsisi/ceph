// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RBD_OBJECT_MAP_TYPES_H
#define CEPH_RBD_OBJECT_MAP_TYPES_H

#include "include/int_types.h"

static const uint8_t OBJECT_NONEXISTENT  = 0;
static const uint8_t OBJECT_EXISTS       = 1;
// set by
// ObjectRemoveRequest::pre_object_map_update, if does not have parent
// ObjectTrimRequest::pre_object_map_update
// librbd::operation::TrimRequest<I>::send_pre_copyup
// librbd::operation::TrimRequest<I>::send_pre_remove
static const uint8_t OBJECT_PENDING      = 2;
static const uint8_t OBJECT_EXISTS_CLEAN = 3;

#endif // CEPH_RBD_OBJECT_MAP_TYPES_H
