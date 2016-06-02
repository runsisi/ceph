// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "SnapMapper.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper."

using std::string;

const string SnapMapper::MAPPING_PREFIX = "MAP_";
const string SnapMapper::OBJECT_PREFIX = "OBJ_";

int OSDriver::get_keys(
  const std::set<std::string> &keys,
  std::map<std::string, bufferlist> *out)
{
  return os->omap_get_values(cid, hoid, keys, out);
}

int OSDriver::get_next(
  const std::string &key,
  pair<std::string, bufferlist> *next)
{
  // see PG::PG
  // cid = coll_t()
  // hoid = OSD::make_snapmapper_oid()), i.e., "snapmapper"
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(cid, hoid);
  if (!iter) {
    ceph_abort();
    return -EINVAL;
  }

  iter->upper_bound(key);
  if (iter->valid()) {
    if (next)
      *next = make_pair(iter->key(), iter->value());
    return 0;
  } else {
    return -ENOENT;
  }
}

struct Mapping {
  snapid_t snap;
  hobject_t hoid;

  explicit Mapping(const pair<snapid_t, hobject_t> &in)
    : snap(in.first), hoid(in.second) {}
  Mapping() : snap(0) {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(snap, bl);
    ::encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(snap, bl);
    ::decode(hoid, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(Mapping)

// called by
// SnapMapper::to_raw_key
// SnapMapper::get_next_objects_to_trim
string SnapMapper::get_prefix(snapid_t snap)
{
  char buf[100];
  int len = snprintf(
    buf, sizeof(buf),
    "%.*X_", (int)(sizeof(snap)*2),
    static_cast<unsigned>(snap));

  // e.g., "MAP_000000000000002A_"
  return MAPPING_PREFIX + string(buf, len);
}

// called by
// SnapMapper::to_raw
// SnapMapper::update_snaps
// SnapMapper::_remove_oid
string SnapMapper::to_raw_key(
  const pair<snapid_t, hobject_t> &in)
{
  // for replicated pg, shard_prefix is empty string, e.g.,
  // "MAP_000000000000002A_" + "" + "0000000000000000.00B7CF30.2a.rbd%udata%e12072ae8944a%e00000000000000fd.."
  return get_prefix(in.first) + shard_prefix + in.second.to_str();
}

// called by
// SnapMapper::add_oid
pair<string, bufferlist> SnapMapper::to_raw(
  const pair<snapid_t, hobject_t> &in)
{
  bufferlist bl;
  ::encode(Mapping(in), bl);
  // "MAP_000000000000002A_"
  return make_pair(to_raw_key(in), bl);
}

// called by
// SnapMapper::get_next_objects_to_trim
pair<snapid_t, hobject_t> SnapMapper::from_raw(
  const pair<std::string, bufferlist> &image)
{
  Mapping map;

  bufferlist bl(image.second);
  bufferlist::iterator bp(bl.begin());
  ::decode(map, bp);
  return make_pair(map.snap, map.hoid);
}

// called by
// SnapMapper::get_next_objects_to_trim, for assertion only
bool SnapMapper::is_mapping(const string &to_test)
{
  // "MAP_"
  return to_test.substr(0, MAPPING_PREFIX.size()) == MAPPING_PREFIX;
}

string SnapMapper::to_object_key(const hobject_t &hoid)
{
  // e.g., "OBJ_" + "" + "0000000000000000.00B7CF30.2a.rbd%udata%e12072ae8944a%e00000000000000fd.."
  return OBJECT_PREFIX + shard_prefix + hoid.to_str();
}

void SnapMapper::object_snaps::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(oid, bl);
  ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void SnapMapper::object_snaps::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(oid, bl);
  ::decode(snaps, bl);
  DECODE_FINISH(bl);
}

// called by
// SnapMapper::update_snaps
// SnapMapper::add_oid
// SnapMapper::_remove_oid
// SnapMapper::get_snaps(const hobject_t &oid, std::set<snapid_t> *snaps)
int SnapMapper::get_snaps(
  const hobject_t &oid,
  object_snaps *out)
{
  assert(check(oid));

  set<string> keys;
  map<string, bufferlist> got;

  // e.g., "OBJ_0000000000000000.00B7CF30.2a.rbd%udata%e12072ae8944a%e00000000000000fd.."
  keys.insert(to_object_key(oid));
  int r = backend.get_keys(keys, &got);
  if (r < 0) {
    dout(20) << __func__ << " " << oid << " got err " << r << dendl;
    return r;
  }
  if (got.empty()) {
    dout(20) << __func__ << " " << oid << " got.empty()" << dendl;
    return -ENOENT;
  }

  // e.g.,
  //  00000000  01 01 59 00 00 00 04 03  47 00 00 00 00 00 00 00  |..Y.....G.......|
  //  00000010  26 00 00 00 72 62 64 5f  64 61 74 61 2e 31 32 30  |&...rbd_data.120|
  //  00000020  37 32 61 65 38 39 34 34  61 2e 30 30 30 30 30 30  |72ae8944a.000000|
  //  00000030  30 30 30 30 30 30 30 30  66 64 2a 00 00 00 00 00  |00000000fd*.....|
  //  00000040  00 00 00 7b fc 03 00 00  00 00 00 00 00 00 00 00  |...{............|
  //  00000050  00 00 00 01 00 00 00 2a  00 00 00 00 00 00 00     |.......*.......|
  //  0000005f
  if (out) {
    bufferlist::iterator bp = got.begin()->second.begin();
    ::decode(*out, bp);
    dout(20) << __func__ << " " << oid << " " << out->snaps << dendl;
    assert(!out->snaps.empty());
  } else {
    dout(20) << __func__ << " " << oid << " (out == NULL)" << dendl;
  }
  return 0;
}

// called by
// SnapMapper::_remove_oid
void SnapMapper::clear_snaps(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  assert(check(oid));
  set<string> to_remove;
  // "OBJ_"
  to_remove.insert(to_object_key(oid));
  if (g_conf->subsys.should_gather(ceph_subsys_osd, 20)) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
}

// called by
// SnapMapper::update_snaps
// SnapMapper::add_oid
void SnapMapper::set_snaps(
  const hobject_t &oid,
  const object_snaps &in,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  assert(check(oid));

  map<string, bufferlist> to_set;
  bufferlist bl;
  ::encode(in, bl);

  // "OBJ_"
  to_set[to_object_key(oid)] = bl;
  dout(20) << __func__ << " " << oid << " " << in.snaps << dendl;
  if (g_conf->subsys.should_gather(ceph_subsys_osd, 20)) {
    for (auto& i : to_set) {
      dout(20) << __func__ << " set " << i.first << dendl;
    }
  }
  backend.set_keys(to_set, t);
}

// called by
// PG::update_snap_map, which called by PG::append_log, which called by PrimaryLogPG::log_operation
int SnapMapper::update_snaps(
  const hobject_t &oid,
  const set<snapid_t> &new_snaps,
  const set<snapid_t> *old_snaps_check, // PG::update_snap_map call us with 0
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << " " << new_snaps
	   << " was " << (old_snaps_check ? *old_snaps_check : set<snapid_t>())
	   << dendl;
  assert(check(oid));

  if (new_snaps.empty())
    return remove_oid(oid, t);

  object_snaps out;
  // "OBJ_"
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;

  if (old_snaps_check)
    assert(out.snaps == *old_snaps_check);

  object_snaps in(oid, new_snaps);
  // "OBJ_"
  set_snaps(oid, in, t);

  set<string> to_remove;
  for (set<snapid_t>::iterator i = out.snaps.begin();
       i != out.snaps.end();
       ++i) {
    if (!new_snaps.count(*i)) {
      // "MAP_000000000000002A_"
      to_remove.insert(to_raw_key(make_pair(*i, oid)));
    }
  }
  if (g_conf->subsys.should_gather(ceph_subsys_osd, 20)) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
  return 0;
}

// called by
// PG::update_object_snap_mapping
// PG::update_snap_map
// PG::_scan_snaps
// PrimaryLogPG::on_local_recover
// ceph_objectstore_tool.cc/get_attrs
void SnapMapper::add_oid(
  const hobject_t &oid,
  const set<snapid_t>& snaps,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << " " << snaps << dendl;
  assert(check(oid));
  {
    object_snaps out;
    int r = get_snaps(oid, &out);
    assert(r == -ENOENT);
  }

  object_snaps _snaps(oid, snaps);
  // set keys start with "OBJ_"
  set_snaps(oid, _snaps, t);

  map<string, bufferlist> to_add;
  for (set<snapid_t>::iterator i = snaps.begin();
       i != snaps.end();
       ++i) {
    // "MAP_000000000000002A_"
    to_add.insert(to_raw(make_pair(*i, oid)));
  }
  if (g_conf->subsys.should_gather(ceph_subsys_osd, 20)) {
    for (auto& i : to_add) {
      dout(20) << __func__ << " set " << i.first << dendl;
    }
  }

  // set keys start with "MAP_"
  backend.set_keys(to_add, t);
}

// called by
// PG::proc_primary_info
// PrimaryLogPG::AwaitAsyncWork::react(const DoSnapWork)
int SnapMapper::get_next_objects_to_trim(
  snapid_t snap,
  unsigned max,
  vector<hobject_t> *out)
{
  assert(out);
  assert(out->empty());

  int r = 0;
  // prefixes were inserted by SnapMapper::update_bits
  for (set<string>::iterator i = prefixes.begin();
       i != prefixes.end() && out->size() < max && r == 0;
       ++i) {
    // "MAP_000000000000002A_"
    string prefix(get_prefix(snap) + *i);
    string pos = prefix;

    while (out->size() < max) {
      pair<string, bufferlist> next;
      // MapCacher::MapCacher<std::string, bufferlist>
      r = backend.get_next(pos, &next); // rely on os driver->get_next, see SnapMapper.h/OSDriver
                                        // which rely on ObjectStore::get_omap_iterator
      dout(20) << __func__ << " get_next(" << pos << ") returns " << r
	       << " " << next << dendl;
      if (r != 0) {
	break; // Done
      }

      if (next.first.substr(0, prefix.size()) != prefix) {
	break; // Done with this prefix
      }

      // starts with "MAP_" prefix
      assert(is_mapping(next.first));

      dout(20) << __func__ << " " << next.first << dendl;
      pair<snapid_t, hobject_t> next_decoded(from_raw(next));
      assert(next_decoded.first == snap);
      assert(check(next_decoded.second));

      out->push_back(next_decoded.second);

      pos = next.first;
    }
  }

  if (out->size() == 0) {
    return -ENOENT;
  } else {
    return 0;
  }
}

// called by
// OSD::recursive_remove_collection
// OSD.cc/remove_dir
// PG::clear_object_snap_mapping
// PG::update_object_snap_mapping
// PG::update_snap_map
// PG::_scan_snaps
// SnapMapper::update_snaps
// ceph_objectstore_tool.cc/remove_object
int SnapMapper::remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  assert(check(oid));
  return _remove_oid(oid, t);
}

// called by
// SnapMapper::remove_oid
int SnapMapper::_remove_oid(
  const hobject_t &oid,
  MapCacher::Transaction<std::string, bufferlist> *t)
{
  dout(20) << __func__ << " " << oid << dendl;
  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;

  // "OBJ_"
  clear_snaps(oid, t);

  set<string> to_remove;
  for (set<snapid_t>::iterator i = out.snaps.begin();
       i != out.snaps.end();
       ++i) {
    // "MAP_000000000000002A_"
    to_remove.insert(to_raw_key(make_pair(*i, oid)));
  }
  if (g_conf->subsys.should_gather(ceph_subsys_osd, 20)) {
    for (auto& i : to_remove) {
      dout(20) << __func__ << " rm " << i << dendl;
    }
  }
  backend.remove_keys(to_remove, t);
  return 0;
}

// called by
// PG::_scan_snaps, which called by PG::build_scrub_map_chunk
int SnapMapper::get_snaps(
  const hobject_t &oid,
  std::set<snapid_t> *snaps)
{
  assert(check(oid));

  object_snaps out;
  int r = get_snaps(oid, &out);
  if (r < 0)
    return r;

  if (snaps)
    snaps->swap(out.snaps);
  return 0;
}
