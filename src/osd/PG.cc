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

#include "PG.h"
#include "common/errno.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDRepOpReply.h"
#include "common/BackTrace.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/pg.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

// prefix pgmeta_oid keys with _ so that PGLog::read_log() can
// easily skip them
const string infover_key("_infover");
const string info_key("_info");
const string biginfo_key("_biginfo");
const string epoch_key("_epoch");


template <class T>
static ostream& _prefix(std::ostream *_dout, T *t)
{
  return *_dout << t->gen_prefix();
}

void PG::get(const string &tag) 
{
  ref.inc();
#ifdef PG_DEBUG_REFS
  Mutex::Locker l(_ref_id_lock);
  if (!_tag_counts.count(tag)) {
    _tag_counts[tag] = 0;
  }
  _tag_counts[tag]++;
#endif
}

void PG::put(const string &tag)
{
#ifdef PG_DEBUG_REFS
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_tag_counts.count(tag));
    _tag_counts[tag]--;
    if (_tag_counts[tag] == 0) {
      _tag_counts.erase(tag);
    }
  }
#endif
  if (ref.dec() == 0)
    delete this;
}

#ifdef PG_DEBUG_REFS
uint64_t PG::get_with_id()
{
  ref.inc();
  Mutex::Locker l(_ref_id_lock);
  uint64_t id = ++_ref_id;
  BackTrace bt(0);
  stringstream ss;
  bt.print(ss);
  dout(20) << __func__ << ": " << info.pgid << " got id " << id << dendl;
  assert(!_live_ids.count(id));
  _live_ids.insert(make_pair(id, ss.str()));
  return id;
}

void PG::put_with_id(uint64_t id)
{
  dout(20) << __func__ << ": " << info.pgid << " put id " << id << dendl;
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_live_ids.count(id));
    _live_ids.erase(id);
  }
  if (ref.dec() == 0)
    delete this;
}

void PG::dump_live_ids()
{
  Mutex::Locker l(_ref_id_lock);
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live ids:" << dendl;
  for (map<uint64_t, string>::iterator i = _live_ids.begin();
       i != _live_ids.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live tags:" << dendl;
  for (map<string, uint64_t>::iterator i = _tag_counts.begin();
       i != _tag_counts.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
}
#endif

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  assert(pi);
  info = *pi;
  auid = pi->auid;
  name = map->get_pool_name(id);
  if (pi->get_snap_epoch() == map->get_epoch()) {
    // snap(either pool snap or selfmanaged snap) created/deleted in this epoch,
    // we note down the newly deleted snap(s) here
    pi->build_removed_snaps(newly_removed_snaps);
    newly_removed_snaps.subtract(cached_removed_snaps);
    cached_removed_snaps.union_of(newly_removed_snaps);
    snapc = pi->get_snap_context(); // get default pool snap context
  } else {
    newly_removed_snaps.clear();
  }
  lgeneric_subdout(g_ceph_context, osd, 20)
    << "PGPool::update cached_removed_snaps "
    << cached_removed_snaps
    << " newly_removed_snaps "
    << newly_removed_snaps
    << " snapc " << snapc
    << (pi->get_snap_epoch() == map->get_epoch() ?
	" (updated)":" (no change)")
    << dendl;
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, spg_t p) :
  osd(o),
  cct(o->cct),
  osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(
    &osdriver,
    p.ps(),
    p.get_split_bits(curmap->get_pg_num(_pool.id)),
    _pool.id,
    p.shard),
  map_lock("PG::map_lock"),
  osdmap_ref(curmap), last_persisted_osdmap_ref(curmap), pool(_pool),
  _lock("PG::_lock"),
  ref(0),
  #ifdef PG_DEBUG_REFS
  _ref_id_lock("PG::_ref_id_lock"), _ref_id(0),
  #endif
  deleting(false), dirty_info(false), dirty_big_info(false),
  info(p),
  info_struct_v(0),
  coll(p), pg_log(cct),
  pgmeta_oid(p.make_pgmeta_oid()),
  missing_loc(this),
  recovery_item(this), stat_queue_item(this),
  snap_trim_queued(false),
  scrub_queued(false),
  recovery_ops_active(0),
  role(0),
  state(0),
  send_notify(false),
  pg_whoami(osd->whoami, p.shard), // identify which osd we are on, and which shard of 
                                   // this pg that we are, for replica pg, the shard should be NO_SHARD
  need_up_thru(false),
  last_peering_reset(0),
  heartbeat_peer_lock("PG::heartbeat_peer_lock"),
  backfill_reserved(false),
  backfill_reserving(false),
  flushes_in_progress(0),
  pg_stats_publish_lock("PG::pg_stats_publish_lock"),
  pg_stats_publish_valid(false),
  osr(osd->osr_registry.lookup_or_create(p, (stringify(p)))), // every pg associates with an instance of ObjectStore::Sequencer
  finish_sync_event(NULL),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(this),
  pg_id(p),
  peer_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  acting_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  upacting_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  do_sort_bitwise(false)
{
#ifdef PG_DEBUG_REFS
  osd->add_pgid(p, this);
#endif
}

PG::~PG()
{
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock_suspend_timeout(ThreadPool::TPHandle &handle)
{
  handle.suspend_tp_timeout();
  lock();
  handle.reset_tp_timeout();
}

void PG::lock(bool no_lockdep) const
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);
  assert(!dirty_big_info);

  dout(30) << "lock" << dendl;
}

std::string PG::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  if (_lock.is_locked_by_me()) {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << info.pgid << "(unlocked)] ";
  }
  return out.str();
}
  
/********* PG **********/

void PG::proc_master_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo,
  pg_log_t &olog, pg_missing_t& omissing, pg_shard_t from)
{
  dout(10) << "proc_master_log for osd." << from << ": "
	   << olog << " " << omissing << dendl;
  assert(!is_peered() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from); // merge authority pg log with ours
  
  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  // See doc/dev/osd_internals/last_epoch_started
  if (oinfo.last_epoch_started > info.last_epoch_started)
    info.last_epoch_started = oinfo.last_epoch_started;

  // merge pg info with the info the authority pg has
  info.history.merge(oinfo.history);
  assert(info.last_epoch_started >= info.history.last_epoch_started);

  peer_missing[from].swap(omissing);
}

// only called by RecoveryState::Active::react(MLogRec) or 
// RecoveryState::GetMissing::react(MLogRec)
void PG::proc_replica_log(
  ObjectStore::Transaction& t,
  pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing,
  pg_shard_t from)
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  // process the peer's log and update the peer's missing and info
  pg_log.proc_replica_log(t, oinfo, olog, omissing, from);

  peer_info[from] = oinfo; // note down peer's info
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  
  might_have_unfound.insert(from);

  for (map<hobject_t, pg_missing_t::item, hobject_t::ComparatorWithDefault>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " after missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }
       
  peer_missing[from].swap(omissing); // note down peer's missing
}

bool PG::proc_replica_info(
  pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch)
{
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) { // got already
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  // the peer must be up and up from at least the msg sent off, i.e.
  // it's up in current map, and up_from <= send_epoch, i.e. the peer must
  // live the same as or longer than the received msg
  if (!get_osdmap()->has_been_up_since(from.osd, send_epoch)) {
    dout(10) << " got info " << oinfo << " from down osd." << from
	     << " discarding" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  assert(is_primary());
  peer_info[from] = oinfo; // update peer info
  
  might_have_unfound.insert(from); // we may have objects on them

  // remove last scrub job from OSDService::sched_scrub_pg
  unreg_next_scrub();
  
  // update PG history to the latest between us and the peer
  if (info.history.merge(oinfo.history)) // update primary pg info (every fields) to the latest
    dirty_info = true;

  // reg a scrub job in OSDService::sched_scrub_pg
  reg_next_scrub();
  
  // stray?
  if (!is_up(from) && !is_acting(from)) { // the src is not in up set and acting set, it's a stray
    dout(10) << " osd." << from << " has stray content: " << oinfo << dendl;

    // stray set that may holding our pg's data
    stray_set.insert(from);

    // the MNotifyRec may come from a resurrected parent PG instance, refer to
    // OSD::handle_pg_peering_evt
    if (is_clean()) {
      purge_strays(); // we are clean, can purge the stray set safely
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end()) // new peer info, may from stray peer
    update_heartbeat_peers();

  return true;
}

void PG::remove_snap_mapped_object(
  ObjectStore::Transaction &t, const hobject_t &soid)
{
  t.remove(
    coll,
    ghobject_t(soid, ghobject_t::NO_GEN, pg_whoami.shard));
  clear_object_snap_mapping(&t, soid);
}

void PG::clear_object_snap_mapping(
  ObjectStore::Transaction *t, const hobject_t &soid)
{
  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  if (soid.snap < CEPH_MAXSNAP) {
    int r = snap_mapper.remove_oid(
      soid,
      &_t);
    if (!(r == 0 || r == -ENOENT)) {
      derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

void PG::update_object_snap_mapping(
  ObjectStore::Transaction *t, const hobject_t &soid, const set<snapid_t> &snaps)
{
  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  assert(soid.snap < CEPH_MAXSNAP);
  int r = snap_mapper.remove_oid(
    soid,
    &_t);
  if (!(r == 0 || r == -ENOENT)) {
    derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
    assert(0);
  }
  snap_mapper.add_oid(
    soid,
    snaps,
    &_t);
}

void PG::merge_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, pg_shard_t from)
{
  PGLogEntryHandler rollbacker;
  
  // merge authority log with ours
  pg_log.merge_log(t, oinfo, olog, from, info, &rollbacker, dirty_info, dirty_big_info);
  
  rollbacker.apply(this, &t);
}

void PG::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead)
{
  PGLogEntryHandler rollbacker;
  pg_log.rewind_divergent_log(
    t, newhead, info, &rollbacker, dirty_info, dirty_big_info);
  rollbacker.apply(this, &t);
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PG::search_for_missing(
  const pg_info_t &oinfo, const pg_missing_t &omissing,
  pg_shard_t from,
  RecoveryCtx *ctx)
{
  unsigned num_unfound_before = missing_loc.num_unfound();

  // add recovery source for missing objects
  bool found_missing = missing_loc.add_source_info(
    from, oinfo, omissing, get_sort_bitwise(), ctx->handle);
  
  if (found_missing && num_unfound_before != missing_loc.num_unfound())
    publish_stats_to_osd();
  
  if (found_missing &&
      (get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, NULL) &
       CEPH_FEATURE_OSD_ERASURE_CODES)) { // ok, find a recovery source for missing objects
    pg_info_t tinfo(oinfo);
    tinfo.pgid.shard = pg_whoami.shard;

    // prepare MOSDPGInfo
    (*(ctx->info_map))[from.osd].push_back(
      make_pair(
	pg_notify_t(
	  from.shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  tinfo),
	past_intervals));
  }
  return found_missing;
}

bool PG::MissingLoc::readable_with_acting(
  const hobject_t &hoid,
  const set<pg_shard_t> &acting) const {
  if (!needs_recovery(hoid)) return true;
  if (!missing_loc.count(hoid)) return false;
  const set<pg_shard_t> &locs = missing_loc.find(hoid)->second;
  dout(10) << __func__ << ": locs:" << locs << dendl;
  set<pg_shard_t> have_acting;
  for (set<pg_shard_t>::const_iterator i = locs.begin();
       i != locs.end();
       ++i) {
    if (acting.count(*i))
      have_acting.insert(*i);
  }
  return (*is_readable)(have_acting);
}

void PG::MissingLoc::add_batch_sources_info(
  const set<pg_shard_t> &sources)
{
  dout(10) << __func__ << ": adding sources in batch " << sources.size() << dendl;
  for (map<hobject_t, pg_missing_t::item, hobject_t::ComparatorWithDefault>::const_iterator i = needs_recovery_map.begin();
      i != needs_recovery_map.end();
      ++i) {
    missing_loc[i->first].insert(sources.begin(), sources.end());
    missing_loc_sources.insert(sources.begin(), sources.end());
  }
}

bool PG::MissingLoc::add_source_info(
  pg_shard_t fromosd,
  const pg_info_t &oinfo,
  const pg_missing_t &omissing,
  bool sort_bitwise,
  ThreadPool::TPHandle* handle)
{
  bool found_missing = false;
  // found items?
  for (map<hobject_t,pg_missing_t::item, hobject_t::ComparatorWithDefault>::const_iterator p = needs_recovery_map.begin();
       p != needs_recovery_map.end();
       ++p) { // iterate each missing object
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (handle) {
      handle->reset_tp_timeout();
    }
    if (oinfo.last_update < need) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (last_update " << oinfo.last_update << " < needed " << need << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_backfill != hobject_t::get_max() &&
	oinfo.last_backfill_bitwise != sort_bitwise) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (last_backfill " << oinfo.last_backfill
	       << " but with wrong sort order)"
	       << dendl;
      continue;
    }
    if (cmp(p->first, oinfo.last_backfill, sort_bitwise) >= 0) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (past last_backfill " << oinfo.last_backfill << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_complete < need) {
      if (omissing.is_missing(soid)) {
	dout(10) << "search_for_missing " << soid << " " << need
		 << " also missing on osd." << fromosd << dendl;
	continue;
      }
    }

    dout(10) << "search_for_missing " << soid << " " << need
	     << " is on osd." << fromosd << dendl;

    // ok, found a recovery source
    missing_loc[soid].insert(fromosd);
    missing_loc_sources.insert(fromosd);
    found_missing = true;
  }

  dout(20) << "needs_recovery_map missing " << needs_recovery_map << dendl;
  return found_missing;
}

// try to query FULLLOG from all shards in PG::might_have_unfound
void PG::discover_all_missing(map<int, map<spg_t,pg_query_t> > &query_map)
{
  const pg_missing_t &missing = pg_log.get_missing();
  assert(have_unfound());

  dout(10) << __func__ << " "
	   << missing.num_missing() << " missing, "
	   << get_num_unfound() << " unfound"
	   << dendl;

  std::set<pg_shard_t>::const_iterator m = might_have_unfound.begin();
  std::set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  
  for (; m != mend; ++m) { // iterate PG::might_have_unfound
    pg_shard_t peer(*m);
    
    if (!get_osdmap()->is_up(peer.osd)) {
      dout(20) << __func__ << " skipping down osd." << peer << dendl;
      continue;
    }

    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne())) {
      // we have a record of this shard, it has nothing to provide, so ignore it
      // ignore empty peers
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_missing_requested" << dendl;
      continue;
    }

    // ok, we have never tried to recover from this peer, now try it

    // Request missing
    dout(10) << __func__ << ": osd." << peer << ": requesting pg_missing_t"
	     << dendl;
    peer_missing_requested.insert(peer);
    query_map[peer.osd][spg_t(info.pgid.pgid, peer.shard)] =
      pg_query_t(
	pg_query_t::FULLLOG,
	peer.shard, pg_whoami.shard,
	info.history, get_osdmap()->get_epoch());
  }
}

/******* PG ***********/
bool PG::needs_recovery() const
{
  assert(is_primary());

  const pg_missing_t &missing = pg_log.get_missing();

  if (missing.num_missing()) { // the primary shard has missing objects
    dout(10) << __func__ << " primary has " << missing.num_missing()
      << " missing" << dendl;
    return true;
  }

  assert(!actingbackfill.empty());
  set<pg_shard_t>::const_iterator end = actingbackfill.end();
  set<pg_shard_t>::const_iterator a = actingbackfill.begin();
  for (; a != end; ++a) { // iterate PG::actingbackfill to check if their have missing objects
    if (*a == get_primary()) continue;
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << " osd." << peer << " doesn't have missing set"
        << dendl;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << " osd." << peer << " has "
        << pm->second.num_missing() << " missing" << dendl;
      return true;
    }
  }

  dout(10) << __func__ << " is recovered" << dendl;
  return false;
}

bool PG::needs_backfill() const
{
  assert(is_primary());

  bool ret = false;

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  set<pg_shard_t>::const_iterator end = backfill_targets.end();
  set<pg_shard_t>::const_iterator a = backfill_targets.begin();
  for (; a != end; ++a) {
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (!pi->second.last_backfill.is_max()) { // ok, there is a shard need to be backfilled
      dout(10) << __func__ << " osd." << peer << " has last_backfill " << pi->second.last_backfill << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " does not need backfill" << dendl;
  return ret;
}

// roughly, we calc past_intervals from info.history.last_epoch_clean to
// info.history.same_interval_since, and during calc, we need to exclude
// those epochs that have been calculated
bool PG::_calc_past_interval_range(epoch_t *start, epoch_t *end, epoch_t oldest_map)
{
  // PG always has a history assicated with it, refer to PG::init
  // h.same_interval_since can only be changed by OSD::build_past_intervals_parallel(load), 
  // OSD::project_pg_history(create) and PG::start_peering_interval(advance osdmap)

  if (info.history.same_interval_since) { // normally, info.history.same_interval_since can only be changed by PG::start_peering_interval
    *end = info.history.same_interval_since;
  } else {
    // PG must be imported, so let's calculate the whole range.
    *end = osdmap_ref->get_epoch();
  }

  // Do we already have the intervals we want?
  map<epoch_t,pg_interval_t>::const_iterator pif = past_intervals.begin();
  if (pif != past_intervals.end()) { // we have built a past_intervals previously
    if (pif->first <= info.history.last_epoch_clean) {
      // our existing past_intervals has covered the interval we want
      dout(10) << __func__ << ": already have past intervals back to "
	       << info.history.last_epoch_clean << dendl;
      return false;
    }

    // still some epochs between info.history.last_epoch_clean to past_intervals.begin()->first 
    // have not been processed
    // TODO: why there is a such weird , whenever we calc past_intervals, we start from the nearest  clean epoch
    *end = past_intervals.begin()->first;
  }

  // the interval start from the epoch when the pg is completely clean
  *start = MAX(MAX(info.history.epoch_created,
		   info.history.last_epoch_clean),
	       oldest_map);
  if (*start >= *end) {
    dout(10) << __func__ << " start epoch " << *start << " >= end epoch " << *end
	     << ", nothing to do" << dendl;
    return false;
  }

  return true;
}

// the PG::past_intervals are generated by OSD::build_past_intervals_parallel,
// PG::generate_past_intervals and PG::start_peering_interval
// trimmed in RecoveryState::Clean::Clean by PG::mark_clean
void PG::generate_past_intervals()
{
  epoch_t cur_epoch, end_epoch;
  // roughly from info.history.last_epoch_clean to info.history.same_interval_since
  if (!_calc_past_interval_range(&cur_epoch, &end_epoch,
      osd->get_superblock().oldest_map)) {
    if (info.history.same_interval_since == 0) // PG must be imported
      info.history.same_interval_since = end_epoch;
    return;
  }

  // ok, still have past_intervals to generate 
  // between:
  // MAX(MAX(info.history.epoch_created, info.history.last_epoch_clean), oldest_map)
  // and:
  // info.history.same_interval_since or past_intervals.begin()->first

  OSDMapRef last_map, cur_map;
  int primary = -1;
  int up_primary = -1;
  vector<int> acting, up, old_acting, old_up;

  cur_map = osd->get_map(cur_epoch); // the nearest epoch that we know the PG must be clean
  cur_map->pg_to_up_acting_osds(
    get_pgid().pgid, &up, &up_primary, &acting, &primary);
  epoch_t same_interval_since = cur_epoch;
  dout(10) << __func__ << " over epochs " << cur_epoch << "-"
	   << end_epoch << dendl;
  ++cur_epoch;
  for (; cur_epoch <= end_epoch; ++cur_epoch) { // to h.same_interval_since
    int old_primary = primary;
    int old_up_primary = up_primary;
    last_map.swap(cur_map);
    old_up.swap(up);
    old_acting.swap(acting);

    cur_map = osd->get_map(cur_epoch);
    pg_t pgid = get_pgid().pgid;
    if (last_map->get_pools().count(pgid.pool()))
      pgid = pgid.get_ancestor(last_map->get_pg_num(pgid.pool())); // we may be a splitted pg
    cur_map->pg_to_up_acting_osds(pgid, &up, &up_primary, &acting, &primary);

    boost::scoped_ptr<IsPGRecoverablePredicate> recoverable(
      get_is_recoverable_predicate()); // for replicated backend, acting size > 0 is recoverable
    std::stringstream debug;

    // check if in cur_map we changed to a new interval, if it is true, we
    // construct an interval and insert into PG::past_intervals
    bool new_interval = pg_interval_t::check_new_interval(
      old_primary,
      primary,
      old_acting,
      acting,
      old_up_primary,
      up_primary,
      old_up,
      up,
      same_interval_since, // a record of the start of our current interval
      info.history.last_epoch_clean, // use to test if we have went rw
      cur_map,
      last_map,
      pgid,
      recoverable.get(),
      &past_intervals, // may insert a new interval
      &debug);
    if (new_interval) {
      dout(10) << debug.str() << dendl;
      same_interval_since = cur_epoch; // used to record the start of the next interval, i.e. prepare for the next interval
    }
  }

  // PG import needs recalculated same_interval_since
  if (info.history.same_interval_since == 0) {
    assert(same_interval_since);
    dout(10) << __func__ << " fix same_interval_since " << same_interval_since << " pg " << *this << dendl;
    dout(10) << __func__ << " past_intervals " << past_intervals << dendl;
    // Fix it
    info.history.same_interval_since = same_interval_since;
  }

  // record our work.
  dirty_info = true;
  dirty_big_info = true;
}

/*
 * Trim past_intervals.
 *
 * This gets rid of all the past_intervals that happened before last_epoch_clean.
 */
void PG::trim_past_intervals()
{
  std::map<epoch_t,pg_interval_t>::iterator pif = past_intervals.begin();
  std::map<epoch_t,pg_interval_t>::iterator end = past_intervals.end();

  while (pif != end) { // in positive order, trim older intervals first
    if (pif->second.last >= info.history.last_epoch_clean)
      return;
    dout(10) << __func__ << ": trimming " << pif->second << dendl;
    past_intervals.erase(pif++);
    dirty_big_info = true;
  }
}


bool PG::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = get_osdmap()->get_up_thru(osd->whoami);

  // PG::need_up_thru was set in PG::build_prior, i.e. when we were to start
  // a new peering process
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    // some other PG(on different peering_wq) on the same osd has sent MOSDAlive
    // to monitor, so we do not have to do the same thing again
    dout(10) << "adjust_need_up_thru now " << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

void PG::remove_down_peer_info(const OSDMapRef osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first.osd)) {
      dout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  // update MissingLoc::missing_loc_sources, MissingLoc::missing_loc,
  // ReplicatedBackend::pull_from_peer, ReplicatedBackend::pulling,
  // ReplicatedPG::peer_log_requested, ReplicatedPG::peer_missing_requested
  check_recovery_sources(osdmap);
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PG::all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const
{
  assert(is_primary());

  // PG::build_might_have_unfound has generated the PG::might_have_unfound for us
  set<pg_shard_t>::const_iterator peer = might_have_unfound.begin();
  set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  
  for (; peer != mend; ++peer) { // iterate PG::might_have_unfound
    if (peer_missing.count(*peer))
      continue;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    if (!osdmap->exists(peer->osd))
      continue;
    
    const osd_info_t &osd_info(osdmap->get_info(peer->osd));
    if (osd_info.lost_at <= osd_info.up_from) { // ok, the osd did not been marked as lost
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  dout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound " << might_have_unfound 
	   << " have been queried or are marked lost" << dendl;
  return true;
}

void PG::build_prior(std::unique_ptr<PriorSet> &prior_set)
{
  if (1) {
    // sanity check
    for (map<pg_shard_t,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 ++it) {
      // peer_info get cleared by PG::clear_primary_state whenever exit state 
      // Primary or start new interval, so if we have old peer_info which means
      // our previous prior set was affected by AdvMap (refer to 
      // prior_set.get()->affected_by_map called in RecoveryState::Peering::react(AdvMap))
                
      // info.history.last_epoch_started only gets updated in 
      // RecoveryState::Active::react(AllReplicasActivated) and PG::append_log
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }

  // generate new prior set
  prior_set.reset(
    new PriorSet( // all parameters are const
      pool.info.ec_pool(),
      get_pgbackend()->get_is_recoverable_predicate(),
      *get_osdmap(), // latest map, i.e. the map we are using for peering
      past_intervals, // may have not reach the latest osdmap, coz in advance_pg we only handle a bunch of maps and then ActMap drove us here
      up,
      acting,
      info,
      this)); // build prior set by iterating past intervals
  PriorSet &prior(*prior_set.get());
				 
  if (prior.pg_down) { // we are blocked by some down osds to survive some intervals
    state_set(PG_STATE_DOWN);
  }


  // multiple PGs are resident on the same osd, so the osd we are on may have
  // been marked alive by some other PG that processes osdmap faster than us,
  // then we have no need to send the MOSDAlive
  // refer to PG::adjust_need_up_thru called in RecoveryState::Peering::react(AdvMap)
  if (get_osdmap()->get_up_thru(osd->whoami) < info.history.same_interval_since) {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " < same_since " << info.history.same_interval_since
	     << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " >= same_since " << info.history.same_interval_since
	     << ", all is well" << dendl;
    need_up_thru = false;
  }

  // set PG::probe_targets, i.e. osds currently are up and we may have data on them
  set_probe_targets(prior_set->probe);
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  stray_purged.clear();
  might_have_unfound.clear();

  last_update_ondisk = eversion_t();

  snap_trimq.clear();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();

  pg_log.reset_recovery_pointers();

  scrubber.reserved_peers.clear();
  scrub_after_recovery = false;

  // ThreadPool::WorkQueue<PG>
  osd->recovery_wq.dequeue(this);

  agent_clear();
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PG::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos) const
{
  /* See doc/dev/osd_internals/last_epoch_started.rst before attempting
   * to make changes to this process.  Also, make sure to update it
   * when you find bugs! */
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;

  // iterate each pg info for all peers (including myself) to find the max 
  // pg_info_t::last_epoch_started
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (!cct->_conf->osd_find_best_info_ignore_history_les && // default is false
	max_last_epoch_started_found < i->second.history.last_epoch_started) {
      min_last_update_acceptable = eversion_t::max();
      max_last_epoch_started_found = i->second.history.last_epoch_started; // from info.history.last_epoch_started
    }
    if (!i->second.is_incomplete() && // not backfilling, i.e. last_backfill == MAX
	max_last_epoch_started_found < i->second.last_epoch_started) {
      min_last_update_acceptable = eversion_t::max();
      max_last_epoch_started_found = i->second.last_epoch_started; // from info.last_epoch_started
    }
  }

  // iterate each pg info for all peers (including myself) to find the min
  // pg_info_t::last_update
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found <= i->second.last_epoch_started) { // max got by applied filter of (!info.is_incomplete)
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
       
  if (min_last_update_acceptable == eversion_t::max())
    return infos.end();

  map<pg_shard_t, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<pg_shard_t, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // disqualify anyone with a too old last_epoch_started
    if (p->second.last_epoch_started < max_last_epoch_started_found)
      continue;
    // Disquality anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) { // set the initial best info
      best = p;
      continue;
    }
    
    // Prefer newer last_update
    if (pool.info.require_rollback()) { // ec pool
      if (p->second.last_update > best->second.last_update)
	continue;
      if (p->second.last_update < best->second.last_update) {
	best = p;
	continue;
      }
    } else { // replicated pool
      if (p->second.last_update < best->second.last_update)
	continue;
      if (p->second.last_update > best->second.last_update) {
	best = p;
	continue;
      }
    }

    // Prefer longer tail
    if (p->second.log_tail > best->second.log_tail) {
      continue;
    } else if (p->second.log_tail < best->second.log_tail) {
      best = p;
      continue;
    }

    // prefer current primary (usually the caller), all things being equal
    if (p->first == pg_whoami) {
      dout(10) << "calc_acting prefer osd." << p->first
	       << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

void PG::calc_ec_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  pg_shard_t acting_primary,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool compat_mode,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  pg_shard_t *want_primary,
  ostream &ss) {
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  unsigned usable = 0;
  for(map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
      i != all_info.end();
      ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (uint8_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], shard_id_t(i)) << std::endl;
      want[i] = up[i];
      ++usable;
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], shard_id_t(i))
	 << " and ";
      backfill->insert(pg_shard_t(up[i], shard_id_t(i)));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], shard_id_t(i)) << std::endl;
      want[i] = acting[i];
      ++usable;
    } else {
      for (set<pg_shard_t>::iterator j = all_info_by_shard[shard_id_t(i)].begin();
	   j != all_info_by_shard[shard_id_t(i)].end();
	   ++j) {
	assert(j->shard == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  ++usable;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << i << std::endl;
    }
  }

  bool found_primary = false;
  for (uint8_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], shard_id_t(i)));
      if (!found_primary) {
	*want_primary = pg_shard_t(want[i], shard_id_t(i));
	found_primary = true;
      }
    }
  }
  acting_backfill->insert(backfill->begin(), backfill->end());
  _want->swap(want);
}

/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
void PG::calc_replicated_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  pg_shard_t acting_primary,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool compat_mode,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  pg_shard_t *want_primary,
  ostream &ss)
{
  ss << "calc_acting newest update on osd." << auth_log_shard->first
     << " with " << auth_log_shard->second << std::endl;
  pg_shard_t auth_log_shard_id = auth_log_shard->first;
  
  // select primary
  map<pg_shard_t,pg_info_t>::const_iterator primary;
  if (up.size() &&
      !all_info.find(up_primary)->second.is_incomplete() &&
      all_info.find(up_primary)->second.last_update >=
        auth_log_shard->second.log_tail) {
    ss << "up_primary: " << up_primary << ") selected as primary" << std::endl;
    primary = all_info.find(up_primary); // prefer up[0], all thing being equal
  } else { // choose a temp primary
    assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << "calc_acting primary is osd." << primary->first
     << " with " << primary->second << std::endl;
  
  *want_primary = primary->first;
  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);
  
  unsigned usable = 1;

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds

  // 1) choose replicas from up set
  for (vector<int>::const_iterator i = up.begin();
       i != up.end();
       ++i) { // iterate each osd of crush calculated up set
    pg_shard_t up_cand = pg_shard_t(*i, shard_id_t::NO_SHARD);
    if (up_cand == primary->first)
      continue;
    
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
      cur_info.last_update < MIN(
	primary->second.log_tail,
	auth_log_shard->second.log_tail)) {
      /* We include auth_log_shard->second.log_tail because in GetLog,
       * we will request logs back to the min last_update over our
       * acting_backfill set, which will result in our log being extended
       * as far backwards as necessary to pick up any peers which can
       * be log recovered by auth_log_shard's log */
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      if (compat_mode) {
	if (backfill->empty()) {
	  backfill->insert(up_cand);
	  want->push_back(*i);
	  acting_backfill->insert(up_cand);
	}
      } else {
	backfill->insert(up_cand);
	acting_backfill->insert(up_cand);
      }
    } else {
      want->push_back(*i);
      acting_backfill->insert(up_cand);
      usable++;
      ss << " osd." << *i << " (up) accepted " << cur_info << std::endl;
    }
  }

  // 2) choose replicas from acting set
  // This no longer has backfill OSDs, but they are covered above.
  for (vector<int>::const_iterator i = acting.begin();
       i != acting.end();
       ++i) {
    pg_shard_t acting_cand(*i, shard_id_t::NO_SHARD);
    if (usable >= size) // already got enough candidates, i.e. >= pool size required
      break;

    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), acting_cand.osd);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < primary->second.log_tail) {
      ss << " shard " << acting_cand << " (stray) REJECTED "
	       << cur_info << std::endl;
    } else {
      want->push_back(*i);
      acting_backfill->insert(acting_cand);
      ss << " shard " << acting_cand << " (stray) accepted "
	 << cur_info << std::endl;
      usable++;
    }
  }

  // 3) choose replicas from any peer_info osds
  for (map<pg_shard_t,pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (usable >= size)
      break;

    // skip up osds we already considered above
    if (i->first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i->first.osd);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(
      acting.begin(), acting.end(), i->first.osd);
    if (acting_it != acting.end())
      continue;

    if (i->second.is_incomplete() ||
	i->second.last_update < primary->second.log_tail) {
      ss << " shard " << i->first << " (stray) REJECTED "
	 << i->second << std::endl;
    } else {
      want->push_back(i->first.osd);
      acting_backfill->insert(i->first);
      ss << " shard " << i->first << " (stray) accepted "
	 << i->second << std::endl;
      usable++;
    }
  }
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 */
bool PG::choose_acting(pg_shard_t &auth_log_shard_id)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end()); // all pg info from peers (not including myself)
  all_info[pg_whoami] = info; // ok, add myself

  for (map<pg_shard_t, pg_info_t>::iterator p = all_info.begin();
       p != all_info.end();
       ++p) {
    dout(10) << "calc_acting osd." << p->first << " " << p->second << dendl;
  }

  // ok, first we need to find the best pg info
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard = find_best_info(all_info);

  if (auth_log_shard == all_info.end()) { // no valid pg info found
    if (up != acting) {
      dout(10) << "choose_acting no suitable info found (incomplete backfills?),"
	       << " reverting to up" << dendl;
      want_acting = up;
      
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid.pgid, empty); // clear OSD::pg_temp_wanted for this pg
    } else {
      dout(10) << "choose_acting failed" << dendl;
      assert(want_acting.empty());
    }
    
    return false;
  }

  // recalc best pg info
  if ((up.size() &&
      !all_info.find(up_primary)->second.is_incomplete() &&
      all_info.find(up_primary)->second.last_update >=
       auth_log_shard->second.log_tail) &&
      auth_log_shard->second.is_incomplete()) {
    map<pg_shard_t, pg_info_t> complete_infos;
    for (map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
	 i != all_info.end();
	 ++i) {
      if (!i->second.is_incomplete())
	complete_infos.insert(*i);
    }
    map<pg_shard_t, pg_info_t>::const_iterator i = find_best_info(
      complete_infos);
    if (i != complete_infos.end()) {
      auth_log_shard = all_info.find(i->first);
    }
  }

  auth_log_shard_id = auth_log_shard->first;

  // Determine if compatibility needed
  bool compat_mode = !cct->_conf->osd_debug_override_acting_compat; // default is false
  if (compat_mode) {
    bool all_support = true;
    OSDMapRef osdmap = get_osdmap();

    for (map<pg_shard_t, pg_info_t>::iterator it = all_info.begin();
	 it != all_info.end();
	 ++it) {
      pg_shard_t peer = it->first;

      const osd_xinfo_t& xi = osdmap->get_xinfo(peer.osd);
      if (!(xi.features & CEPH_FEATURE_OSD_ERASURE_CODES)) {
	all_support = false;
	break;
      }
    }
    if (all_support) // support all features, compatibility not needed
      compat_mode = false;
  }

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  pg_shard_t want_primary;
  stringstream ss;
  if (!pool.info.ec_pool()) // replicated pool
    calc_replicated_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid), // pool size
      acting,
      primary,
      up,
      up_primary,
      all_info,
      compat_mode,
      &want, // want_acting
      &want_backfill, // backfill_targets
      &want_acting_backfill, // actingbackfill
      &want_primary,
      ss);
  else // ec pool
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      primary,
      up,
      up_primary,
      all_info,
      compat_mode,
      &want,
      &want_backfill,
      &want_acting_backfill,
      &want_primary,
      ss);
  dout(10) << ss.str() << dendl;

  unsigned num_want_acting = 0;
  for (vector<int>::iterator i = want.begin();
       i != want.end();
       ++i) {
    if (*i != CRUSH_ITEM_NONE)
      ++num_want_acting;
  }

  // We go incomplete if below min_size for ec_pools since backfill
  // does not currently maintain rollbackability
  // Otherwise, we will go "peered", but not "active"
  if (num_want_acting < pool.info.min_size &&
      (pool.info.ec_pool() ||
       (!(get_min_peer_features() & CEPH_FEATURE_OSD_MIN_SIZE_RECOVERY)) ||
       !cct->_conf->osd_allow_recovery_below_min_size)) { // default is true
    want_acting.clear();
    dout(10) << "choose_acting failed, below min size" << dendl;
    
    return false;
  }

  /* Check whether we have enough acting shards to later perform recovery */
  boost::scoped_ptr<IsPGRecoverablePredicate> recoverable_predicate(
    get_pgbackend()->get_is_recoverable_predicate());
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE)
      have.insert(
	pg_shard_t(
	  want[i],
	  pool.info.ec_pool() ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  if (!(*recoverable_predicate)(have)) {
    want_acting.clear();
    dout(10) << "choose_acting failed, not recoverable" << dendl;
    return false;
  }

  if (want != acting) { // our want_acting set is not the same as current acting set
    dout(10) << "choose_acting want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;

    want_acting = want; // set want_acting set

    if (want_acting == up) {
      // There can't be any pending backfill if
      // want is the same as crush map up OSDs.
      assert(compat_mode || want_backfill.empty());
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid.pgid, empty); // clear OSDService::pg_temp_wanted
    } else // wanted acting set != up set, request mon to set pg_temp
      osd->queue_want_pg_temp(info.pgid.pgid, want);
    
    return false;
  }

  // ok, want_acting set is the same as current acting set, no need to request
  // mon to set pg_temp

  // if we need to request mon to set pg_temp, then we have called 
  // OSDService::queue_want_pg_temp to queue this request on OSDService::pg_temp_wanted
  want_acting.clear();
  
  actingbackfill = want_acting_backfill;
  dout(10) << "actingbackfill is " << actingbackfill << dendl;
  
  assert(backfill_targets.empty() || backfill_targets == want_backfill);
  
  if (backfill_targets.empty()) { // backfill_targets is empty, set it
    // Caller is GetInfo
    backfill_targets = want_backfill;
    for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	 i != backfill_targets.end();
	 ++i) {
      assert(!stray_set.count(*i));
    }
  } else { // not empty, asserts it has not changed
    // Will not change if already set because up would have had to change
    assert(backfill_targets == want_backfill);
    
    // Verify that nothing in backfill is in stray_set
    for (set<pg_shard_t>::iterator i = want_backfill.begin();
	 i != want_backfill.end();
	 ++i) {
      assert(stray_set.find(*i) == stray_set.end());
    }
  }
  dout(10) << "choose_acting want " << want << " (== acting) backfill_targets " 
	   << want_backfill << dendl;
  return true;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PG::build_might_have_unfound()
{
  assert(might_have_unfound.empty());
  assert(is_primary());

  dout(10) << __func__ << dendl;

  // Make sure that we have past intervals.
  generate_past_intervals();

  // We need to decide who might have unfound objects that we need
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator end = past_intervals.rend();
  for (; p != end; ++p) {
    const pg_interval_t &interval(p->second);
    // We already have all the objects that exist at last_epoch_clean,
    // so there's no need to look at earlier intervals.
    if (interval.last < info.history.last_epoch_clean)
      break;

    // If nothing changed, we don't care about this interval.
    if (!interval.maybe_went_rw)
      continue;

    int i = 0;
    std::vector<int>::const_iterator a = interval.acting.begin();
    std::vector<int>::const_iterator a_end = interval.acting.end();
    for (; a != a_end; ++a, ++i) {
      pg_shard_t shard(*a, pool.info.ec_pool() ? shard_id_t(i) : shard_id_t::NO_SHARD);
      if (*a != CRUSH_ITEM_NONE && shard != pg_whoami) // this shard may include objects we need
	might_have_unfound.insert(shard);
    }
  }

  // include any (stray) peers
  for (map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       ++p)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

struct C_PG_ActivateCommitted : public Context {
  PGRef pg;
  epoch_t epoch;
  epoch_t activation_epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e, epoch_t ae)
    : pg(p), epoch(e), activation_epoch(ae) {}
  void finish(int r) {
    pg->_activate_committed(epoch, activation_epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t activation_epoch,
		  list<Context*>& tfin,
		  map<int, map<spg_t,pg_query_t> >& query_map,
		  map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map,
                  RecoveryCtx *ctx)
{
  assert(!is_peered());
  assert(scrubber.callbacks.empty());
  assert(callbacks_for_degraded_object.empty());

  // -- crash recovery?
  if (acting.size() >= pool.info.min_size &&
      is_primary() &&
      pool.info.crash_replay_interval > 0 &&
      may_need_replay(get_osdmap())) {
    replay_until = ceph_clock_now(cct);
    replay_until += pool.info.crash_replay_interval;
    dout(10) << "activate starting replay interval for " << pool.info.crash_replay_interval
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);

    // TODOSAM: osd->osd-> is no good
    osd->osd->replay_queue_lock.Lock();
    // push this pg on OSDService::replay_queue, OSD::tick will check the replay_queue
    // periodically
    osd->osd->replay_queue.push_back(pair<spg_t,utime_t>(
	info.pgid, replay_until));
    osd->osd->replay_queue_lock.Unlock();
  }

  // twiddle pg state
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  if (is_primary()) {
    // only update primary last_epoch_started if we will go active
    if (acting.size() >= pool.info.min_size) {
      assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	     info.last_epoch_started <= activation_epoch);
      info.last_epoch_started = activation_epoch;
    }
  } else if (is_acting(pg_whoami)) {
    /* update last_epoch_started on acting replica to whatever the primary sent
     * unless it's smaller (could happen if we are going peered rather than
     * active, see doc/dev/osd_internals/last_epoch_started.rst) */
    if (info.last_epoch_started < activation_epoch)
      info.last_epoch_started = activation_epoch;
  }

  const pg_missing_t &missing = pg_log.get_missing();

  if (is_primary()) {
    last_update_ondisk = info.last_update;
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }
  last_update_applied = info.last_update;
  last_rollback_info_trimmed_to_applied = pg_log.get_rollback_trimmed_to();

  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_big_info = true; // maybe

  t.register_on_complete(
    // will call PG::_activate_committed when the pg info committed, if primary
    // and replicas have committed their pg info, we queue an AllReplicasActivated
    // peering evt and let the recovery_state machine to handle
    new C_PG_ActivateCommitted(
      this,
      get_osdmap()->get_epoch(),
      activation_epoch));
  
  // initialize snap_trimq
  if (is_primary()) {
    dout(20) << "activate - purged_snaps " << info.purged_snaps
	     << " cached_removed_snaps " << pool.cached_removed_snaps << dendl;
    snap_trimq = pool.cached_removed_snaps;
    snap_trimq.subtract(info.purged_snaps);
    dout(10) << "activate - snap_trimq " << snap_trimq << dendl;
    if (!snap_trimq.empty() && is_clean())
      queue_snap_trim();
  }

  // init complete pointer
  if (missing.num_missing() == 0) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    pg_log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    pg_log.activate_not_complete(info);
  }
    
  log_weirdness();

  // ok, following dirty works are to be done by the primary

  // if primary..
  if (is_primary()) {
    assert(ctx);
    // start up replicas

    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) { // iterate each acting shard of this pg
      if (*i == pg_whoami) continue; // exclude ourself, i.e. the primary shard
      
      pg_shard_t peer = *i;
      assert(peer_info.count(peer));
      pg_info_t& pi = peer_info[peer];

      dout(10) << "activate peer osd." << peer << " " << pi << dendl;

      MOSDPGLog *m = 0;
      pg_missing_t& pm = peer_missing[peer];

      bool needs_past_intervals = pi.dne();

      /*
       * cover case where peer sort order was different and
       * last_backfill cannot be interpreted
       */
      bool force_restart_backfill =
	!pi.last_backfill.is_max() &&
	pi.last_backfill_bitwise != get_sort_bitwise();

      if (pi.last_update == info.last_update && !force_restart_backfill) {
        // empty log
	if (!pi.last_backfill.is_max())
	  osd->clog->info() << info.pgid << " continuing backfill to osd."
			    << peer
			    << " from (" << pi.log_tail << "," << pi.last_update
			    << "] " << pi.last_backfill
			    << " to " << info.last_update;

        // RecoveryState::ReplicaActive::react(Activate) will call us with 
        // activator_map (i.e. info_map :)) setting to NULL
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd." << peer << " is up to date, queueing in pending_activators" << dendl;

          // prepare MOSDPGInfo
	  (*activator_map)[peer.osd].push_back(
	    make_pair(
	      pg_notify_t(
		peer.shard, pg_whoami.shard,
		get_osdmap()->get_epoch(),
		get_osdmap()->get_epoch(),
		info),
	      past_intervals));
	} else {
	  dout(10) << "activate peer osd." << peer << " is up to date, but sending pg_log anyway" << dendl;

          // prepare MOSDPGLog
	  m = new MOSDPGLog(
	    i->shard, pg_whoami.shard,
	    get_osdmap()->get_epoch(), info);
	}
      } else if (
	pg_log.get_tail() > pi.last_update ||
	pi.last_backfill == hobject_t() ||
	force_restart_backfill ||
	(backfill_targets.count(*i) && pi.last_backfill.is_max())) {
	/* ^ This last case covers a situation where a replica is not contiguous
	 * with the auth_log, but is contiguous with this replica.  Reshuffling
	 * the active set to handle this would be tricky, so instead we just go
	 * ahead and backfill it anyway.  This is probably preferrable in any
	 * case since the replica in question would have to be significantly
	 * behind.
	 */
	// backfill
	osd->clog->info() << info.pgid << " starting backfill to osd." << peer
			 << " from (" << pi.log_tail << "," << pi.last_update
			  << "] " << pi.last_backfill
			 << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.set_last_backfill(hobject_t(), get_sort_bitwise());
	pi.last_epoch_started = info.last_epoch_started;
	pi.history = info.history;
	pi.hit_set = info.hit_set;
	pi.stats.stats.clear();

	// initialize peer with our purged_snaps.
	pi.purged_snaps = info.purged_snaps;

	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(), pi);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(pg_log.get_log(), cct->_conf->osd_min_pg_log_entries); // default is 3000
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	assert(pg_log.get_tail() <= pi.last_update);
	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(), info);
	// send new stuff to append to replicas log
	m->log.copy_after(pg_log.get_log(), pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (needs_past_intervals)
	m->past_intervals = past_intervals; // in MOSDPGLog we may not carry the past_intervals

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (list<pg_log_entry_t>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             ++p)
	  if (cmp(p->soid, pi.last_backfill, get_sort_bitwise()) <= 0)
	    pm.add_next_event(*p);
      }

      // send MOSDPGLog, the MOSDPGInfo will be sent outside of the recovery_state 
      // handling
      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log << dendl;
	//m->log.print(cout);
	osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
      }

      // peer now has 
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd." << peer << " " << pi << " uptodate" << dendl;
      } else {
        dout(10) << "activate peer osd." << peer << " " << pi << " missing " << pm << dendl;
      }
    }

    // Set up missing_loc
    set<pg_shard_t> complete_shards;
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) {
        // register missing objects of primary shard in PG::missing_loc
	missing_loc.add_active_missing(missing);
        
        if (!missing.have_missing())
          complete_shards.insert(*i);
      } else {
	assert(peer_missing.count(*i)); // refer to RecoveryState::GetMissing::GetMissing

        // register missing objects of replica shard in PG::missing_loc
	missing_loc.add_active_missing(peer_missing[*i]);
        
        if (!peer_missing[*i].have_missing() && peer_info[*i].last_backfill == hobject_t::get_max())
          complete_shards.insert(*i);
      }
    }
         
    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();

    // ok, if we need objects to recover, then update recovery source for
    // each object
    
    if (needs_recovery()) { // has missing objects for any shard of this pg
      // If only one shard has missing, we do a trick to add all others as recovery
      // source, this is considered safe since the PGLogs have been merged locally,
      // and covers vast majority of the use cases, like one OSD/host is down for
      // a while for hardware repairing
      if (complete_shards.size() + 1 == actingbackfill.size()) {
        // add recovery source for missing objects
        missing_loc.add_batch_sources_info(complete_shards);
      } else {
        // add primary shard as the recovery source for missing objects
        missing_loc.add_source_info(pg_whoami, info, pg_log.get_missing(),
				    get_sort_bitwise(), ctx->handle);
        for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	     i != actingbackfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue; // already added above
	  dout(10) << __func__ << ": adding " << *i << " as a source" << dendl;
	  assert(peer_missing.count(*i));
	  assert(peer_info.count(*i));

          // add replica shard as the recovery source for missing objects
	  missing_loc.add_source_info(
	    *i, // check if we can recover from this shard
	    peer_info[*i],
	    peer_missing[*i],
	    get_sort_bitwise(),
            ctx->handle);
        }
      }
      
      for (map<pg_shard_t, pg_missing_t>::iterator i = peer_missing.begin();
	   i != peer_missing.end();
	   ++i) {
        // if this shard is in PG::actingbackfill, then it has been added to
        // the recovery source above
	if (is_actingbackfill(i->first))
	  continue;
	assert(peer_info.count(i->first));

        // try to add recovery source
	search_for_missing(
	  peer_info[i->first],
	  i->second,
	  i->first,
	  ctx);
      }

      // build PG::might_have_unfound from past_intervals and stray peers
      build_might_have_unfound();

      state_set(PG_STATE_DEGRADED);
      dout(10) << "activate - starting recovery" << dendl;

      // queue this pg on OSDService::recovery_wq
      osd->queue_for_recovery(this);
      
      if (have_unfound()) // have unfound objects, i.e. with existing shards that cannot be recovered objects
        // iterate each shard in PG::might_have_unfound to request FULLLOG
	discover_all_missing(query_map);
    }

    // degraded?
    if (get_osdmap()->get_pg_size(info.pgid.pgid) > actingset.size()) {
      state_set(PG_STATE_DEGRADED);
      state_set(PG_STATE_UNDERSIZED);
    }

    state_set(PG_STATE_ACTIVATING);
  }
}

bool PG::op_has_sufficient_caps(OpRequestRef& op)
{
  // only check MOSDOp
  if (op->get_req()->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->get_req());

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  string key = req->get_object_locator().key;
  if (key.length() == 0)
    key = req->get_oid().name;

  bool cap = caps.is_capable(pool.name, req->get_object_locator().nspace,
                             pool.auid, key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps pool=" << pool.id << " (" << pool.name
		   << " " << req->get_object_locator().nspace
	   << ") owner=" << pool.auid
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::take_op_map_waiters()
{
  Mutex::Locker l(map_lock);
  for (list<OpRequestRef>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       ) {
    if (op_must_wait_for_map(get_osdmap_with_maplock()->get_epoch(), *i)) {
      break;
    } else {
      osd->op_wq.queue(make_pair(PGRef(this), *i));
      waiting_for_map.erase(i++);
    }
  }
}

void PG::queue_op(OpRequestRef& op)
{
  Mutex::Locker l(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    op->mark_delayed("waiting_for_map not empty");
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock()->get_epoch(), op)) {
    waiting_for_map.push_back(op);
    op->mark_delayed("op must wait for map");
    return;
  }
  op->mark_queued_for_pg();
  osd->op_wq.queue(make_pair(PGRef(this), op));
  {
    // after queue() to include any locking costs
#ifdef WITH_LTTNG
    osd_reqid_t reqid = op->get_reqid();
#endif
    tracepoint(pg, queue_op, reqid.name._type,
        reqid.name._num, reqid.tid, reqid.inc, op->rmw_flags);
  }
}

void PG::replay_queued_ops()
{
  assert(is_replay());
  assert(is_active() || is_activating());
  eversion_t c = info.last_update;
  list<OpRequestRef> replay;
  dout(10) << "replay_queued_ops" << dendl;
  state_clear(PG_STATE_REPLAY);

  for (map<eversion_t,OpRequestRef>::iterator p = replay_queue.begin();
       p != replay_queue.end();
       ++p) { // iterate each op
    if (p->first.version != c.version+1) {
      dout(10) << "activate replay " << p->first
	       << " skipping " << c.version+1 - p->first.version 
	       << " ops"
	       << dendl;      
      c = p->first;
    }
    dout(10) << "activate replay " << p->first << " "
             << *p->second->get_req() << dendl;
    replay.push_back(p->second); // need to replay
  }
  replay_queue.clear();
  
  if (is_active()) {
    requeue_ops(replay); // queue on OSDService::op_wq
    requeue_ops(waiting_for_active); // TODO: requeue this list before replay ???
    assert(waiting_for_peered.empty());
  } else {
    // splice with previous pending ops
    waiting_for_active.splice(waiting_for_active.begin(), replay);
  }

  publish_stats_to_osd();
}

void PG::_activate_committed(epoch_t epoch, epoch_t activation_epoch)
{
  lock();
  if (pg_has_reset_since(epoch)) { // obsolete activation
    dout(10) << "_activate_committed " << epoch
	     << ", that was an old interval" << dendl;
  } else if (is_primary()) { // we are primary shard
    peer_activated.insert(pg_whoami);
    dout(10) << "_activate_committed " << epoch
	     << " peer_activated now " << peer_activated 
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_interval_since " << info.history.same_interval_since << dendl;
    assert(!actingbackfill.empty());
    if (peer_activated.size() == actingbackfill.size()) // all peers activated
      // queue a peering evt AllReplicasActivated, the peering evt will be
      // handled by RecoveryState::Active::react(AllReplicasActivated) or
      // RecoveryState::Recovered::react(AllReplicasActivated)
      all_activated_and_committed();
  } else { // we are replica shard
    dout(10) << "_activate_committed " << epoch << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(epoch);
    pg_notify_t i = pg_notify_t(
      get_primary().shard, pg_whoami.shard,
      get_osdmap()->get_epoch(),
      get_osdmap()->get_epoch(),
      info);

    // ok, we are not primary PG, set the h.last_epoch_started directly
    i.info.history.last_epoch_started = activation_epoch;
    if (acting.size() >= pool.info.min_size) {
      state_set(PG_STATE_ACTIVE);
    } else {
      state_set(PG_STATE_PEERED);
    }

    // tell the primary we have committed our pg info updates
    m->pg_list.push_back(make_pair(i, pg_interval_map_t()));
    osd->send_message_osd_cluster(get_primary().osd, m, get_osdmap()->get_epoch());

    // waiters
    if (flushes_in_progress == 0) {
      requeue_ops(waiting_for_peered);
    }
  }

  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }

  unlock();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PG::all_activated_and_committed()
{
  dout(10) << "all_activated_and_committed" << dendl;
  assert(is_primary());
  assert(peer_activated.size() == actingbackfill.size());
  assert(!actingbackfill.empty());
  assert(blocked_by.empty());

  queue_peering_event(
    CephPeeringEvtRef(
      new CephPeeringEvt(
        get_osdmap()->get_epoch(),
        get_osdmap()->get_epoch(),
        AllReplicasActivated())));
}

void PG::queue_snap_trim()
{
  if (snap_trim_queued) {
    dout(10) << "queue_snap_trim -- already queued" << dendl;
  } else {
    dout(10) << "queue_snap_trim -- queuing" << dendl;
    snap_trim_queued = true;
    osd->queue_for_snap_trim(this); // queue on OSDService::op_wq
  }
}

bool PG::requeue_scrub()
{
  assert(is_locked());
  if (scrub_queued) {
    dout(10) << __func__ << ": already queued" << dendl;
    return false;
  } else {
    dout(10) << __func__ << ": queueing" << dendl;
    scrub_queued = true;
    osd->queue_for_scrub(this);
    return true;
  }
}

bool PG::queue_scrub()
{
  assert(_lock.is_locked());
  if (is_scrubbing()) {
    return false;
  }
  scrubber.must_scrub = false;
  state_set(PG_STATE_SCRUBBING);
  if (scrubber.must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
    scrubber.must_deep_scrub = false;
  }
  if (scrubber.must_repair || scrubber.auto_repair) {
    state_set(PG_STATE_REPAIR);
    scrubber.must_repair = false;
  }
  requeue_scrub();
  return true;
}

struct C_PG_FinishRecovery : public Context {
  PGRef pg;
  C_PG_FinishRecovery(PG *p) : pg(p) {}
  void finish(int r) {
    pg->_finish_recovery(this);
  }
};

void PG::mark_clean()
{
  // only mark CLEAN if we have the desired number of replicas AND we
  // are not remapped.
  if (actingset.size() == get_osdmap()->get_pg_size(info.pgid.pgid) &&
      up == acting)
    state_set(PG_STATE_CLEAN);

  // NOTE: this is actually a bit premature: we haven't purged the
  // strays yet.
  info.history.last_epoch_clean = get_osdmap()->get_epoch();

  // trim past_intervals up to info.history.last_epoch_clean
  trim_past_intervals();

  if (is_clean() && !snap_trimq.empty())
    queue_snap_trim();

  dirty_info = true;
}

unsigned PG::get_recovery_priority()
{
  // a higher value -> a higher priority
  return OSD_RECOVERY_PRIORITY_MAX;
}

unsigned PG::get_backfill_priority()
{
  // a higher value -> a higher priority

  // undersized: 200 + num missing replicas
  if (is_undersized()) {
    assert(pool.info.size > actingset.size());
    return 200 + (pool.info.size - actingset.size());
  }

  // degraded: baseline degraded
  if (is_degraded()) {
    return 200;
  }

  // baseline
  return 1;
}

void PG::finish_recovery(list<Context*>& tfin)
{
  dout(10) << "finish_recovery" << dendl;
  assert(info.last_complete == info.last_update);

  // clear recovering and backfilling intermediate states
  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  tfin.push_back(finish_sync_event);
}

void PG::_finish_recovery(Context *c)
{
  lock();
  
  if (deleting) {
    unlock();
    return;
  }

  // PG::finish_sync_event may be cleared by PG::clear_recovery_state or 
  // PG::clear_recovery_state, we need a method to detect the stale context
  if (c == finish_sync_event) {
    dout(10) << "_finish_recovery" << dendl;
    finish_sync_event = 0;

    // iterate PG::stray_set to send MOSDPGRemove to purge strays
    purge_strays();

    publish_stats_to_osd();

    if (scrub_after_recovery) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      scrub_after_recovery = false;
      scrubber.must_deep_scrub = true;
      queue_scrub();
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  
  unlock();
}

void PG::start_recovery_op(const hobject_t& soid)
{
  dout(10) << "start_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")"
#endif
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++; // inc PG::recovery_ops_active by one
  
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid) == 0);
  recovering_oids.insert(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->start_recovery_op(this, soid); // inc OSD::recovery_ops_active by one
}

void PG::finish_recovery_op(const hobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")" 
#endif
	   << dendl;
  assert(recovery_ops_active > 0);
  recovery_ops_active--;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid));
  recovering_oids.erase(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->finish_recovery_op(this, soid, dequeue);
}

static void split_replay_queue(
  map<eversion_t, OpRequestRef> *from,
  map<eversion_t, OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (map<eversion_t, OpRequestRef>::iterator i = from->begin();
       i != from->end();
       ) {
    if (OSD::split_request(i->second, match, bits)) {
      to->insert(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

void PG::split_ops(PG *child, unsigned split_bits) {
  unsigned match = child->info.pgid.ps();
  assert(waiting_for_all_missing.empty());
  assert(waiting_for_cache_not_full.empty());
  assert(waiting_for_unreadable_object.empty());
  assert(waiting_for_degraded_object.empty());
  assert(waiting_for_ack.empty());
  assert(waiting_for_ondisk.empty());
  assert(waiting_for_active.empty());
  split_replay_queue(&replay_queue, &(child->replay_queue), match, split_bits);

  snap_trim_queued = false;
  // dequeue pg from OSD::op_shardedwq and get a list of OpRequestRef that are
  // pending for handling (push back of PG::waiting_for_peered)
  osd->dequeue_pg(this, &waiting_for_peered);

  OSD::split_list(
    &waiting_for_peered, &(child->waiting_for_peered), match, split_bits);
  {
    Mutex::Locker l(map_lock); // to avoid a race with the osd dispatch
    OSD::split_list(
      &waiting_for_map, &(child->waiting_for_map), match, split_bits);
  }
}

void PG::split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  child->update_snap_mapper_bits(split_bits);
  child->update_osdmap_ref(get_osdmap());

  child->pool = pool;

  // Log
  pg_log.split_into(child_pgid, split_bits, &(child->pg_log));
  child->info.last_complete = info.last_complete;

  info.last_update = pg_log.get_head();
  child->info.last_update = child->pg_log.get_head();

  child->info.last_user_version = info.last_user_version;

  info.log_tail = pg_log.get_tail();
  child->info.log_tail = child->pg_log.get_tail();

  if (info.last_complete < pg_log.get_tail())
    info.last_complete = pg_log.get_tail();
  if (child->info.last_complete < child->pg_log.get_tail())
    child->info.last_complete = child->pg_log.get_tail();

  // Info
  child->info.history = info.history;
  child->info.purged_snaps = info.purged_snaps;
  child->info.set_last_backfill(info.last_backfill, info.last_backfill_bitwise);

  child->info.stats = info.stats;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.last_epoch_started = info.last_epoch_started;

  child->snap_trimq = snap_trimq;

  // There can't be recovery/backfill going on now
  int primary, up_primary;
  vector<int> newup, newacting;
  get_osdmap()->pg_to_up_acting_osds(
    child->info.pgid.pgid, &newup, &up_primary, &newacting, &primary);
  child->init_primary_up_acting(
    newup,
    newacting,
    up_primary,
    primary);
  child->role = OSDMap::calc_pg_role(osd->whoami, child->acting);

  // this comparison includes primary rank via pg_shard_t
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap()->get_epoch();

  // History
  child->past_intervals = past_intervals;

  split_ops(child, split_bits);
  _split_into(child_pgid, child, split_bits);

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PG::clear_recovery_state() 
{
  dout(10) << "clear_recovery_state" << dendl;

  // set complete_to = log.end() and last_requested = 0
  pg_log.reset_recovery_pointers();
  
  finish_sync_event = 0;

  hobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    // PG::recovery_ops_active--, OSDService::recovery_ops_active-- and
    // dequeue us from OSDService::recovery_wq
    finish_recovery_op(soid, true);
  }

  backfill_targets.clear(); // set in PG::choose_acting
  backfill_info.clear();
  peer_backfill_info.clear();
  waiting_on_backfill.clear();

  // clear missing_loc, backfills_in_flight, recovering, pending_backfill_updates
  // for ReplicatedPG, and clear pushing/pulling maps for ReplicatedBackend
  _clear_recovery_state();  // pg impl specific hook
}

void PG::cancel_recovery()
{
  dout(10) << "cancel_recovery" << dendl;
  clear_recovery_state();
}


void PG::purge_strays()
{
  dout(10) << "purge_strays " << stray_set << dendl;
  
  bool removed = false;
  for (set<pg_shard_t>::iterator p = stray_set.begin();
       p != stray_set.end();
       ++p) {
    assert(!is_actingbackfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap()->get_epoch(),
	to_remove);
      
      osd->send_message_osd_cluster(p->osd, m, get_osdmap()->get_epoch());
      stray_purged.insert(*p);
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_missing.erase(*p);
    peer_info.erase(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PG::set_probe_targets(const set<pg_shard_t> &probe_set)
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
  for (set<pg_shard_t>::iterator i = probe_set.begin();
       i != probe_set.end();
       ++i) {
    probe_targets.insert(i->osd);
  }
}

void PG::clear_probe_targets()
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers()
{
  assert(is_locked());

  set<int> new_peers;
  if (is_primary()) {
    for (unsigned i=0; i<acting.size(); i++) {
      if (acting[i] != CRUSH_ITEM_NONE)
	new_peers.insert(acting[i]);
    }
    for (unsigned i=0; i<up.size(); i++) {
      if (up[i] != CRUSH_ITEM_NONE)
	new_peers.insert(up[i]);
    }
    for (map<pg_shard_t,pg_info_t>::iterator p = peer_info.begin();
	 p != peer_info.end();
	 ++p)
      new_peers.insert(p->first.osd);
  }

  bool need_update = false;
  heartbeat_peer_lock.Lock();
  if (new_peers == heartbeat_peers) {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " unchanged" << dendl;
  } else {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " -> " << new_peers << dendl;
    heartbeat_peers.swap(new_peers);
    need_update = true;
  }
  heartbeat_peer_lock.Unlock();

  if (need_update)
    osd->need_heartbeat_peer_update();
}

void PG::_update_calc_stats()
{
  info.stats.version = info.last_update;
  info.stats.created = info.history.epoch_created;
  info.stats.last_scrub = info.history.last_scrub;
  info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
  info.stats.last_deep_scrub = info.history.last_deep_scrub;
  info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
  info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
  info.stats.last_epoch_clean = info.history.last_epoch_clean;

  info.stats.log_size = pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.ondisk_log_size =
    pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.log_start = pg_log.get_tail();
  info.stats.ondisk_log_start = pg_log.get_tail();

  // calc copies, degraded
  unsigned target = get_osdmap()->get_pg_size(info.pgid.pgid);
  info.stats.stats.calc_copies(MAX(target, actingbackfill.size()));
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;
  if ((is_degraded() || is_undersized() || !is_clean()) && is_peered()) {
    // NOTE: we only generate copies, degraded, unfound values for
    // the summation, not individual stat categories.
    uint64_t num_objects = info.stats.stats.sum.num_objects;

    // a degraded objects has fewer replicas or EC shards than the
    // pool specifies
    int64_t degraded = 0;

    // if acting is smaller than desired, add in those missing replicas
    if (actingset.size() < target)
      degraded += (target - actingset.size()) * num_objects;

    // missing on primary
    info.stats.stats.sum.num_objects_missing_on_primary =
      pg_log.get_missing().num_missing();
    degraded += pg_log.get_missing().num_missing();

    assert(!acting.empty());
    for (set<pg_shard_t>::iterator i = actingset.begin();
	 i != actingset.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      assert(peer_missing.count(*i));

      // in missing set
      degraded += peer_missing[*i].num_missing();

      // not yet backfilled
      int64_t diff = num_objects - peer_info[*i].stats.stats.sum.num_objects;
      if (diff > 0)
        degraded += diff;
    }
    info.stats.stats.sum.num_objects_degraded = degraded;
    info.stats.stats.sum.num_objects_unfound = get_num_unfound();

    // a misplaced object is not stored on the correct OSD
    uint64_t misplaced = 0;
    unsigned i = 0;
    unsigned in_place = 0;
    for (vector<int>::iterator p = up.begin(); p != up.end(); ++p, ++i) {
      pg_shard_t s(*p,
		   pool.info.ec_pool() ? shard_id_t(i) : shard_id_t::NO_SHARD);
      if (actingset.count(s)) {
	++in_place;
      } else {
	// not where it should be
	misplaced += num_objects;
	if (actingbackfill.count(s)) {
	  // ...but partially backfilled
	  misplaced -= peer_info[s].stats.stats.sum.num_objects;
	  dout(20) << __func__ << " osd." << *p << " misplaced "
		   << num_objects << " but partially backfilled "
		   << peer_info[s].stats.stats.sum.num_objects
		   << dendl;
	} else {
	  dout(20) << __func__ << " osd." << *p << " misplaced "
		   << num_objects << " but partially backfilled "
		   << dendl;
	}
      }
    }
    // count extra replicas in acting but not in up as misplaced
    if (in_place < actingset.size())
      misplaced += (actingset.size() - in_place) * num_objects;
    info.stats.stats.sum.num_objects_misplaced = misplaced;
  }
}

void PG::_update_blocked_by()
{
  // set a max on the number of blocking peers we report. if we go
  // over, report a random subset.  keep the result sorted.
  unsigned keep = MIN(blocked_by.size(), g_conf->osd_max_pg_blocked_by); // default is 16
  unsigned skip = blocked_by.size() - keep;
  info.stats.blocked_by.clear();
  info.stats.blocked_by.resize(keep);
  unsigned pos = 0;
  for (set<int>::iterator p = blocked_by.begin();
       p != blocked_by.end() && keep > 0;
       ++p) {
    if (skip > 0 && (rand() % (skip + keep) < skip)) {
      --skip;
    } else {
      info.stats.blocked_by[pos++] = *p;
      --keep;
    }
  }
}

void PG::publish_stats_to_osd()
{
  if (!is_primary()) // only primary can publish
    return;

  pg_stats_publish_lock.Lock();

  if (info.stats.stats.sum.num_scrub_errors)
    state_set(PG_STATE_INCONSISTENT);
  else
    state_clear(PG_STATE_INCONSISTENT);

  utime_t now = ceph_clock_now(cct);
  if (info.stats.state != state) { // PG_STATE_XXX changed
    info.stats.state = state;
    info.stats.last_change = now;
    if ((state & PG_STATE_ACTIVE) &&
	!(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_became_active = now;
    if ((state & (PG_STATE_ACTIVE|PG_STATE_PEERED)) &&
	!(info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED)))
      info.stats.last_became_peered = now;
  }

  _update_calc_stats();
  _update_blocked_by();

  bool publish = false;
  utime_t cutoff = now;
  cutoff -= g_conf->osd_pg_stat_report_interval_max; // default is 500
  if (pg_stats_publish_valid && info.stats == pg_stats_publish &&
      info.stats.last_fresh > cutoff) { // our last refresh in max interval
    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	     << ": no change since" << dendl;
  } else {
    // update our stat summary and timestamps
    info.stats.reported_epoch = get_osdmap()->get_epoch();
    ++info.stats.reported_seq;

    info.stats.last_fresh = now;

    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    if (info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED))
      info.stats.last_peered = now;
    info.stats.last_unstale = now;
    if ((info.stats.state & PG_STATE_DEGRADED) == 0)
      info.stats.last_undegraded = now;
    if ((info.stats.state & PG_STATE_UNDERSIZED) == 0)
      info.stats.last_fullsized = now;

    publish = true;
    pg_stats_publish_valid = true; // resets in PG::clear_publish_stats and PG::start_peering_interval
    pg_stats_publish = info.stats;
    pg_stats_publish.stats.add(unstable_stats);

    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	     << ":" << pg_stats_publish.reported_seq << dendl;
  }
  pg_stats_publish_lock.Unlock();

  if (publish) // we should publish the stats
    osd->pg_stat_queue_enqueue(this); // enqueue on OSD::pg_stat_queue
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();

  osd->pg_stat_queue_dequeue(this);
}

/**
 * initialize a newly instantiated pg
 *
 * Initialize PG state, as when a PG is initially created, or when it
 * is first instantiated on the current node.
 *
 * @param role our role/rank
 * @param newup up set
 * @param newacting acting set
 * @param history pg history
 * @param pi past_intervals
 * @param backfill true if info should be marked as backfill
 * @param t transaction to write out our new state in
 */
void PG::init(
  int role,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  pg_interval_map_t& pi,
  bool backfill,
  ObjectStore::Transaction *t)
{
  dout(10) << "init role " << role << " up " << newup << " acting " << newacting
	   << " history " << history
	   << " " << pi.size() << " past_intervals"
	   << dendl;

  set_role(role);
  acting = newacting;
  up = newup;

  // initialize vector<int> up, vector<int> acting, 
  // pg_shard_t up_primary, pg_shard_t primary, 
  // vector<pg_shard_t> actingset
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  info.history = history;
  past_intervals.swap(pi);

  info.stats.up = up;
  info.stats.up_primary = new_up_primary;
  info.stats.acting = acting;
  info.stats.acting_primary = new_acting_primary;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (backfill) {
    dout(10) << __func__ << ": Setting backfill" << dendl;
    info.set_last_backfill(hobject_t(), get_sort_bitwise());
    info.last_complete = info.last_update;
    pg_log.mark_log_for_rewrite(); // mark to remove all old pg log entries
  }

  // register the next scrub if we are primary, update the min feature set
  // with other osd(s), then update PG::do_sort_bitwise and may update 
  // ReplicatedPG::object_contexts if PG::do_sort_bitwise changed
  on_new_interval();

  dirty_info = true;
  dirty_big_info = true;

  // write pg info and pg log, includes past_intervals and purged_snaps
  write_if_dirty(*t);
}

void PG::upgrade(ObjectStore *store)
{
  assert(info_struct_v <= 8);
  ObjectStore::Transaction t;

  assert(0 == "no support for pre v8");
}

int PG::_prepare_write_info(map<string,bufferlist> *km,
			    epoch_t epoch,
			    pg_info_t &info, coll_t coll,
			    map<epoch_t,pg_interval_t> &past_intervals,
			    ghobject_t &pgmeta_oid,
			    bool dirty_big_info)
{
  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  ::encode(epoch, (*km)[epoch_key]); // "_epoch"
  purged_snaps.swap(info.purged_snaps);
  ::encode(info, (*km)[info_key]); // "_info"
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = (*km)[biginfo_key]; // "_biginfo"
    ::encode(past_intervals, bigbl);
    ::encode(info.purged_snaps, bigbl);
    //dout(20) << "write_info bigbl " << bigbl.length() << dendl;
  }

  return 0;
}

void PG::_create(ObjectStore::Transaction& t, spg_t pgid, int bits)
{
  coll_t coll(pgid);
  t.create_collection(coll, bits);
}

void PG::_init(ObjectStore::Transaction& t, spg_t pgid, const pg_pool_t *pool)
{
  coll_t coll(pgid);

  if (pool) {
    // Give a hint to the PG collection
    bufferlist hint;
    uint32_t pg_num = pool->get_pg_num();
    uint64_t expected_num_objects_pg = pool->expected_num_objects / pg_num;
    ::encode(pg_num, hint);
    ::encode(expected_num_objects_pg, hint);
    uint32_t hint_type = ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS;
    t.collection_hint(coll, hint_type, hint);
  }

  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  t.touch(coll, pgmeta_oid);
  map<string,bufferlist> values;
  __u8 struct_v = cur_struct_v;
  ::encode(struct_v, values[infover_key]);
  t.omap_setkeys(coll, pgmeta_oid, values);
}

void PG::prepare_write_info(map<string,bufferlist> *km)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  // encode current pg epoch, pg info, and if dirty_big_info is true encode
  // past_intervals and info.purged_snaps too into km
  int ret = _prepare_write_info(km, get_osdmap()->get_epoch(), info, coll,
				past_intervals, pgmeta_oid,
				dirty_big_info);
  assert(ret == 0);
  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
  dirty_big_info = false;
}

bool PG::_has_removal_flag(ObjectStore *store,
			   spg_t pgid)
{
  coll_t coll(pgid);
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());

  // first try new way
  set<string> keys;
  keys.insert("_remove");
  map<string,bufferlist> values;
  if (store->omap_get_values(coll, pgmeta_oid, keys, &values) == 0 &&
      values.size() == 1)
    return true;

  return false;
}

int PG::peek_map_epoch(ObjectStore *store,
		       spg_t pgid,
		       epoch_t *pepoch,
		       bufferlist *bl)
{
  coll_t coll(pgid);
  ghobject_t legacy_infos_oid(OSD::make_infos_oid());
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid()); // e.g. current/5.7_head/__head_00000007__5
  epoch_t cur_epoch = 0;

  assert(bl);
  {
    // validate collection name
    assert(coll.is_pg());
  }

  // try for v8
  set<string> keys;
  keys.insert(infover_key); // "_infover"
  keys.insert(epoch_key); // "_epoch"
  map<string,bufferlist> values;
  int r = store->omap_get_values(coll, pgmeta_oid, keys, &values);
  if (r != 0) {
    // probably bug 10617; see OSD::load_pgs()
    return -1;
  }
  assert(values.size() == 2);

  // sanity check version
  bufferlist::iterator bp = values[infover_key].begin();
  __u8 struct_v = 0;
  ::decode(struct_v, bp);
  assert(struct_v >= 8);

  // get epoch
  bp = values[epoch_key].begin();
  ::decode(cur_epoch, bp);

  *pepoch = cur_epoch;
  return 0;
}

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  map<string,bufferlist> km;
  if (dirty_big_info || dirty_info)
    // encode current pg epoch, pg info, and maybe (when dirty_big_info is true) 
    // past_intervals and info.purged_snaps into km
    prepare_write_info(&km);

  // remove old dirty log entries, and encode the new dirty log entries into km
  pg_log.write_log(t, &km, coll, pgmeta_oid);

  // ok, we get all what we want to write, includes current pg epoch, pg info, and
  // maybe past_intervals and info.purged_snaps, new dirty log entries are also
  // included, all these have been encoded into km (note: the old dirty entries 
  // have been removed in pg_log.write_log)
  
  // write km into omap of the pg meta object
  if (!km.empty())
    t.omap_setkeys(coll, pgmeta_oid, km);
}

void PG::trim_peers()
{
  assert(is_primary());

  // calc which eversion of pg log we can trim to, i.e. ReplicatedPG::pg_trim_to,
  // never past min_last_complete_ondisk 
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  
  if (pg_trim_to != eversion_t()) { // ok, we want to trim some of the pg log entries
    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      osd->send_message_osd_cluster(
	i->osd,
	new MOSDPGTrim(
	  get_osdmap()->get_epoch(),
	  spg_t(info.pgid.pgid, i->shard),
	  pg_trim_to), // eversion of the pg log entry i want the replicas to trim to
	get_osdmap()->get_epoch()); // the osdmap epoch i send the message
    }
  }
}

void PG::add_log_entry(const pg_log_entry_t& e)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    // if this condition is true, then even if we get called multiple times by the 
    // caller, info.last_complete still equals to info.last_update
    info.last_complete = e.version;
  
  // raise last_update.
  assert(e.version > info.last_update);
  info.last_update = e.version;

  // raise user_version, if it increased (it may have not get bumped
  // by all logged updates)
  if (e.user_version > info.last_user_version)
    info.last_user_version = e.user_version;

  // log mutation
  pg_log.add(e);
  dout(10) << "add_log_entry " << e << dendl;
}


void PG::append_log(
  const vector<pg_log_entry_t>& logv,
  eversion_t trim_to,
  eversion_t trim_rollback_to,
  ObjectStore::Transaction &t,
  bool transaction_applied)
{
  if (transaction_applied)
    update_snap_map(logv, t);

  /* The primary has sent an info updating the history, but it may not
   * have arrived yet.  We want to make sure that we cannot remember this
   * write without remembering that it happened in an interval which went
   * active in epoch history.last_epoch_started.
   */
  if (info.last_epoch_started != info.history.last_epoch_started) {
    info.history.last_epoch_started = info.last_epoch_started;
  }
  dout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  for (vector<pg_log_entry_t>::const_iterator p = logv.begin();
       p != logv.end();
       ++p) {
    // update info.last_complete if needed, update info.last_update to
    // the logged operation and push this log entry back of PG::pg_log,
    // so, if the op generates multiple log entries, the info.last_update
    // always update to the latest eversion, the info.last_complete may
    // lag behind
    add_log_entry(*p);
  }

  PGLogEntryHandler handler;
  if (!transaction_applied) {
    pg_log.clear_can_rollback_to(&handler);
    t.register_on_applied(
      new C_UpdateLastRollbackInfoTrimmedToApplied(
	this,
	get_osdmap()->get_epoch(),
	info.last_update));
  } else if (trim_rollback_to > pg_log.get_rollback_trimmed_to()) {
    // update log.can_rollback_to, log.rollback_info_trimmed_to and 
    // log.rollback_info_trimmed_to_riter
    pg_log.trim_rollback_info(
      trim_rollback_to,
      &handler);
    t.register_on_applied(
      new C_UpdateLastRollbackInfoTrimmedToApplied(
	this,
	get_osdmap()->get_epoch(),
	trim_rollback_to));
  }

  // trim pg log, i.e. remove old entries from the log entry list, pg_log.trim
  // will also record trimmed log entries in "handler", but the trim_to always
  // <= trim_rollback_to, so no duplicate entries recorded
  pg_log.trim(&handler, trim_to, info);

  dout(10) << __func__ << ": trimming to " << trim_rollback_to
	   << " entries " << handler.to_trim << dendl;

  // apply the trim, i.e. remove the stashed objects
  handler.apply(this, &t);

  // update the local pg, pg log
  dirty_info = true;

  // write pg info, pg log entries, so the info.last_complete, info.last_update, 
  // etc. get persisted
  write_if_dirty(t);
}

bool PG::check_log_for_corruption(ObjectStore *store)
{
  /// TODO: this method needs to work with the omap log
  return true;
}

//! Get the name we're going to save our corrupt page log as
std::string PG::get_corrupt_pg_log_name() const
{
  const int MAX_BUF = 512;
  char buf[MAX_BUF];
  struct tm tm_buf;
  time_t my_time(time(NULL));
  const struct tm *t = localtime_r(&my_time, &tm_buf);
  int ret = strftime(buf, sizeof(buf), "corrupt_log_%Y-%m-%d_%k:%M_", t);
  if (ret == 0) {
    dout(0) << "strftime failed" << dendl;
    return "corrupt_log_unknown_time";
  }
  string out(buf);
  out += stringify(info.pgid);
  return out;
}

int PG::read_info(
  ObjectStore *store, spg_t pgid, const coll_t &coll, bufferlist &bl,
  pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
  __u8 &struct_v)
{
  // try for v8 or later
  set<string> keys;
  keys.insert(infover_key); // "_infover"
  keys.insert(info_key); // "_info"
  keys.insert(biginfo_key); // "_biginfo"
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid()); // e.g. current/5.4_head/__head_00000004__5
  map<string,bufferlist> values;

  // get values of sepcified keys from pg's meta object, e.g.
  // current/5.4_head/__head_00000004__5
  int r = store->omap_get_values(coll, pgmeta_oid, keys, &values);
  if (r == 0) {
    assert(values.size() == 3);

    bufferlist::iterator p = values[infover_key].begin();
    ::decode(struct_v, p);
    assert(struct_v >= 8);

    p = values[info_key].begin();
    // pg_info_t
    ::decode(info, p);

    p = values[biginfo_key].begin();

    // map<epoch_t,pg_interval_t>
    ::decode(past_intervals, p);
    // interval_set<snapid_t>
    ::decode(info.purged_snaps, p);
    return 0;
  }

  // legacy (ver < 8)
  ghobject_t infos_oid(OSD::make_infos_oid());
  bufferlist::iterator p = bl.begin();
  ::decode(struct_v, p);
  assert(struct_v == 7);

  // get info out of leveldb
  string k = get_info_key(info.pgid);
  string bk = get_biginfo_key(info.pgid);
  keys.clear();
  keys.insert(k);
  keys.insert(bk);
  values.clear();
  store->omap_get_values(coll_t::meta(), ghobject_t(infos_oid), keys, &values);
  assert(values.size() == 2);

  p = values[k].begin();
  ::decode(info, p);

  p = values[bk].begin();
  ::decode(past_intervals, p);
  interval_set<snapid_t> snap_collections;  // obsolete
  ::decode(snap_collections, p);
  ::decode(info.purged_snaps, p);
  return 0;
}

void PG::read_state(ObjectStore *store, bufferlist &bl)
{
  // get PG::info, PG::past_intervals from pg meta object, e.g. 
  // current/5.4_head/__head_00000004__5
  int r = read_info(store, pg_id, coll, bl, info, past_intervals,
		    info_struct_v);
  assert(r >= 0);

  ostringstream oss;

  // read PGLog::log, PGLog::missing, PGLog::divergent_priors from pg's meta object,
  // old meta object, e.g. current/meta/pglog\u342.a1__0_A0B3C172__none
  // new meta object, e.g. current/5.4_head/__head_00000004__5
  // osd map is still stored in current/meta, e.g.
  // current/meta/DIR_4/DIR_0/osdmap.156833__0_DF8E0104__none
  pg_log.read_log(store,
		  coll,
		  info_struct_v < 8 ? coll_t::meta() : coll,
		  ghobject_t(info_struct_v < 8 ? OSD::make_pg_log_oid(pg_id) : pgmeta_oid),
		  info, oss);
  if (oss.tellp())
    osd->clog->error() << oss.rdbuf();

  // log any weirdness, do osd->clog log
  log_weirdness();
}

void PG::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    osd->clog->error() << info.pgid
		      << " info mismatch, log.tail " << pg_log.get_tail()
		      << " != info.log_tail " << info.log_tail
		      << "\n";
  if (pg_log.get_head() != info.last_update)
    osd->clog->error() << info.pgid
		      << " info mismatch, log.head " << pg_log.get_head()
		      << " != info.last_update " << info.last_update
		      << "\n";

  if (!pg_log.get_log().empty()) {
    // sloppy check
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()))
      osd->clog->error() << info.pgid
			<< " log bound mismatch, info (" << pg_log.get_tail() << ","
			<< pg_log.get_head() << "]"
			<< " actual ["
			<< pg_log.get_log().log.begin()->version << ","
			<< pg_log.get_log().log.rbegin()->version << "]"
			<< "\n";
  }
  
  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    osd->clog->error() << info.pgid
		      << " caller_ops.size " << pg_log.get_log().caller_ops.size()
		      << " > log size " << pg_log.get_log().log.size()
		      << "\n";
  }
}

void PG::update_snap_map(
  const vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction &t)
{
  for (vector<pg_log_entry_t>::const_iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	assert(r == 0);
      } else {
	assert(i->snaps.length() > 0);
	vector<snapid_t> snaps;
	bufferlist snapbl = i->snaps;
	bufferlist::iterator p = snapbl.begin();
	try {
	  ::decode(snaps, p);
	} catch (...) {
	  snaps.clear();
	}
	set<snapid_t> _snaps(snaps.begin(), snaps.end());

	if (i->is_clone() || i->is_promote()) {
	  snap_mapper.add_oid(
	    i->soid,
	    _snaps,
	    &_t);
	} else if (i->is_modify()) {
	  assert(i->is_modify());
	  int r = snap_mapper.update_snaps(
	    i->soid,
	    _snaps,
	    0,
	    &_t);
	  assert(r == 0);
	} else {
	  assert(i->is_clean());
	}
      }
    }
  }
}

/**
 * filter trimming|trimmed snaps out of snapcontext
 */
void PG::filter_snapc(vector<snapid_t> &snaps)
{
  //nothing needs to trim, we can return immediately
  if(snap_trimq.empty() && info.purged_snaps.empty())
    return;

  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snaps.begin();
       p != snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snaps << dendl;
	newsnaps.insert(newsnaps.begin(), snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snaps << dendl;
  }
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef>, hobject_t::BitwiseComparator>& m)
{
  for (map<hobject_t, list<OpRequestRef>, hobject_t::BitwiseComparator>::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_op(OpRequestRef op)
{
  osd->op_wq.queue_front(make_pair(PGRef(this), op));
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(PGRef(this), *i));
  }
  ls.clear();
}


// ==========================================================================================
// SCRUB

/*
 * when holding pg and sched_scrub_lock, then the states are:
 *   scheduling:
 *     scrubber.reserved = true
 *     scrub_rserved_peers includes whoami
 *     osd->scrub_pending++
 *   scheduling, replica declined:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers includes -1
 *     osd->scrub_pending++
 *   pending:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers.size() == acting.size();
 *     pg on scrub_wq
 *     osd->scrub_pending++
 *   scrubbing:
 *     scrubber.reserved = false;
 *     scrubber.reserved_peers empty
 *     osd->scrubber.active++
 */

// returns true if a scrub has been newly kicked off
bool PG::sched_scrub()
{
  assert(_lock.is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return false;
  }

  bool time_for_deep = (ceph_clock_now(cct) >=
    info.history.last_deep_scrub_stamp + cct->_conf->osd_deep_scrub_interval);

  //NODEEP_SCRUB so ignore time initiated deep-scrub
  if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
      pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB))
    time_for_deep = false;

  if (!scrubber.must_scrub) {
    assert(!scrubber.must_deep_scrub);

    //NOSCRUB so skip regular scrubs
    if ((osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
	 pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB)) && !time_for_deep)
      return false;
  }

  if (cct->_conf->osd_scrub_auto_repair
      && get_pgbackend()->auto_repair_supported()
      && time_for_deep
      // respect the command from user, and not do auto-repair
      && !scrubber.must_repair
      && !scrubber.must_scrub
      && !scrubber.must_deep_scrub) {
    dout(20) << __func__ << ": auto repair with deep scrubbing" << dendl;
    scrubber.auto_repair = true;
  } else {
    // this happens when user issue the scrub/repair command during
    // the scheduling of the scrub/repair (e.g. request reservation)
    scrubber.auto_repair = false;
  }

  bool ret = true;
  if (!scrubber.reserved) {
    assert(scrubber.reserved_peers.empty());
    if (osd->inc_scrubs_pending()) {
      dout(20) << "sched_scrub: reserved locally, reserving replicas" << dendl;
      scrubber.reserved = true;
      scrubber.reserved_peers.insert(pg_whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << "sched_scrub: failed to reserve locally" << dendl;
      ret = false;
    }
  }
  if (scrubber.reserved) {
    if (scrubber.reserve_failed) {
      dout(20) << "sched_scrub: failed, a peer declined" << dendl;
      clear_scrub_reserved();
      scrub_unreserve_replicas();
      ret = false;
    } else if (scrubber.reserved_peers.size() == acting.size()) {
      dout(20) << "sched_scrub: success, reserved self and replicas" << dendl;
      if (time_for_deep) {
	dout(10) << "sched_scrub: scrub will be deep" << dendl;
	state_set(PG_STATE_DEEP_SCRUB);
      }
      queue_scrub();
    } else {
      // none declined, since scrubber.reserved is set
      dout(20) << "sched_scrub: reserved " << scrubber.reserved_peers << ", waiting for replicas" << dendl;
    }
  }

  return ret;
}

void PG::reg_next_scrub()
{
  if (!is_primary()) // pg_whoami == primary
    return;

  utime_t reg_stamp;
  if (scrubber.must_scrub ||
      (info.stats.stats_invalid && g_conf->osd_scrub_invalid_stats)) { // default is true
    reg_stamp = ceph_clock_now(cct);
  } else {
    reg_stamp = info.history.last_scrub_stamp;
  }
  // note down the sched_time, so we can locate this scrub, and remove it
  // later on.
  // reg a scrub job in OSDService::sched_scrub_pg
  scrubber.scrub_reg_stamp = osd->reg_pg_scrub(info.pgid,
					       reg_stamp,
					       scrubber.must_scrub);
}

void PG::unreg_next_scrub()
{
  if (is_primary()) // remove last scrub job from OSDService::sched_scrub_pg
    osd->unreg_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::sub_op_scrub_map(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->get_req());
  assert(m->get_type() == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_map" << dendl;

  if (m->map_epoch < info.history.same_interval_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << m->map_epoch << " < " << info.history.same_interval_since << dendl;
    return;
  }

  if (!scrubber.is_chunky_scrub_active()) {
    dout(10) << "sub_op_scrub_map scrub isn't active" << dendl;
    return;
  }

  op->mark_started();

  dout(10) << " got " << m->from << " scrub map" << dendl;
  bufferlist::iterator p = m->get_data().begin();

  scrubber.received_maps[m->from].decode(p, info.pgid.pool());
  dout(10) << "map version is "
	     << scrubber.received_maps[m->from].valid_through
	     << dendl;

  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(m->from);

  if (scrubber.waiting_on == 0) {
    requeue_scrub();
  }
}

// send scrub v3 messages (chunky scrub)
void PG::_request_scrub_map(
  pg_shard_t replica, eversion_t version,
  hobject_t start, hobject_t end,
  bool deep, uint32_t seed)
{
  assert(replica != pg_whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica
	   << " deep " << (int)deep << " seed " << seed << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(
    spg_t(info.pgid.pgid, replica.shard), version,
    get_osdmap()->get_epoch(),
    start, end, deep, seed);
  // default priority, we want the rep scrub processed prior to any recovery
  // or client io messages (we are holding a lock!)
  osd->send_message_osd_cluster(
    replica.osd, repscrubop, get_osdmap()->get_epoch());
}

void PG::sub_op_scrub_reserve(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_type() == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_reserve" << dendl;

  if (scrubber.reserved) {
    dout(10) << "Ignoring reserve request: Already reserved" << dendl;
    return;
  }

  op->mark_started();

  scrubber.reserved = osd->inc_scrubs_pending();

  MOSDSubOpReply *reply = new MOSDSubOpReply(
    m, pg_whoami, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  ::encode(scrubber.reserved, reply->get_data());
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::sub_op_scrub_reserve_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->get_req());
  assert(reply->get_type() == MSG_OSD_SUBOPREPLY);
  dout(7) << "sub_op_scrub_reserve_reply" << dendl;

  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  op->mark_started();

  pg_shard_t from = reply->from;
  bufferlist::iterator p = reply->get_data().begin();
  bool reserved;
  ::decode(reserved, p);

  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    if (reserved) {
      dout(10) << " osd." << from << " scrub reserve = success" << dendl;
      scrubber.reserved_peers.insert(from);
    } else {
      /* One decline stops this pg from being scheduled for scrubbing. */
      dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
      scrubber.reserve_failed = true;
    }
    sched_scrub();
  }
}

void PG::sub_op_scrub_unreserve(OpRequestRef op)
{
  assert(op->get_req()->get_type() == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_unreserve" << dendl;

  op->mark_started();

  clear_scrub_reserved();
}

void PG::sub_op_scrub_stop(OpRequestRef op)
{
  op->mark_started();

  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_type() == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_stop" << dendl;

  // see comment in sub_op_scrub_reserve
  scrubber.reserved = false;

  MOSDSubOpReply *reply = new MOSDSubOpReply(
    m, pg_whoami, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::reject_reservation()
{
  osd->send_message_osd_cluster(
    primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap()->get_epoch()),
    get_osdmap()->get_epoch());
}

void PG::schedule_backfill_full_retry()
{
  Mutex::Locker lock(osd->backfill_request_lock);
  osd->backfill_request_timer.add_event_after(
    cct->_conf->osd_backfill_retry_interval,
    new QueuePeeringEvt<RequestBackfill>(
      this, get_osdmap()->get_epoch(),
      RequestBackfill()));
}

void PG::clear_scrub_reserved()
{
  scrubber.reserved_peers.clear();
  scrubber.reserve_failed = false;

  if (scrubber.reserved) {
    scrubber.reserved = false;
    osd->dec_scrubs_pending(); // OSDService::scrubs_pending--
  }
}

void PG::scrub_reserve_replicas()
{
  assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting reserve from osd." << *i << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_RESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(
      reqid, pg_whoami, spg_t(info.pgid.pgid, i->shard), poid, 0,
      get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(
      i->osd, subop, get_osdmap()->get_epoch());
  }
}

void PG::scrub_unreserve_replicas()
{
  assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting unreserve from osd." << *i << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_UNRESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(
      reqid, pg_whoami, spg_t(info.pgid.pgid, i->shard), poid, 0,
      get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(i->osd, subop, get_osdmap()->get_epoch());
  }
}

void PG::_scan_rollback_obs(
  const vector<ghobject_t> &rollback_obs,
  ThreadPool::TPHandle &handle)
{
  ObjectStore::Transaction *t = NULL;
  eversion_t trimmed_to = last_rollback_info_trimmed_to_applied;
  for (vector<ghobject_t>::const_iterator i = rollback_obs.begin();
       i != rollback_obs.end();
       ++i) {
    if (i->generation < trimmed_to.version) {
      osd->clog->error() << "osd." << osd->whoami
			<< " pg " << info.pgid
			<< " found obsolete rollback obj "
			<< *i << " generation < trimmed_to "
			<< trimmed_to
			<< "...repaired";
      if (!t)
	t = new ObjectStore::Transaction;
      t->remove(coll, *i);
    }
  }
  if (t) {
    derr << __func__ << ": queueing trans to clean up obsolete rollback objs"
	 << dendl;
    osd->store->queue_transaction_and_cleanup(osr.get(), t);
  }
}

void PG::_scan_snaps(ScrubMap &smap) 
{
  for (map<hobject_t, ScrubMap::object, hobject_t::BitwiseComparator>::iterator i = smap.objects.begin();
       i != smap.objects.end();
       ++i) {
    const hobject_t &hoid = i->first;
    ScrubMap::object &o = i->second;

    if (hoid.snap < CEPH_MAXSNAP) {
      // fake nlinks for old primaries
      bufferlist bl;
      bl.push_back(o.attrs[OI_ATTR]);
      object_info_t oi(bl);
      if (oi.snaps.empty()) {
	// Just head
	o.nlinks = 1;
      } else if (oi.snaps.size() == 1) {
	// Just head + only snap
	o.nlinks = 2;
      } else {
	// Just head + 1st and last snaps
	o.nlinks = 3;
      }

      // check and if necessary fix snap_mapper
      set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
      set<snapid_t> cur_snaps;
      int r = snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	assert(0);
      }
      if (r == -ENOENT || cur_snaps != oi_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	if (r == 0) {
	  r = snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r)
		 << dendl;
	    assert(0);
	  }
	  osd->clog->error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps in mapper: "
			    << cur_snaps << ", oi: "
			    << oi_snaps
			    << "...repaired";
	} else {
	  osd->clog->error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps missing in mapper"
			    << ", should be: "
			    << oi_snaps
			    << "...repaired";
	}
	snap_mapper.add_oid(hoid, oi_snaps, &_t);
	r = osd->store->apply_transaction(osr.get(), t);
	if (r != 0) {
	  derr << __func__ << ": apply_transaction got " << cpp_strerror(r)
	       << dendl;
	}
      }
    } else {
      o.nlinks = 1;
    }
  }
}

/*
 * build a scrub map over a chunk without releasing the lock
 * only used by chunky scrub
 */
int PG::build_scrub_map_chunk(
  ScrubMap &map,
  hobject_t start, hobject_t end, bool deep, uint32_t seed,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " [" << start << "," << end << ") "
	   << " seed " << seed << dendl;

  map.valid_through = info.last_update;

  // objects
  vector<hobject_t> ls;
  vector<ghobject_t> rollback_obs;
  int ret = get_pgbackend()->objects_list_range(
    start,
    end,
    0,
    &ls,
    &rollback_obs);
  if (ret < 0) {
    dout(5) << "objects_list_range error: " << ret << dendl;
    return ret;
  }


  get_pgbackend()->be_scan_list(map, ls, deep, seed, handle);
  _scan_rollback_obs(rollback_obs, handle);
  _scan_snaps(map);

  dout(20) << __func__ << " done" << dendl;
  return 0;
}

void PG::repair_object(
  const hobject_t& soid, list<pair<ScrubMap::object, pg_shard_t> > *ok_peers,
  pg_shard_t bad_peer)
{
  dout(10) << "repair_object " << soid << " bad_peer osd."
	   << bad_peer << " ok_peers osd.{" << ok_peers << "}" << dendl;
  ScrubMap::object &po = ok_peers->back().first;
  eversion_t v;
  bufferlist bv;
  bv.push_back(po.attrs[OI_ATTR]);
  object_info_t oi(bv);
  if (bad_peer != primary) {
    peer_missing[bad_peer].add(soid, oi.version, eversion_t());
  } else {
    // We should only be scrubbing if the PG is clean.
    assert(waiting_for_unreadable_object.empty());

    pg_log.missing_add(soid, oi.version, eversion_t());

    pg_log.set_last_requested(0);
    dout(10) << __func__ << ": primary = " << primary << dendl;
  }

  if (is_ec_pg() || bad_peer == primary) {
    // we'd better collect all shard for EC pg, and prepare good peers as the
    // source of pull in the case of replicated pg.
    missing_loc.add_missing(soid, oi.version, eversion_t());
    list<pair<ScrubMap::object, pg_shard_t> >::iterator i;
    for (i = ok_peers->begin();
	i != ok_peers->end();
	++i)
      missing_loc.add_location(soid, i->second);
  }
}

/* replica_scrub
 *
 * Wait for last_update_applied to match msg->scrub_to as above. Wait
 * for pushes to complete in case of recent recovery. Build a single
 * scrubmap of objects that are in the range [msg->start, msg->end).
 */
void PG::replica_scrub(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  MOSDRepScrub *msg = static_cast<MOSDRepScrub *>(op->get_req());
  assert(!scrubber.active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_interval_since) {
    dout(10) << "replica_scrub discarding old replica_scrub from "
	     << msg->map_epoch << " < " << info.history.same_interval_since 
	     << dendl;
    return;
  }

  ScrubMap map;

  assert(msg->chunky);
  if (last_update_applied < msg->scrub_to) {
    dout(10) << "waiting for last_update_applied to catch up" << dendl;
    scrubber.active_rep_scrub = op;
    msg->get();
    return;
  }

  if (active_pushes > 0) {
    dout(10) << "waiting for active pushes to finish" << dendl;
    scrubber.active_rep_scrub = op;
    return;
  }

  // compensate for hobject_t's with wrong pool from sloppy hammer OSDs
  hobject_t start = msg->start;
  hobject_t end = msg->end;
  start.pool = info.pgid.pool();
  end.pool = info.pgid.pool();

  build_scrub_map_chunk(
    map, start, end, msg->deep, msg->seed,
    handle);

  vector<OSDOp> scrub(1);
  scrub[0].op.op = CEPH_OSD_OP_SCRUB_MAP;
  hobject_t poid;
  eversion_t v;
  osd_reqid_t reqid;
  MOSDSubOp *subop = new MOSDSubOp(
    reqid,
    pg_whoami,
    spg_t(info.pgid.pgid, get_primary().shard),
    poid,
    0,
    msg->map_epoch,
    osd->get_tid(),
    v);
  ::encode(map, subop->get_data());
  subop->ops = scrub;

  osd->send_message_osd_cluster(subop, msg->get_connection());
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * scrub will be chunky if all OSDs in PG support chunky scrub
 * scrub will fail if OSDs are too old.
 */
void PG::scrub(epoch_t queued, ThreadPool::TPHandle &handle)
{
  if (g_conf->osd_scrub_sleep > 0 &&
      (scrubber.state == PG::Scrubber::NEW_CHUNK ||
       scrubber.state == PG::Scrubber::INACTIVE)) {
    dout(20) << __func__ << " state is INACTIVE|NEW_CHUNK, sleeping" << dendl;
    unlock();
    utime_t t;
    t.set_from_double(g_conf->osd_scrub_sleep);
    t.sleep();
    lock();
    dout(20) << __func__ << " slept for " << t << dendl;
  }
  if (pg_has_reset_since(queued)) {
    return;
  }
  assert(scrub_queued);
  scrub_queued = false;

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_DEEP_SCRUB);
    publish_stats_to_osd();
    return;
  }

  if (!scrubber.active) {
    assert(backfill_targets.empty());

    scrubber.deep = state_test(PG_STATE_DEEP_SCRUB);

    dout(10) << "starting a new chunky scrub" << dendl;
  }

  chunky_scrub(handle);
}

/*
 * Chunky scrub scrubs objects one chunk at a time with writes blocked for that
 * chunk.
 *
 * The object store is partitioned into chunks which end on hash boundaries. For
 * each chunk, the following logic is performed:
 *
 *  (1) Block writes on the chunk
 *  (2) Request maps from replicas
 *  (3) Wait for pushes to be applied (after recovery)
 *  (4) Wait for writes to flush on the chunk
 *  (5) Wait for maps from replicas
 *  (6) Compare / repair all scrub maps
 *  (7) Wait for digest updates to apply
 *
 * This logic is encoded in the mostly linear state machine:
 *
 *           +------------------+
 *  _________v__________        |
 * |                    |       |
 * |      INACTIVE      |       |
 * |____________________|       |
 *           |                  |
 *           |   +----------+   |
 *  _________v___v______    |   |
 * |                    |   |   |
 * |      NEW_CHUNK     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |     WAIT_PUSHES    |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |  WAIT_LAST_UPDATE  |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |      BUILD_MAP     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    WAIT_REPLICAS   |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    COMPARE_MAPS    |   |   |
 * |____________________|   |   |
 *           |              |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |WAIT_DIGEST_UPDATES |   |   |
 * |____________________|   |   |
 *           |   |          |   |
 *           |   +----------+   |
 *  _________v__________        |
 * |                    |       |
 * |       FINISH       |       |
 * |____________________|       |
 *           |                  |
 *           +------------------+
 *
 * The primary determines the last update from the subset by walking the log. If
 * it sees a log entry pertaining to a file in the chunk, it tells the replicas
 * to wait until that update is applied before building a scrub map. Both the
 * primary and replicas will wait for any active pushes to be applied.
 *
 * In contrast to classic_scrub, chunky_scrub is entirely handled by scrub_wq.
 *
 * scrubber.state encodes the current state of the scrub (refer to state diagram
 * for details).
 */
void PG::chunky_scrub(ThreadPool::TPHandle &handle)
{
  // check for map changes
  if (scrubber.is_chunky_scrub_active()) {
    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }
  }

  bool done = false;
  int ret;

  while (!done) {
    dout(20) << "scrub state " << Scrubber::state_string(scrubber.state) << dendl;

    switch (scrubber.state) {
      case PG::Scrubber::INACTIVE:
        dout(10) << "scrub start" << dendl;

        publish_stats_to_osd();
        scrubber.epoch_start = info.history.same_interval_since;
        scrubber.active = true;

	osd->inc_scrubs_active(scrubber.reserved);
	if (scrubber.reserved) {
	  scrubber.reserved = false;
	  scrubber.reserved_peers.clear();
	}

        scrubber.start = hobject_t();
        scrubber.state = PG::Scrubber::NEW_CHUNK;

	{
	  bool repair = state_test(PG_STATE_REPAIR);
	  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
	  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));
	  stringstream oss;
	  oss << info.pgid.pgid << " " << mode << " starts" << std::endl;
	  osd->clog->info(oss);
	}

	if (get_min_acting_features() & CEPH_FEATURE_OSD_OBJECT_DIGEST)
	  scrubber.seed = -1; // better, and enables oi digest checks
	else
	  scrubber.seed = 0;  // compat

        break;

      case PG::Scrubber::NEW_CHUNK:
        scrubber.primary_scrubmap = ScrubMap();
        scrubber.received_maps.clear();

        {
	  hobject_t candidate_end;

          // get the start and end of our scrub chunk
          //
          // start and end need to lie on a hash boundary. We test for this by
          // requesting a list and searching backward from the end looking for a
          // boundary. If there's no boundary, we request a list after the first
          // list, and so forth.

          bool boundary_found = false;
          hobject_t start = scrubber.start;
          while (!boundary_found) {
            vector<hobject_t> objects;
            ret = get_pgbackend()->objects_list_partial(
	      start,
	      cct->_conf->osd_scrub_chunk_min,
	      cct->_conf->osd_scrub_chunk_max,
	      &objects,
	      &candidate_end);
            assert(ret >= 0);

            // in case we don't find a boundary: start again at the end
            start = candidate_end;

            // special case: reached end of file store, implicitly a boundary
            if (objects.empty()) {
              break;
            }

            // search backward from the end looking for a boundary
            objects.push_back(candidate_end);
            while (!boundary_found && objects.size() > 1) {
              hobject_t end = objects.back().get_boundary();
              objects.pop_back();

              if (objects.back().get_hash() != end.get_hash()) {
                candidate_end = end;
                boundary_found = true;
              }
            }
          }

	  if (!_range_available_for_scrub(scrubber.start, candidate_end)) {
	    // we'll be requeued by whatever made us unavailable for scrub
	    dout(10) << __func__ << ": scrub blocked somewhere in range "
		     << "[" << scrubber.start << ", " << candidate_end << ")"
		     << dendl;
	    done = true;
	    break;
	  }
	  scrubber.end = candidate_end;
        }

        // walk the log to find the latest update that affects our chunk
        scrubber.subset_last_update = pg_log.get_tail();
        for (list<pg_log_entry_t>::const_iterator p = pg_log.get_log().log.begin();
             p != pg_log.get_log().log.end();
             ++p) {
          if (cmp(p->soid, scrubber.start, get_sort_bitwise()) >= 0 &&
	      cmp(p->soid, scrubber.end, get_sort_bitwise()) < 0)
            scrubber.subset_last_update = p->version;
        }

        // ask replicas to wait until last_update_applied >= scrubber.subset_last_update and then scan
        scrubber.waiting_on_whom.insert(pg_whoami);
        ++scrubber.waiting_on;

        // request maps from replicas
	for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	     i != actingbackfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
          _request_scrub_map(*i, scrubber.subset_last_update,
                             scrubber.start, scrubber.end, scrubber.deep,
			     scrubber.seed);
          scrubber.waiting_on_whom.insert(*i);
          ++scrubber.waiting_on;
        }

        scrubber.state = PG::Scrubber::WAIT_PUSHES;

        break;

      case PG::Scrubber::WAIT_PUSHES:
        if (active_pushes == 0) {
          scrubber.state = PG::Scrubber::WAIT_LAST_UPDATE;
        } else {
          dout(15) << "wait for pushes to apply" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::WAIT_LAST_UPDATE:
        if (last_update_applied >= scrubber.subset_last_update) {
          scrubber.state = PG::Scrubber::BUILD_MAP;
        } else {
          // will be requeued by op_applied
          dout(15) << "wait for writes to flush" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::BUILD_MAP:
        assert(last_update_applied >= scrubber.subset_last_update);

        // build my own scrub map
        ret = build_scrub_map_chunk(scrubber.primary_scrubmap,
                                    scrubber.start, scrubber.end,
                                    scrubber.deep, scrubber.seed,
				    handle);
        if (ret < 0) {
          dout(5) << "error building scrub map: " << ret << ", aborting" << dendl;
          scrub_clear_state();
          scrub_unreserve_replicas();
          return;
        }

        --scrubber.waiting_on;
        scrubber.waiting_on_whom.erase(pg_whoami);

        scrubber.state = PG::Scrubber::WAIT_REPLICAS;
        break;

      case PG::Scrubber::WAIT_REPLICAS:
        if (scrubber.waiting_on > 0) {
          // will be requeued by sub_op_scrub_map
          dout(10) << "wait for replicas to build scrub map" << dendl;
          done = true;
        } else {
          scrubber.state = PG::Scrubber::COMPARE_MAPS;
        }
        break;

      case PG::Scrubber::COMPARE_MAPS:
        assert(last_update_applied >= scrubber.subset_last_update);
        assert(scrubber.waiting_on == 0);

        scrub_compare_maps();
	scrubber.start = scrubber.end;
	scrubber.run_callbacks();

        // requeue the writes from the chunk that just finished
        requeue_ops(waiting_for_active);

	scrubber.state = PG::Scrubber::WAIT_DIGEST_UPDATES;

	// fall-thru

      case PG::Scrubber::WAIT_DIGEST_UPDATES:
	if (scrubber.num_digest_updates_pending) {
	  dout(10) << __func__ << " waiting on "
		   << scrubber.num_digest_updates_pending
		   << " digest updates" << dendl;
	  done = true;
	  break;
	}

	if (cmp(scrubber.end, hobject_t::get_max(), get_sort_bitwise()) < 0) {
          scrubber.state = PG::Scrubber::NEW_CHUNK;
	  requeue_scrub();
          done = true;
        } else {
          scrubber.state = PG::Scrubber::FINISH;
        }

	break;

      case PG::Scrubber::FINISH:
        scrub_finish();
        scrubber.state = PG::Scrubber::INACTIVE;
        done = true;

        break;

      default:
        assert(0);
    }
  }
}

void PG::scrub_clear_state()
{
  assert(_lock.is_locked());
  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);

  // pg state and other stats may changed, if we are primary, we publish our stats
  publish_stats_to_osd();

  // active -> nothing.
  if (scrubber.active)
    osd->dec_scrubs_active(); // OSDService::scrubs_active--

  requeue_ops(waiting_for_active); // requeue on OSDService::op_wq

  if (scrubber.queue_snap_trim) {
    dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
    queue_snap_trim();
  }

  scrubber.reset();

  // type-specific state clear
  _scrub_clear_state();
}

void PG::scrub_compare_maps() 
{
  dout(10) << __func__ << " has maps, analyzing" << dendl;

  // construct authoritative scrub map for type specific scrubbing
  ScrubMap authmap(scrubber.primary_scrubmap);
  map<hobject_t, pair<uint32_t, uint32_t>, hobject_t::BitwiseComparator> missing_digest;

  if (acting.size() > 1) {
    dout(10) << __func__ << "  comparing replica scrub maps" << dendl;

    stringstream ss;

    // Map from object with errors to good peer
    map<hobject_t, list<pg_shard_t>, hobject_t::BitwiseComparator> authoritative;
    map<pg_shard_t, ScrubMap *> maps;

    dout(2) << __func__ << "   osd." << acting[0] << " has "
	    << scrubber.primary_scrubmap.objects.size() << " items" << dendl;
    maps[pg_whoami] = &scrubber.primary_scrubmap;

    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      dout(2) << __func__ << " replica " << *i << " has "
	      << scrubber.received_maps[*i].objects.size()
	      << " items" << dendl;
      maps[*i] = &scrubber.received_maps[*i];
    }

    // can we relate scrub digests to oi digests?
    bool okseed = (get_min_upacting_features() & CEPH_FEATURE_OSD_OBJECT_DIGEST);
    assert(okseed == (scrubber.seed == 0xffffffff));

    get_pgbackend()->be_compare_scrubmaps(
      maps,
      okseed,
      state_test(PG_STATE_REPAIR),
      scrubber.missing,
      scrubber.inconsistent,
      authoritative,
      missing_digest,
      scrubber.shallow_errors,
      scrubber.deep_errors,
      info.pgid, acting,
      ss);
    dout(2) << ss.str() << dendl;

    if (!ss.str().empty()) {
      osd->clog->error(ss);
    }

    for (map<hobject_t, list<pg_shard_t>, hobject_t::BitwiseComparator>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      list<pair<ScrubMap::object, pg_shard_t> > good_peers;
      for (list<pg_shard_t>::const_iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	good_peers.push_back(make_pair(maps[*j]->objects[i->first], *j));
      }
      scrubber.authoritative.insert(
	make_pair(
	  i->first,
	  good_peers));
    }

    for (map<hobject_t, list<pg_shard_t>, hobject_t::BitwiseComparator>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      authmap.objects.erase(i->first);
      authmap.objects.insert(*(maps[i->second.back()]->objects.find(i->first)));
    }
  }

  // ok, do the pg-type specific scrubbing
  _scrub(authmap, missing_digest);
}

bool PG::scrub_process_inconsistent()
{
  dout(10) << __func__ << ": checking authoritative" << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));
  
  // authoriative only store objects which missing or inconsistent.
  if (!scrubber.authoritative.empty()) {
    stringstream ss;
    ss << info.pgid << " " << mode << " "
       << scrubber.missing.size() << " missing, "
       << scrubber.inconsistent.size() << " inconsistent objects";
    dout(2) << ss.str() << dendl;
    osd->clog->error(ss);
    if (repair) {
      state_clear(PG_STATE_CLEAN);
      for (map<hobject_t, list<pair<ScrubMap::object, pg_shard_t> >, hobject_t::BitwiseComparator>::iterator i =
	     scrubber.authoritative.begin();
	   i != scrubber.authoritative.end();
	   ++i) {
	set<pg_shard_t>::iterator j;
	
	if (scrubber.missing.count(i->first)) {
	  for (j = scrubber.missing[i->first].begin();
	       j != scrubber.missing[i->first].end(); 
	       ++j) {
	    repair_object(
	      i->first,
	      &(i->second),
	      *j);
	    ++scrubber.fixed;
	  }
	}
	if (scrubber.inconsistent.count(i->first)) {
	  for (j = scrubber.inconsistent[i->first].begin(); 
	       j != scrubber.inconsistent[i->first].end(); 
	       ++j) {
	    repair_object(i->first, 
	      &(i->second),
	      *j);
	    ++scrubber.fixed;
	  }
	}
      }
    }
  }
  return (!scrubber.authoritative.empty() && repair);
}

// the part that actually finalizes a scrub
void PG::scrub_finish() 
{
  bool repair = state_test(PG_STATE_REPAIR);
  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair
  if (repair && scrubber.auto_repair
      && scrubber.authoritative.size() > cct->_conf->osd_scrub_auto_repair_num_errors) {
    state_clear(PG_STATE_REPAIR);
    repair = false;
  }
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // type-specific finish (can tally more errors)
  _scrub_finish();

  bool has_error = scrub_process_inconsistent();

  {
    stringstream oss;
    oss << info.pgid.pgid << " " << mode << " ";
    int total_errors = scrubber.shallow_errors + scrubber.deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!deep_scrub && info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << info.stats.stats.sum.num_deep_scrub_errors
          << " remaining deep scrub error(s) )";
    if (repair)
      oss << ", " << scrubber.fixed << " fixed";
    oss << "\n";
    if (total_errors)
      osd->clog->error(oss);
    else
      osd->clog->info(oss);
  }

  // finish up
  unreg_next_scrub();
  utime_t now = ceph_clock_now(cct);
  info.history.last_scrub = info.last_update;
  info.history.last_scrub_stamp = now;
  if (scrubber.deep) {
    info.history.last_deep_scrub = info.last_update;
    info.history.last_deep_scrub_stamp = now;
  }
  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (scrubber.fixed == scrubber.shallow_errors + scrubber.deep_errors) {
      assert(deep_scrub);
      scrubber.shallow_errors = scrubber.deep_errors = 0;
    } else {
      // Deep scrub in order to get corrected error counts
      scrub_after_recovery = true;
    }
  }
  if (deep_scrub) {
    if ((scrubber.shallow_errors == 0) && (scrubber.deep_errors == 0))
      info.history.last_clean_scrub_stamp = now;
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    info.stats.stats.sum.num_deep_scrub_errors = scrubber.deep_errors;
  } else {
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
    // because of deep-scrub errors
    if (scrubber.shallow_errors == 0)
      info.history.last_clean_scrub_stamp = now;
  }
  info.stats.stats.sum.num_scrub_errors = 
    info.stats.stats.sum.num_shallow_scrub_errors +
    info.stats.stats.sum.num_deep_scrub_errors;
  reg_next_scrub();

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    dirty_info = true;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }


  if (has_error) {
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  DoRecovery())));
  }

  scrub_clear_state();
  scrub_unreserve_replicas();

  if (is_active() && is_primary()) {
    share_pg_info();
  }
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new pg_info_t with replicas
  assert(!actingbackfill.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    pg_shard_t peer = *i;
    if (peer_info.count(peer)) {
      peer_info[peer].last_epoch_started = info.last_epoch_started;
      peer_info[peer].history.merge(info.history);
    }
    MOSDPGInfo *m = new MOSDPGInfo(get_osdmap()->get_epoch());
    m->pg_list.push_back(
      make_pair(
	pg_notify_t(
	  peer.shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  info),
	pg_interval_map_t()));
    osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
  }
}

/*
 * Share a new segment of this PG's log with some replicas, after PG is active.
 *
 * Updates peer_missing and peer_info.
 */
void PG::share_pg_log()
{
  dout(10) << __func__ << dendl;
  assert(is_primary());

  set<pg_shard_t>::const_iterator a = actingbackfill.begin();
  assert(a != actingbackfill.end());
  set<pg_shard_t>::const_iterator end = actingbackfill.end();
  while (a != end) {
    pg_shard_t peer(*a);
    ++a;
    if (peer == pg_whoami) continue;
    pg_missing_t& pmissing(peer_missing[peer]);
    pg_info_t& pinfo(peer_info[peer]);

    MOSDPGLog *m = new MOSDPGLog(
      peer.shard, pg_whoami.shard,
      info.last_update.epoch, info);
    m->log.copy_after(pg_log.get_log(), pinfo.last_update);

    for (list<pg_log_entry_t>::const_iterator i = m->log.log.begin();
	 i != m->log.log.end();
	 ++i) {
      pmissing.add_next_event(*i);
    }
    pinfo.last_update = m->log.head;

    osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
  }
}

void PG::update_history_from_master(pg_history_t new_history)
{
  unreg_next_scrub();
  info.history.merge(new_history);
  reg_next_scrub();
}

void PG::fulfill_info(
  pg_shard_t from, const pg_query_t &query,
  pair<pg_shard_t, pg_info_t> &notify_info)
{
  assert(from == primary);
  assert(query.type == pg_query_t::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(
  pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
{
  dout(10) << "log request from " << from << dendl;
  assert(from == primary);
  assert(query.type != pg_query_t::INFO);

  MOSDPGLog *mlog = new MOSDPGLog(
    from.shard, pg_whoami.shard,
    get_osdmap()->get_epoch(),
    info, query_epoch);
  mlog->missing = pg_log.get_missing(); // PGLog::missing

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    dout(10) << " sending info+missing+log since " << query.since
	     << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      osd->clog->error() << info.pgid << " got broken pg_query_t::LOG since " << query.since
			<< " when my log.tail is " << pg_log.get_tail()
			<< ", sending full log instead\n";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    dout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  ConnectionRef con = osd->get_con_osd_cluster(
    from.osd, get_osdmap()->get_epoch());
  if (con) {
    osd->share_map_peer(from.osd, con.get(), get_osdmap());
    osd->send_message_osd_cluster(mlog, con.get());
  } else {
    mlog->put();
  }
}


// true if all OSDs in prior intervals may have crashed, and we need to replay
// false positives are okay, false negatives are not.
bool PG::may_need_replay(const OSDMapRef osdmap) const
{
  bool crashed = false;

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) {
    const pg_interval_t &interval = p->second;
    dout(10) << "may_need_replay " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at whether any of the osds during this interval survived
    // past the end of the interval (i.e., didn't crash and
    // potentially fail to COMMIT a write that it ACKed).
    bool any_survived_interval = false;

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];
      if (o == CRUSH_ITEM_NONE)
	continue;

      const osd_info_t *pinfo = 0;
      if (osdmap->exists(o))
	pinfo = &osdmap->get_info(o);

      // does this osd appear to have survived through the end of the
      // interval?
      if (pinfo) {
	if (pinfo->up_from <= interval.first && pinfo->up_thru > interval.last) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " up_thru " << pinfo->up_thru
		   << " survived the interval" << dendl;
	  any_survived_interval = true;
	}
	else if (pinfo->up_from <= interval.first &&
		 (std::find(acting.begin(), acting.end(), o) != acting.end() ||
		  std::find(up.begin(), up.end(), o) != up.end())) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " and is in acting|up,"
		   << " assumed to have survived the interval" << dendl;
	  // (if it hasn't, we will rebuild PriorSet)
	  any_survived_interval = true;
	}
	else if (pinfo->up_from > interval.last &&
		 pinfo->last_clean_begin <= interval.first &&
		 pinfo->last_clean_end > interval.last) {
	  dout(10) << "may_need_replay  prior osd." << o
		   << " up_from " << pinfo->up_from
		   << " and last clean interval ["
		   << pinfo->last_clean_begin << "," << pinfo->last_clean_end
		   << ") survived the interval" << dendl;
	  any_survived_interval = true;
	}
      }
    }

    if (!any_survived_interval) {
      dout(3) << "may_need_replay  no known survivors of interval "
	      << interval.first << "-" << interval.last
	      << ", may need replay" << dendl;
      crashed = true;
      break;
    }
  }

  return crashed;
}

void PG::check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap)
{
  bool changed = false;
  if (osdmap->test_flag(CEPH_OSDMAP_FULL) &&
      !lastmap->test_flag(CEPH_OSDMAP_FULL)) {
    // in current map, osd is full, in last map, osd is not full
    dout(10) << " cluster was marked full in " << osdmap->get_epoch() << dendl;
    changed = true;
  }
  const pg_pool_t *pi = osdmap->get_pg_pool(info.pgid.pool());
  assert(pi);
  if (pi->has_flag(pg_pool_t::FLAG_FULL)) {
    const pg_pool_t *opi = lastmap->get_pg_pool(info.pgid.pool());
    if (!opi || !opi->has_flag(pg_pool_t::FLAG_FULL)) {
      // in current map pool is full, in last map, pool is not full or pool did not exist
      dout(10) << " pool was marked full in " << osdmap->get_epoch() << dendl;
      changed = true;
    }
  }
  
  if (changed) { // osd -> full, or pool -> full
    info.history.last_epoch_marked_full = osdmap->get_epoch();
    dirty_info = true;
  }
}

bool PG::should_restart_peering(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup,
  const vector<int>& newacting,
  OSDMapRef lastmap,
  OSDMapRef osdmap)
{
  if (pg_interval_t::is_new_interval(
	primary.osd,
	newactingprimary,
	acting,
	newacting,
	up_primary.osd,
	newupprimary,
	up,
	newup,
	osdmap,
	lastmap,
	info.pgid.pgid)) {
    dout(20) << "new interval newup " << newup
	     << " newacting " << newacting << dendl;
    return true;
  } else {
    return false;
  }
}

bool PG::old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch)
{
  if (last_peering_reset > reply_epoch ||
      last_peering_reset > query_epoch) {
    dout(10) << "old_peering_msg reply_epoch " << reply_epoch << " query_epoch " << query_epoch
	     << " last_peering_reset " << last_peering_reset
	     << dendl;
    return true;
  }
  return false;
}

void PG::set_last_peering_reset()
{
  dout(20) << "set_last_peering_reset " << get_osdmap()->get_epoch() << dendl;
  if (last_peering_reset != get_osdmap()->get_epoch()) {
    // epoch below this is obsolete, old peering msg/event which carries with
    // an epoch like this can be safely discarded
    last_peering_reset = get_osdmap()->get_epoch();

    // restart buffering outgoing recovery messages, any buffered messages discarded,
    // queue a new QueuePeeringEvt<IntervalFlush> callback if needed, use 
    // IntervalFlush to unblock recovery messages buffering
    // if currently there are ObjectStore Op(s) inprogress, queue a callback
    // of type QueuePeeringEvt<IntervalFlush> on OpSequencer::flush_commit_waiters,
    // and reset RecoveryState::rctx to buffer all outgoing recovery messages in 
    // RecoveryState::messages_pending_flush
    // after current busy Op(s) committed (i.e. journaled) and applied, the callback 
    // will queue a peering event of type IntervalFlush for this pg, only after 
    // the IntervalFlush event is handled will unblock the buffering
    // if we have another peering reset before the callback is called, the callback
    // will detect this is an obsolete peering msg/event, then discard it safely
    reset_interval_flush();
  }
}

struct FlushState {
  PGRef pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
  ~FlushState() {
    pg->lock();
    if (!pg->pg_has_reset_since(epoch))
      pg->queue_flushed(epoch);
    pg->unlock();
  }
};
typedef ceph::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush(ObjectStore::Transaction *t,
		     list<Context *> *on_applied,
		     list<Context *> *on_safe)
{
  // flush in progress ops
  FlushStateRef flush_trigger(
    new FlushState(this, get_osdmap()->get_epoch()));
  
  t->nop(); // in case no pending/previous txn

  // block ReplicatedPG::do_request, will be unblocked the ReplicatedPG::on_flushed
  // which triggered by the callback flush_trigger
  flushes_in_progress++;

  // will trigger when both on_applied and on_safe destroyed
  on_applied->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
  on_safe->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
}

void PG::reset_interval_flush()
{
  dout(10) << "Clearing blocked outgoing recovery messages" << dendl;

  // restart outgoing recovery messages buffering, discard previous buffered 
  // messages(RecoveryState::messages_pending_flush)
  recovery_state.clear_blocked_outgoing();
  
  // when called, the callback queues a peering event(IntervalFlush) for this pg,
  // the previous queued QueuePeeringEvt callback will be called but useless because
  // we update the last_peering_reset in set_last_peering_reset, so when the
  // callback try to queue an IntervalFlush event, it will find that the event
  // is obsolete, thus will discard it silently
  Context *c = new QueuePeeringEvt<IntervalFlush>(
    this, get_osdmap()->get_epoch(), IntervalFlush());
  // PG::osr was created by PG::PG, i.e. pg's ctor, and stored in 
  // OSDService::osr_registry
  if (!osr->flush_commit(c)) {
    // filestore is not idle, i.e. there are inprogress ops, queue the callback
    dout(10) << "Beginning to block outgoing recovery messages" << dendl;

    // reset RecoveryState::rctx, all outgoing recovery messages afterward 
    // buffers in rctx, only after the IntervalFlush event is processed will
    // unblock this buffering
    recovery_state.begin_block_outgoing();
  } else {
    dout(10) << "Not blocking outgoing recovery messages" << dendl;
    delete c;
  }
}

/* Called before initializing peering during advance_map */
void PG::start_peering_interval(
  const OSDMapRef lastmap,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  ObjectStore::Transaction *t)
{
  const OSDMapRef osdmap = get_osdmap(); // new map

  // set PG::last_peering_reset to note that we started a new peering
  // at this epoch, which can be used to block old messages/events,
  // restart interval flush, i.e. queue a new QueuePeeringEvt<IntervalFlush> 
  // callback if needed, and start recovery messages buffering, then when the
  // callback get called, it queue a IntervalFlush peering event for this pg, 
  // use this event to unblock the recovery messages buffering
  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();

  // if we are primary, unreg the next scrub
  unreg_next_scrub();

  // get up_primary, primary (i.e. acting primary), up, acting
  pg_shard_t old_acting_primary = get_primary(); // PG::primary
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary(); // pg_whoami == primary 

  acting.swap(oldacting);
  up.swap(oldup);

  // initialize vector<int> up, vector<int> acting, 
  // pg_shard_t up_primary, pg_shard_t primary, 
  // vector<pg_shard_t> actingset
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  if (info.stats.up != up ||
      info.stats.acting != acting ||
      info.stats.up_primary != new_up_primary ||
      info.stats.acting_primary != new_acting_primary) {
    info.stats.up = up;
    info.stats.up_primary = new_up_primary;
    info.stats.acting = acting;
    info.stats.acting_primary = new_acting_primary;
    info.stats.mapping_epoch = info.history.same_interval_since;
  }

  pg_stats_publish_lock.Lock();
  // reset, checked by OSD::send_pg_stats to determine if we have a valid 
  // pg_stat_t, initialized to false in ctor, sets in PG::publish_stats_to_osd
  // and resets in PG::clear_publish_stats and here
  pg_stats_publish_valid = false; // pg stats now is invalid, PG::publish_stats_to_osd should validate it again
  pg_stats_publish_lock.Unlock();

  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  // set my current role (index in PG::acting set, -1 if the osd we are on is not
  // in the set), for replicated pool because each shard contains the same data,
  // change the index order is not important, so we can use the calculated role 
  // directly, for ec pool the order can not be changed, if the calculated role
  // is different than the previous role (i.e. index or shard), the role is reset
  int role = osdmap->calc_pg_role(osd->whoami, acting, acting.size());
  if (pool.info.is_replicated() || role == pg_whoami.shard) // 
    set_role(role);
  else
    set_role(-1);

  // did acting, up, primary|acker change?
  if (!lastmap) {
    // we are only called by RecoveryState::Reset::react(AdvMap), which can
    // only be called by PG::handle_advance_map, which can only be called by
    // OSD::advance_pg further, so "lastmap" can not be NULL
    dout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
    info.history.same_interval_since = osdmap->get_epoch();
  } else {
    std::stringstream debug;
    // PG::generate_past_intervals in the caller always set it to non-zero 
    // even if it was 0
    assert(info.history.same_interval_since != 0);
    boost::scoped_ptr<IsPGRecoverablePredicate> recoverable(
      get_is_recoverable_predicate());

    // check if we are to change to a new interval, if it is true then construct
    // an instance of pg_interval_t to record the info of the interval, and insert
    // the new interval into info.past_intervals
    bool new_interval = pg_interval_t::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      old_up_primary.osd,
      new_up_primary,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap,
      info.pgid.pgid,
      recoverable.get(),
      &past_intervals,
      &debug);
    dout(10) << __func__ << ": check_new_interval output: "
	     << debug.str() << dendl;
    if (new_interval) {
      // we generate a new interval, so past_intervals and 
      // info.history.same_interval_since both changed
      dout(10) << " noting past " << past_intervals.rbegin()->second << dendl;
      dirty_info = true;
      dirty_big_info = true;

      // prepare for the next interval's start epoch
      info.history.same_interval_since = osdmap->get_epoch();
    }
  }

  if (old_up_primary != up_primary ||
      oldup != up) {
    // update the same up interval start epoch
    info.history.same_up_since = osdmap->get_epoch();
  }
  
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    // update the same primary interval start epoch
    info.history.same_primary_since = osdmap->get_epoch();
  }

  // register the next scrub if we are primary, update the min feature set
  // with other osd(s), then update PG::do_sort_bitwise and may update 
  // ReplicatedPG::object_contexts if PG::do_sort_bitwise changed
  on_new_interval();

  dout(10) << " up " << oldup << " -> " << up 
	   << ", acting " << oldacting << " -> " << acting 
	   << ", acting_primary " << old_acting_primary << " -> " << new_acting_primary
	   << ", up_primary " << old_up_primary << " -> " << new_up_primary
	   << ", role " << oldrole << " -> " << role
	   << ", features acting " << acting_features
	   << " upacting " << upacting_features
	   << dendl;

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_PEERED);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERING);

  peer_missing.clear();
  peer_purged.clear();
  actingbackfill.clear(); // set in PG::choose_acting
  snap_trim_queued = false;
  scrub_queued = false;

  // reset primary state?
  if (was_old_primary || is_primary()) { // last/current primary clear/reset pg_temp
    // remove us from OSD::pg_temp_wanted
    osd->remove_want_pg_temp(info.pgid.pgid);
  }

  // clear peering state, remove us from osd->recovery_wq
  clear_primary_state();
    
  // pg->on_*
  on_change(t); // handle a varity of ops requeue or clear, scrub, cache tier, etc.

  assert(!deleting);

  // should we tell the primary we are here?
  send_notify = !is_primary(); // primary was set in init_primary_up_acting called above

  if (role != oldrole || // index of this osd in PG::acting, of type vector<int>
      was_old_primary != is_primary()) { // PG::pg_whoami == PG::primary, of type pg_shard_t
    // did primary change?
    if (was_old_primary != is_primary()) { // non-primary -> primary or primary -> non-primary
      state_clear(PG_STATE_CLEAN);
      clear_publish_stats();
	
      // take replay queue waiters
      list<OpRequestRef> ls;
      for (map<eversion_t,OpRequestRef>::iterator it = replay_queue.begin();
	   it != replay_queue.end();
	   ++it)
	ls.push_back(it->second);
      replay_queue.clear(); // TODO: ???
      requeue_ops(ls);
    }

    on_role_change(); // clear hit set

    // take active waiters
    requeue_ops(waiting_for_peered); // ReplicatedPG::on_change above always requeue these

  } else {
    // no role change.
    // did primary change?
    if (get_primary() != old_acting_primary) {    
      dout(10) << *this << " " << oldacting << " -> " << acting 
	       << ", acting primary " 
	       << old_acting_primary << " -> " << get_primary() 
	       << dendl;
    } else {
      // primary is the same.
      if (is_primary()) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);
	  
	dout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }

  // call clear_recovery_state to clear a varity of states related with backfill
  // and others
  cancel_recovery();

  if (acting.empty() && !up.empty() && up_primary == pg_whoami) {
    dout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    // clear OSD::pg_temp_wanted
    osd->queue_want_pg_temp(info.pgid.pgid, acting);
  }
}

void PG::on_new_interval()
{
  const OSDMapRef osdmap = get_osdmap();

  // if we are primary pg, register the next scrub
  reg_next_scrub();

  // initialize features
  acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    uint64_t f = osdmap->get_xinfo(*p).features;
    acting_features &= f;
    upacting_features &= f;
  }
  for (vector<int>::iterator p = up.begin(); p != up.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    upacting_features &= osdmap->get_xinfo(*p).features;
  }

  // update PG::do_sort_bitwise
  do_sort_bitwise = get_osdmap()->test_flag(CEPH_OSDMAP_SORTBITWISE);
  if (do_sort_bitwise) {
    assert(get_min_upacting_features() & CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT);
    if (g_conf->osd_debug_randomize_hobject_sort_order) {
      // randomly use a nibblewise sort (when we otherwise might have
      // done bitwise) based on some *deterministic* function such that
      // all peers/osds will agree.
      do_sort_bitwise =
	(info.history.same_interval_since + info.pgid.ps()) & 1;
    }
  }

  // may update ReplicatedPG::object_contexts if PG::do_sort_bitwise changed
  _on_new_interval();
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  assert(!is_primary());

  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();

  if (last_complete_ondisk.epoch >= info.history.last_epoch_started) {
    // DEBUG: verify that the snaps are empty in snap_mapper
    if (cct->_conf->osd_debug_verify_snaps_on_info) {
      interval_set<snapid_t> p;
      p.union_of(oinfo.purged_snaps, info.purged_snaps);
      p.subtract(info.purged_snaps);
      if (!p.empty()) {
	for (interval_set<snapid_t>::iterator i = p.begin();
	     i != p.end();
	     ++i) {
	  for (snapid_t snap = i.get_start();
	       snap != i.get_len() + i.get_start();
	       ++snap) {
	    hobject_t hoid;
	    int r = snap_mapper.get_next_object_to_trim(snap, &hoid);
	    if (r != 0 && r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << dendl;
	      assert(0);
	    } else if (r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << " for object "
		   << hoid << " on snap " << snap
		   << " which should have been fully trimmed " << dendl;
	      assert(0);
	    }
	  }
	}
      }
    }
    info.purged_snaps = oinfo.purged_snaps;
    dirty_info = true;
    dirty_big_info = true;
  }
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info
      << " " << pg.up;
  if (pg.acting != pg.up)
    out << "/" << pg.acting;
  out << " r=" << pg.get_role();
  out << " lpr=" << pg.get_last_peering_reset();

  if (!pg.past_intervals.empty()) {
    out << " pi=" << pg.past_intervals.begin()->first << "-" << pg.past_intervals.rbegin()->second.last
	<< "/" << pg.past_intervals.size();
  }

  if (pg.is_peered()) {
    if (pg.last_update_ondisk != pg.info.last_update)
      out << " luod=" << pg.last_update_ondisk;
    if (pg.last_update_applied != pg.info.last_update)
      out << " lua=" << pg.last_update_applied;
  }

  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  if (pg.pg_log.get_tail() != pg.info.log_tail ||
      pg.pg_log.get_head() != pg.info.last_update)
    out << " (info mismatch, " << pg.pg_log.get_log() << ")";

  if (!pg.pg_log.get_log().empty()) {
    if ((pg.pg_log.get_log().log.begin()->version <= pg.pg_log.get_tail())) {
      out << " (log bound mismatch, actual=["
	  << pg.pg_log.get_log().log.begin()->version << ","
	  << pg.pg_log.get_log().log.rbegin()->version << "]";
      out << ")";
    }
  }

  if (!pg.backfill_targets.empty())
    out << " bft=" << pg.backfill_targets;
  out << " crt=" << pg.pg_log.get_log().can_rollback_to;

  if (pg.last_complete_ondisk != pg.info.last_complete)
    out << " lcod " << pg.last_complete_ondisk;

  if (pg.is_primary()) {
    out << " mlcod " << pg.min_last_complete_ondisk;
  }

  out << " " << pg_state_string(pg.get_state());
  if (pg.should_send_notify())
    out << " NOTIFY";

  if (pg.scrubber.must_repair)
    out << " MUST_REPAIR";
  if (pg.scrubber.auto_repair)
    out << " AUTO_REPAIR";
  if (pg.scrubber.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (pg.scrubber.must_scrub)
    out << " MUST_SCRUB";

  if (!pg.get_sort_bitwise())
    out << " NIBBLEWISE";

  //out << " (" << pg.pg_log.get_tail() << "," << pg.pg_log.get_head() << "]";
  if (pg.pg_log.get_missing().num_missing()) {
    out << " m=" << pg.pg_log.get_missing().num_missing();
    if (pg.is_primary()) {
      int unfound = pg.get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (pg.snap_trimq.size())
    out << " snaptrimq=" << pg.snap_trimq;

  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef& op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());

  if (OSD::op_is_discardable(m)) { // conn has been disconnected and this is not a replay op
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  // Objecter always resend their op whenever the primary changed
  if (m->get_map_epoch() < info.history.same_primary_since) { // primary changed
    dout(7) << " changed after " << m->get_map_epoch()
	    << ", dropping " << *m << dendl;
    return true;
  }

  if (m->get_map_epoch() < pool.info.last_force_op_resend &&
      m->get_connection()->has_feature(CEPH_FEATURE_OSD_POOLRESEND)) { // cache pool's setting changed
    dout(7) << __func__ << " sent before last_force_op_resend "
	    << pool.info.last_force_op_resend << ", dropping" << *m << dendl;
    return true;
  }

  if ((m->get_flags() & (CEPH_OSD_FLAG_BALANCE_READS |
			 CEPH_OSD_FLAG_LOCALIZE_READS)) &&
      op->may_read() &&
      !(op->may_write() || op->may_cache())) { // read op and client allows to read from replicas
    // balanced reads; any replica will do
    if (!(is_primary() || is_replica())) {
      // reply with ENXIO, the client has the correct map, they must calc the
      // right target
      osd->handle_misdirected_op(this, op);
      return true;
    }
  } else {
    // normal case; must be primary
    if (!is_primary()) {
      osd->handle_misdirected_op(this, op);
      return true;
    }
  }
  
  if (is_replay()) { // PG_STATE_REPLAY
    if (m->get_version().version > 0) {
      dout(7) << " queueing replay at " << m->get_version()
	      << " for " << *m << dendl;
      replay_queue[m->get_version()] = op;
      op->mark_delayed("waiting for replay");
      return true;
    }
  }
  return false;
}

template<typename T, int MSGTYPE>
bool PG::can_discard_replica_op(OpRequestRef& op)
{
  T *m = static_cast<T *>(op->get_req());
  assert(m->get_type() == MSGTYPE);

  /* Mostly, this overlaps with the old_peering_msg
   * condition.  An important exception is pushes
   * sent by replicas not in the acting set, since
   * if such a replica goes down it does not cause
   * a new interval. */
  int from = m->get_source().num();
  if (get_osdmap()->get_down_at(from) >= m->map_epoch)
    return true;

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (old_peering_msg(m->map_epoch, m->map_epoch)) {
    dout(10) << "can_discard_replica_op pg changed " << info.history
	     << " after " << m->map_epoch
	     << ", dropping" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_BACKFILL);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old backfill, ignoring" << dendl;
    return true;
  }

  return false;

}

bool PG::can_discard_request(OpRequestRef& op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op); // misdirected op
  case MSG_OSD_SUBOP:
    return can_discard_replica_op<MOSDSubOp, MSG_OSD_SUBOP>(op);
  case MSG_OSD_REPOP:
    return can_discard_replica_op<MOSDRepOp, MSG_OSD_REPOP>(op);
  case MSG_OSD_PG_PUSH:
    return can_discard_replica_op<MOSDPGPush, MSG_OSD_PG_PUSH>(op);
  case MSG_OSD_PG_PULL:
    return can_discard_replica_op<MOSDPGPull, MSG_OSD_PG_PULL>(op);
  case MSG_OSD_PG_PUSH_REPLY:
    return can_discard_replica_op<MOSDPGPushReply, MSG_OSD_PG_PUSH_REPLY>(op);
  case MSG_OSD_SUBOPREPLY:
    return can_discard_replica_op<MOSDSubOpReply, MSG_OSD_SUBOPREPLY>(op);
  case MSG_OSD_REPOPREPLY:
    return can_discard_replica_op<MOSDRepOpReply, MSG_OSD_REPOPREPLY>(op);

  case MSG_OSD_EC_WRITE:
    return can_discard_replica_op<MOSDECSubOpWrite, MSG_OSD_EC_WRITE>(op);
  case MSG_OSD_EC_WRITE_REPLY:
    return can_discard_replica_op<MOSDECSubOpWriteReply, MSG_OSD_EC_WRITE_REPLY>(op);
  case MSG_OSD_EC_READ:
    return can_discard_replica_op<MOSDECSubOpRead, MSG_OSD_EC_READ>(op);
  case MSG_OSD_EC_READ_REPLY:
    return can_discard_replica_op<MOSDECSubOpReadReply, MSG_OSD_EC_READ_REPLY>(op);
  case MSG_OSD_REP_SCRUB:
    return can_discard_replica_op<MOSDRepScrub, MSG_OSD_REP_SCRUB>(op);

  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);
  case MSG_OSD_PG_BACKFILL:
    return can_discard_backfill(op);
  }
  return true;
}

bool PG::op_must_wait_for_map(epoch_t cur_epoch, OpRequestRef& op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDOp*>(op->get_req())->get_map_epoch());

  case MSG_OSD_SUBOP:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDSubOp*>(op->get_req())->map_epoch);

  case MSG_OSD_REPOP:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDRepOp*>(op->get_req())->map_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDSubOpReply*>(op->get_req())->map_epoch);

  case MSG_OSD_REPOPREPLY:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDRepOpReply*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_SCAN:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDPGScan*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_BACKFILL:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDPGBackfill*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PUSH:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDPGPush*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PULL:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDPGPull*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PUSH_REPLY:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDPGPushReply*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_WRITE:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDECSubOpWrite*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_WRITE_REPLY:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDECSubOpWriteReply*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_READ:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDECSubOpRead*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_READ_REPLY:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDECSubOpReadReply*>(op->get_req())->map_epoch);

  case MSG_OSD_REP_SCRUB:
    return !have_same_or_newer_map(
      cur_epoch,
      static_cast<MOSDRepScrub*>(op->get_req())->map_epoch);
  }
  assert(0);
  return false;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;

  // check PG::waiting_for_map op list, may queue us on OSDService::op_wq
  take_op_map_waiters();
  
  for (list<CephPeeringEvtRef>::iterator i = peering_waiters.begin();
       i != peering_waiters.end();
       ++i)
    osd->queue_for_peering(this); // queue us on peering_wq
       
  peering_queue.splice(peering_queue.begin(), peering_waiters,
		       peering_waiters.begin(), peering_waiters.end());
}

void PG::handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx)
{
  dout(10) << "handle_peering_event: " << evt->get_desc() << dendl;
  if (!have_same_or_newer_map(evt->get_epoch_sent())) {
    // pg must have the same or newer map than the evt sent to us, note
    // that OSD::require_same_or_newer_map has a much more paranoid check,
    // refer to: OSD::handle_pg_trim
    
    // this pg has older osdmap than evt
    dout(10) << "deferring event " << evt->get_desc() << dendl;
    peering_waiters.push_back(evt); // wait for newer map
    return;
  }
  if (old_peering_evt(evt)) // evt sent before last_peering_reset, we have started a new interval
    return;

  // ok, use the external peering evt to drive the state machine
  recovery_state.handle_event(evt, rctx);
}

void PG::queue_peering_event(CephPeeringEvtRef evt)
{
  // last_peering_reset is larger than either evt.epoch_requested 
  // or evt.epoch_sent, which means this peering event is useless now
  if (old_peering_evt(evt))
    return;
  peering_queue.push_back(evt);

  // queue pg on OSD::peering_wq
  osd->queue_for_peering(this);
}

// only called in OSD::add_newly_split_pg, OSD::handle_pg_peering_evt and
// OSD::consume_map, they all call it with msg_epoch == query_epoch, so
// CephPeeringEvt always have the members epoch_requested == epoch_sent 
void PG::queue_null(epoch_t msg_epoch,
		    epoch_t query_epoch)
{
  dout(10) << "null" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 NullEvt())));
}

void PG::queue_flushed(epoch_t e)
{
  dout(10) << "flushed" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(e, e,
					 FlushedEvt())));
}

// RecoveryState::Stray::react(MQuery) and RecoveryState::Stray::react(MQuery) will
// handle the MQuery peering evt, so we must be a replica/stray
void PG::queue_query(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     pg_shard_t from, const pg_query_t& q)
{
  dout(10) << "handle_query " << q << " from replica " << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MQuery(from, q, query_epoch))));
}

void PG::handle_advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  RecoveryCtx *rctx)
{
  assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map "
	   << newup << "/" << newacting
	   << " -- " << up_primary << "/" << acting_primary
	   << dendl;

  // update PG::osdmap_ref to next map, so above assertions are always assured
  update_osdmap_ref(osdmap); // update pg's osdmap

  // note down the newly removed snap(s)
  pool.update(osdmap);

  // handle_event is encapsulated as follows: 
  // start_handle(rctx);
  // machine.process_event(evt);
  // end_handle();

  // last map -> new map, the pg has maintained its initial/last internal 
  // states, now we push new info to it
  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);

  // handle the AdvMap event, update info.history.last_epoch_marked_full if
  // osd or pool changed from not full to full, then if in the new map we
  // changed to a new interval, we call pg->start_peering_interval to reset
  // the last peering and start the new peering
  recovery_state.handle_event(evt, rctx);
  
  if (pool.info.last_change == osdmap_ref->get_epoch())
    on_pool_change(); // handle cache pool changes
}

void PG::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  ActMap evt;

  recovery_state.handle_event(evt, rctx);

  // check if we should persist the current epoch and info of the pg
  if (osdmap_ref->get_epoch() - last_persisted_osdmap_ref->get_epoch() >
    cct->_conf->osd_pg_epoch_persisted_max_stale) { // default is 150
    dout(20) << __func__ << ": Dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    dout(20) << __func__ << ": Not dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  
  if (osdmap_ref->check_new_blacklist_entries()) // new blacklist entries added
    check_blacklisted_watchers();
}

void PG::handle_loaded(RecoveryCtx *rctx)
{
  dout(10) << "handle_loaded" << dendl;
  Load evt;
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_create(RecoveryCtx *rctx)
{
  dout(10) << "handle_create" << dendl;
  Initialize evt; // now we are in state Initial
  recovery_state.handle_event(evt, rctx); // ok, boost::statechart::transition< Initialize, Reset >,
  ActMap evt2; // now we are in state Reset
  recovery_state.handle_event(evt2, rctx); // ok, GetInfo or Stray
}

void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  QueryState q(f);
  recovery_state.handle_event(q, 0);
}



std::ostream& operator<<(std::ostream& oss,
			 const struct PG::PriorSet &prior)
{
  oss << "PriorSet[probe=" << prior.probe << " "
      << "down=" << prior.down << " "
      << "blocked_by=" << prior.blocked_by << "]";
  return oss;
}

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
PG::RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Crashed")
{
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
PG::RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Initial")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::Initial::react(const Load& l)
{
  PG *pg = context< RecoveryMachine >().pg;

  // do we tell someone we're here?
  pg->send_notify = (!pg->is_primary()); // if we are not primary, then we need to tell someone

  return transit< Reset >(); // transit to Reset
}

boost::statechart::result PG::RecoveryState::Initial::react(const MNotifyRec& notify)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(
    notify.from, notify.notify.info, notify.notify.epoch_sent);
  pg->update_heartbeat_peers();
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MInfoRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MLogRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PG::RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PG::RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::Started::react(const IntervalFlush&)
{
  dout(10) << "Ending blocked outgoing recovery messages" << dendl;
  context< RecoveryMachine >().pg->recovery_state.end_block_outgoing();
  return discard_event();
}


boost::statechart::result
PG::RecoveryState::Started::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->on_flushed();
  return discard_event();
}


boost::statechart::result PG::RecoveryState::Started::react(const AdvMap& advmap)
{
  dout(10) << "Started advmap" << dendl;
  PG *pg = context< RecoveryMachine >().pg;

  // if osd -> full, or pool -> full, change info.history.last_epoch_marked_full
  // to current map's epoch
  pg->check_full_transition(advmap.lastmap, advmap.osdmap);
  
  if (pg->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) { // we are to change into a new interval
    dout(10) << "should_restart_peering, transitioning to Reset" << dendl;

    // transit into state Reset and continue to handle evt AdvMap which restarts
    // peering process, i.e. start_peering_interval
    post_event(advmap);
    return transit< Reset >();
  }

  // ok, no need to restart peering process

  // iterate each peer in peer_info, and remove down peers from peer_missing, 
  // peer_log_requested, peer_missing_requested, and finally itself from peer_info
  pg->remove_down_peer_info(advmap.osdmap);
  
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Started::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_started_latency, dur);
}

/*--------Reset---------*/
PG::RecoveryState::Reset::Reset(my_context ctx) // note: only ActMap can get me out of Reset
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Reset")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  // initialized in PG::PG, i.e. pg's ctor
  // reset in PG::RecoveryState::Reset::Reset, i.e. when state transits into Reset
  // increased by one in PG::start_flush
  // decreased by one in ReplicatedPG::on_flushed
  pg->flushes_in_progress = 0;
  
  pg->set_last_peering_reset();
}

boost::statechart::result
PG::RecoveryState::Reset::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->on_flushed();
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::Reset::react(const IntervalFlush&)
{
  dout(10) << "Ending blocked outgoing recovery messages" << dendl;

  // populate current rctx with the buffered messages(coveryState::messages_pending_flush)
  context< RecoveryMachine >().pg->recovery_state.end_block_outgoing();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Reset advmap" << dendl;

  // make sure we have past_intervals filled in.  hopefully this will happen
  // _before_ we are active.
  // generate PG's historical intervals between info.history.last_epoch_clean (or created)
  // to info.history.same_interval_since, TODO: for imported PG only ???
  // most of the time the past_intervals is constructed by pg_interval_t::check_new_interval
  // called by PG::start_peering_interval
  // note: normally the info.history.same_interval_since can only be changed by PG::start_peering_interval
  pg->generate_past_intervals(); // note: OSD::load_pgs has built a past_interals for each pg on this osd

  // check if we have transitted from not full to full, includes pool quota 
  // and osd's capacity, if it is true, update info.history.last_epoch_marked_full
  pg->check_full_transition(advmap.lastmap, advmap.osdmap);

  // check if we have to change to a new interval in the new map
  if (pg->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) { // pg mapping changed in new map, we should restart peering
    dout(10) << "should restart peering, calling start_peering_interval again"
	     << dendl;
    // ok, in new map, we need to change to a new interval

    // start new peering interval, reset last peering, restart interal flush,
    // reset all pg internal states and peering, recovery states
    pg->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< RecoveryMachine >().get_cur_transaction());
  }

  // if peer any peer is down then remove it and update heartbeat peers, and
  // all other recovery sources
  pg->remove_down_peer_info(advmap.osdmap);

  // remains in state Reset, ActMap will drive us into state Started and its 
  // substates, i.e. Reset -> Started -> Start -> Primary -> Peering -> GetInfo,
  // or Reset -> Started -> Start -> Stray (then continue to transit by evt 
  // MLogRec/MInfoRec)
  return discard_event(); // remain in Reset, only ActMap can get me out of Reset
}

boost::statechart::result PG::RecoveryState::Reset::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;

  // notify primary osd to create primary PG or update primary PG info,
  // see RecoveryState::GetInfo::react(MNotifyRec)
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    // we are not primary pg(stray or replica) and new primary pg's osd exists, 
    // we need to notify the primary pg
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals);
  }

  pg->update_heartbeat_peers();

  // we updated a bunch of osdmap(s), may handle waiters (including ops and 
  // peering events that are waiting)
  pg->take_waiters();

  return transit< Started >(); // finally, we are to Started
}

boost::statechart::result PG::RecoveryState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Reset::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PG::RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Start")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    // primary get re-set in PG::start_peering_interval whenever we change to a 
    // new interval
    dout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary()); // ok, boost::statechart::transition< MakePrimary, Primary >
  } else { //is_stray
    dout(1) << "transitioning to Stray" << dendl; 
    post_event(MakeStray()); // ok, boost::statechart::transition< MakeStray, Stray >
  }
}

void PG::RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PG::RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->want_acting.empty());

  // set CREATING bit until we have peered for the first time.
  if (pg->info.history.last_epoch_started == 0)
    pg->state_set(PG_STATE_CREATING);
}

boost::statechart::result PG::RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(const ActMap&)
{
  dout(7) << "handle ActMap primary" << dendl;
  PG *pg = context< RecoveryMachine >().pg;

  // publish pg stats, i.e. enqueue pg on OSD::pg_stat_queue
  pg->publish_stats_to_osd();
  
  // requeue pend peering evt and may requeue ops that are waiting for new osdmap
  pg->take_waiters();
  return discard_event();
}

void PG::RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->want_acting.clear();
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_primary_latency, dur);
  pg->clear_primary_state();
  pg->state_clear(PG_STATE_CREATING);
}

/*---------Peering--------*/
PG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_peered());
  assert(!pg->is_peering());
  assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result PG::RecoveryState::Peering::react(const AdvMap& advmap) 
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Peering advmap" << dendl;
  if (prior_set.get()->affected_by_map(advmap.osdmap, pg)) {
    // need to rebuild prior set, return to state Reset, handle a batch of new
    // osdmap updates and then let ActMap evt to drive us to rebuild it again
    dout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    // PriorSet need to be rebuild, transit into state Reset directly, 
    // Peering::prior_set is released thereafter, after handle this osdmap 
    // or more, 1) we may still the primary pg of current interval, then after
    // handle the ActMap evt, we will eventually transit into state GetInfo and
    // rebuild the prior set, 2) we are no longer the primary pg of current interval,
    // i.e. the peering is restarted (PG::start_peering_interval), all previous 
    // peering states cleared, and we changed to state Stray, the new primary
    // pg (may not be on the same osd as the old primary pg) will do the rebuilding
    // thing
    return transit< Reset >();
  }

  // reset need_up_thru if up_thru changed to >= info.history.same_interval_since 
  // in new map, i.e. we are handling a series of osdmaps, in previous build_prior we
  // find that we need to send MOSDAlive, but then we may find in new osdmaps some 
  // other PG may has done the notify thing(MOSDAlive), so we cancel the sending
  pg->adjust_need_up_thru(advmap.osdmap);

  // let state Started to determine if we should restart the peering process, if
  // should then we transit into state Reset, or we update our peer_info
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Peering::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  for (map<epoch_t,pg_interval_t>::iterator p = pg->past_intervals.begin();
       p != pg->past_intervals.end();
       ++p) {
    q.f->open_object_section("past_interval");
    p->second.dump(q.f);
    q.f->close_section();
  }
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<pg_shard_t>::iterator p = prior_set->probe.begin();
       p != prior_set->probe.end();
       ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set->pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set->down.begin();
       p != prior_set->down.end();
       ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set->blocked_by.begin();
       p != prior_set->blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::Peering::exit()
{
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();

  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PG::RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Backfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = true;
  pg->osd->queue_for_recovery(pg);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL);
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const RemoteReservationRejected &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  for (set<pg_shard_t>::iterator it = pg->backfill_targets.begin();
       it != pg->backfill_targets.end();
       ++it) {
    assert(*it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	  MBackfillReserve::REJECT,
	  spg_t(pg->info.pgid.pgid, it->shard),
	  pg->get_osdmap()->get_epoch()),
	con.get());
    }
  }

  pg->osd->recovery_wq.dequeue(pg);

  pg->waiting_on_backfill.clear();
  pg->finish_recovery_op(hobject_t::get_max());

  pg->schedule_backfill_full_retry();
  return transit<NotBackfilling>();
}

void PG::RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_BACKFILL);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PG::RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().remote_shards_to_reserve_backfill.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  if (backfill_osd_it != context< Active >().remote_shards_to_reserve_backfill.end()) {
    //The primary never backfills itself
    assert(*backfill_osd_it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      backfill_osd_it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	MBackfillReserve::REQUEST,
	spg_t(pg->info.pgid.pgid, backfill_osd_it->shard),
	pg->get_osdmap()->get_epoch(),
	pg->get_backfill_priority()),
      con.get());
    }
    ++backfill_osd_it;
  } else {
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremotebackfillreserved_latency, dur);
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  // Send REJECT to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end, next;
  begin = context< Active >().remote_shards_to_reserve_backfill.begin();
  end = context< Active >().remote_shards_to_reserve_backfill.end();
  assert(begin != end);
  for (next = it = begin, ++next ; next != backfill_osd_it; ++it, ++next) {
    //The primary never backfills itself
    assert(*it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	MBackfillReserve::REJECT,
	spg_t(pg->info.pgid.pgid, it->shard),
	pg->get_osdmap()->get_epoch()),
      con.get());
    }
  }

  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->schedule_backfill_full_retry();

  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PG::RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalBackfillReserved()),
    pg->get_backfill_priority());
}

void PG::RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PG::RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/NotBackfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::NotBackfilling::react(const RemoteBackfillReserved &evt)
{
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::NotBackfilling::react(const RemoteReservationRejected &evt)
{
  return discard_event();
}

void PG::RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_notbackfilling_latency, dur);
}

/*---RepNotRecovering----*/
PG::RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepNotRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PG::RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->osd->remote_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      RemoteRecoveryReserved()),
    pg->get_recovery_priority());
}

boost::statechart::result
PG::RecoveryState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

void PG::RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PG::RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  double ratio, max_ratio;

  if (g_conf->osd_debug_reject_backfill_probability > 0 && // default is 0
      (rand()%1000 < (g_conf->osd_debug_reject_backfill_probability*1000.0))) {
    dout(10) << "backfill reservation rejected: failure injection" << dendl;
    post_event(RemoteReservationRejected());
  } else if (pg->osd->too_full_for_backfill(&ratio, &max_ratio) && // osd reached backfill full ratio
      !pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation) { // default is false
    dout(10) << "backfill reservation rejected: full ratio is "
	     << ratio << ", which is greater than max allowed ratio "
	     << max_ratio << dendl;
    post_event(RemoteReservationRejected());
  } else {
    pg->osd->remote_reserver.request_reservation(
      pg->info.pgid,
      new QueuePeeringEvt<RemoteBackfillReserved>(
        pg, pg->get_osdmap()->get_epoch(),
        RemoteBackfillReserved()), evt.priority);
  }
  return transit<RepWaitBackfillReserved>();
}

void PG::RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repwaitbackfillreserved_latency, dur);
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  double ratio, max_ratio;
  if (g_conf->osd_debug_reject_backfill_probability > 0 &&
      (rand()%1000 < (g_conf->osd_debug_reject_backfill_probability*1000.0))) {
    dout(10) << "backfill reservation rejected after reservation: "
	     << "failure injection" << dendl;
    pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
    post_event(RemoteReservationRejected());
    return discard_event();
  } else if (pg->osd->too_full_for_backfill(&ratio, &max_ratio) &&
	     !pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation) {
    dout(10) << "backfill reservation rejected after reservation: full ratio is "
	     << ratio << ", which is greater than max allowed ratio "
	     << max_ratio << dendl;
    pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
    post_event(RemoteReservationRejected());
    return discard_event();
  } else {
    pg->osd->send_message_osd_cluster(
      pg->primary.osd,
      new MBackfillReserve(
	MBackfillReserve::GRANT,
	spg_t(pg->info.pgid.pgid, pg->primary.shard),
	pg->get_osdmap()->get_epoch()),
      pg->get_osdmap()->get_epoch());
    return transit<RepRecovering>();
  }
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PG::RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::RepRecovering::react(const BackfillTooFull &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return discard_event();
}

void PG::RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_RepRecovering_latency, dur);
}

/*------Activating--------*/
PG::RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Activating")
{
  // will transit into Recovered if AllReplicasRecovered evt received
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_activating_latency, dur);
}

PG::RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalRecoveryReserved()),
    pg->get_recovery_priority());
}

void PG::RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

PG::RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    remote_recovery_reservation_it(context< Active >().remote_shards_to_reserve_recovery.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  PG *pg = context< RecoveryMachine >().pg;

  if (remote_recovery_reservation_it != context< Active >().remote_shards_to_reserve_recovery.end()) {
    assert(*remote_recovery_reservation_it != pg->pg_whoami);
  }

  if (remote_recovery_reservation_it != context< Active >().remote_shards_to_reserve_recovery.end()) {
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      remote_recovery_reservation_it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MRecoveryReserve(
	  MRecoveryReserve::REQUEST,
	  spg_t(pg->info.pgid.pgid, remote_recovery_reservation_it->shard),
	  pg->get_osdmap()->get_epoch()),
	con.get());
    }
    ++remote_recovery_reservation_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremoterecoveryreserved_latency, dur);
}

PG::RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Recovering")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_set(PG_STATE_RECOVERING);
  pg->osd->queue_for_recovery(pg);
}

void PG::RecoveryState::Recovering::release_reservations()
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->pg_log.get_missing().have_missing());

  // release remote reservations
  for (set<pg_shard_t>::const_iterator i =
	 context< Active >().remote_shards_to_reserve_recovery.begin();
        i != context< Active >().remote_shards_to_reserve_recovery.end();
        ++i) {
    if (*i == pg->pg_whoami) // skip myself
      continue;
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      i->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MRecoveryReserve(
	  MRecoveryReserve::RELEASE,
	  spg_t(pg->info.pgid.pgid, i->shard),
	  pg->get_osdmap()->get_epoch()),
	con.get());
    }
  }
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<WaitRemoteBackfillReserved>();
}

void PG::RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_recovering_latency, dur);
}

PG::RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  assert(!pg->needs_recovery());

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED | UNDERSIZED is appropriate.
  assert(!pg->actingbackfill.empty());
  if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <=
      pg->actingbackfill.size())
    pg->state_clear(PG_STATE_DEGRADED);

  // adjust acting set?  (e.g. because backfill completed...)
  if (pg->acting != pg->up && !pg->choose_acting(auth_log_shard)) // first time called in RecoveryState::GetLog::GetLog
    assert(pg->want_acting.size());

  // RecoveryState::Active::react(AllReplicasActivated) will set this field to true
  if (context< Active >().all_replicas_activated)
    post_event(GoClean()); // transit into state Clean
}

void PG::RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_recovered_latency, dur);
}

PG::RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Clean")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  if (pg->info.last_complete != pg->info.last_update) {
    assert(0);
  }

  // clear recovering and backfilling intermediate states and populate the 
  // rctx->on_safe->contexts
  pg->finish_recovery(*context< RecoveryMachine >().get_on_safe_context_list());

  // set PG_STATE_CLEAN if acingset size == pool size and acting == up, update 
  // info.history.last_epoch_clean and trim past_intervals up to 
  // info.history.last_epoch_clean
  pg->mark_clean();

  // update peer_info[*], send MOSDPGInfo (including my PG::info and empty 
  // past_intervals) to actingbackfill
  pg->share_pg_info();
  
  pg->publish_stats_to_osd();

}

void PG::RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_clean_latency, dur);
}

template <typename T>
set<pg_shard_t> unique_osd_shard_set(const pg_shard_t & skip, const T &in)
{
  set<int> osds_found;
  set<pg_shard_t> out;
  for (typename T::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (*i != skip && !osds_found.count(i->osd)) {
      osds_found.insert(i->osd);
      out.insert(*i);
    }
  }
  return out;
}

/*---------Active---------*/
PG::RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active"),
    remote_shards_to_reserve_recovery( // const set<pg_shard_t>
      unique_osd_shard_set(
	context< RecoveryMachine >().pg->pg_whoami, // skip
	context< RecoveryMachine >().pg->actingbackfill)),
    remote_shards_to_reserve_backfill( // const set<pg_shard_t>
      unique_osd_shard_set(
	context< RecoveryMachine >().pg->pg_whoami, // skip
	context< RecoveryMachine >().pg->backfill_targets)),
    all_replicas_activated(false)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  assert(!pg->backfill_reserving);
  assert(!pg->backfill_reserved);
  assert(pg->is_primary());
  dout(10) << "In Active, about to call activate" << dendl;

  // flush current pending and all previous txn(s) and prevent OSD::osd_op_tp from
  // handling client I/O
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());

  // send MOSDPGInfo, build missing
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(), // t
	       pg->get_osdmap()->get_epoch(), // activation_epoch
	       *context< RecoveryMachine >().get_on_safe_context_list(), // tfin
	       *context< RecoveryMachine >().get_query_map(), // query_map
	       context< RecoveryMachine >().get_info_map(), // activator_map
	       context< RecoveryMachine >().get_recovery_ctx()); // ctx

  // everyone has to commit/ack before we are truly active
  pg->blocked_by.clear();
  for (set<pg_shard_t>::iterator p = pg->actingbackfill.begin();
       p != pg->actingbackfill.end();
       ++p) {
    if (p->shard != pg->pg_whoami.shard) {
      pg->blocked_by.insert(p->shard);
    }
  }
  pg->publish_stats_to_osd();
  dout(10) << "Activate Finished" << dendl;

  // ok, we are to transit into the substate Activating
}

boost::statechart::result PG::RecoveryState::Active::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  
  if (!pg->pool.newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool.newly_removed_snaps);
    dout(10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
    pg->dirty_big_info = true;
  }

  for (size_t i = 0; i < pg->want_acting.size(); i++) { // TODO: what's the intent ???
    int osd = pg->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      assert(pg->is_acting(osd_with_shard) || pg->is_up(osd_with_shard));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(pg->info.pgid.pgid) !=
      pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid)) { // pool size changed in new map
    if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <= pg->actingset.size()) {
      // pool size changed to smaller
      pg->state_clear(PG_STATE_UNDERSIZED);
      
      if (pg->needs_recovery()) { // missing objects in this pg (including all shards)
	pg->state_set(PG_STATE_DEGRADED);
      } else {
	pg->state_clear(PG_STATE_DEGRADED);
      }
    } else { // pool size changed to bigger
      pg->state_set(PG_STATE_UNDERSIZED);
      pg->state_set(PG_STATE_DEGRADED);
    }
    pg->publish_stats_to_osd(); // degraded may have changed
  }

  // if we haven't reported our PG stats in a long time, do so now.
  if (pg->info.stats.reported_epoch + pg->cct->_conf->osd_pg_stat_report_interval_max < advmap.osdmap->get_epoch()) { // default is 500
    dout(20) << "reporting stats to osd after " << (advmap.osdmap->get_epoch() - pg->info.stats.reported_epoch)
	     << " epochs" << dendl;
    pg->publish_stats_to_osd();
  }

  // let RecoveryState::Started::react(AdvMap) handle it, we may need to start a 
  // new interval
  return forward_event();
}
    
boost::statechart::result PG::RecoveryState::Active::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_primary());

  if (pg->have_unfound()) { // PG::activate has done this once, with new map we do it again
    // iterate each shard in PG::might_have_unfound to find if we can recover 
    // the unfound objects from new peers
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (pg->cct->_conf->osd_check_for_log_corruption) // default is false
    pg->check_log_for_corruption(pg->osd->store); // do nothing

  int unfound = pg->missing_loc.num_unfound();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) { // we have tried all recovery sources and no lucky
    if (pg->cct->_conf->osd_auto_mark_unfound_lost) { // default is false
      pg->osd->clog->error() << pg->info.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically marking lost but NOT IMPLEMENTED\n";
      //pg->mark_all_unfound_lost(*context< RecoveryMachine >().get_cur_transaction());
    } else
      pg->osd->clog->error() << pg->info.pgid << " has " << unfound << " objects unfound and apparently lost\n";
  }

  if (!pg->snap_trimq.empty() &&
      pg->is_clean()) {
    dout(10) << "Active: queuing snap trim" << dendl;
    pg->queue_snap_trim();
  }

  if (!pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL) &&
      (!pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) || pg->is_degraded())) {
    // finally, queue the pg on OSDService::recovery_wq
    pg->osd->queue_for_recovery(pg);
  }

  // let RecoveryState::Primary::react(ActMap) have a chance to handle it, i.e.
  // publish pg stats and take_waiters
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MNotifyRec& notevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", already have info from that osd, ignoring" 
	     << dendl;
  } else if (pg->peer_purged.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from
	     << ", already purged that peer, ignoring"
	     << dendl;
  } else {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", calling proc_replica_info and discover_all_missing"
	     << dendl;
    pg->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);

    // TODO: we have queried all shards that may have our unfound objects in PG::activate, why we have to do it again ???
    if (pg->have_unfound()) { // have missing objects that can not be recovered
      // iterate each PG::might_have_unfound to request FULLLOG
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_primary());

  assert(!pg->actingbackfill.empty());
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_actingbackfill(infoevt.from)) { // the peer is in PG::actingbackfill set
    // the peer told us that they have activated
    dout(10) << " peer osd." << infoevt.from << " activated and committed" 
	     << dendl;
    pg->peer_activated.insert(infoevt.from);
    pg->blocked_by.erase(infoevt.from.shard);
    pg->publish_stats_to_osd();
    
    if (pg->peer_activated.size() == pg->actingbackfill.size()) {
      // queue peering evt AllReplicasActivated, note: we are in substate 
      // Activating current now, and will handle the queued AllReplicasActivated
      // evt in substate Activating, and while substate Activating can't handle
      // it, state Active will handle it eventually
      pg->all_activated_and_committed();
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_log(
    *context<RecoveryMachine>().get_cur_transaction(),
    logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);

  // check if the peer can be used as a recovery source, and if the peer can
  // be used as the recovery source, then prepare MOSDPGInfo for it 
  bool got_missing = pg->search_for_missing(
    pg->peer_info[logevt.from],
    pg->peer_missing[logevt.from],
    logevt.from,
    context< RecoveryMachine >().get_recovery_ctx());
  if (got_missing) // found a recovery source for missing objects
    pg->osd->queue_for_recovery(pg);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<pg_shard_t>::iterator p = pg->might_have_unfound.begin();
	 p != pg->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (pg->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (pg->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!pg->get_osdmap()->is_up(p->osd)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    pg->dump_recovery_info(q.f);
    q.f->close_section();
  }

  {
    q.f->open_object_section("scrub");
    q.f->dump_stream("scrubber.epoch_start") << pg->scrubber.epoch_start;
    q.f->dump_int("scrubber.active", pg->scrubber.active);
    q.f->dump_int("scrubber.waiting_on", pg->scrubber.waiting_on);
    {
      q.f->open_array_section("scrubber.waiting_on_whom");
      for (set<pg_shard_t>::iterator p = pg->scrubber.waiting_on_whom.begin();
	   p != pg->scrubber.waiting_on_whom.end();
	   ++p) {
	q.f->dump_stream("shard") << *p;
      }
      q.f->close_section();
    }
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

// PG::all_activated_and_committed queued the peering evt AllReplicasActivated,
// actually now we are in substate Activating but the substate can not handle the
// evt, so the parent state, i.e. Active, handles it
boost::statechart::result PG::RecoveryState::Active::react(const AllReplicasActivated &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  // used for RecoveryState::Recovered::Recovered to determine if we need to
  // queue an internal evt GoClean 
  all_replicas_activated = true;

  pg->state_clear(PG_STATE_ACTIVATING);
  pg->state_clear(PG_STATE_CREATING);
  if (pg->acting.size() >= pg->pool.info.min_size) {
    pg->state_set(PG_STATE_ACTIVE);
  } else {
    pg->state_set(PG_STATE_PEERED);
  }

  // info.last_epoch_started is set during activate()
  pg->info.history.last_epoch_started = pg->info.last_epoch_started;

  pg->share_pg_info();
  pg->publish_stats_to_osd();

  pg->check_local(); // debug use only

  // waiters
  if (pg->flushes_in_progress == 0) {
    pg->requeue_ops(pg->waiting_for_peered);
  }

  // queue peering evt DoRecovery/RequestBackfill/AllReplicasRecovered accordingly,
  // note: current now we are still in state Activating, so the pending peering
  // evt is to be handled by state Activating, i.e.
  // boost::statechart::transition< AllReplicasRecovered, Recovered >,
  // boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
  // boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved >
  pg->on_activate();

  return discard_event();
}

void PG::RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  pg->blocked_by.clear();
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_ACTIVATING);
  pg->state_clear(PG_STATE_DEGRADED);
  pg->state_clear(PG_STATE_UNDERSIZED);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_REPLAY);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_active_latency, dur);
  pg->agent_stop();
}

/*------ReplicaActive-----*/
PG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}


boost::statechart::result PG::RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  map<int, map<spg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(), // t
	       actevt.activation_epoch, // activation_epoch
	       *context< RecoveryMachine >().get_on_safe_context_list(), // tfin
	       query_map, // query_map
	       NULL, // activator_map
	       NULL); // ctx
  dout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  pg->merge_log(*t,logevt.msg->info, logevt.msg->log, logevt.from);
  assert(pg->pg_log.get_head() == pg->info.last_update);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;

  // we only handle pg_query_t::MISSING here, pg_query_t::INFO, pg_query_t::LOG, 
  // pg_query_t::FULLLOG are handled in RecoveryState::Stray::react(MQuery)
  
  if (query.query.type == pg_query_t::MISSING) {
    // update PG::info.history, i.e. merge with primary shard's pg history
    pg->update_history_from_master(query.query.history);

    // send MOSDPGLog with PGLog::missing
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  } // else: from prior to activation, safe to ignore
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_replicaactive_latency, dur);
}

/*-------Stray---*/
PG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Stray")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_peered());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}

// primary shard of this pg sends us MOSDPGLog in PG::activate if the
// replica has less pg log than the primary to drive us into state ReplicaActive
boost::statechart::result PG::RecoveryState::Stray::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  dout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  if (msg->info.last_backfill == hobject_t()) {
    if (!(msg->get_connection()->get_features() & CEPH_FEATURE_OSD_MIN_SIZE_RECOVERY)) {
      dout(10) << "Got logevt resetting backfill from peer featuring bug"
	       << " 10780, setting msg->info.last_epoch_started to logevt.query_epoch,"
	       << " which is the activation epoch." << dendl;
      msg->info.last_epoch_started = msg->get_query_epoch();
    }
    // restart backfill
    pg->unreg_next_scrub();
    pg->info = msg->info;
    pg->reg_next_scrub();
    pg->dirty_info = true;
    pg->dirty_big_info = true;  // maybe.

    PGLogEntryHandler rollbacker;
    pg->pg_log.claim_log_and_clear_rollback_info(msg->log, &rollbacker);
    rollbacker.apply(pg, t);

    pg->pg_log.reset_backfill();
  } else {
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

// 1) primary shard of this pg sends us MOSDPGInfo in PG::activate if the primary
// has no pg log to send, i.e. we have the same pg log as the primary shard,
// 2) or primary shard of this pg prepares MOSDPGLog in PG::search_for_missing,
// to drive us into state ReplicaActive
boost::statechart::result PG::RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->info.last_update > infoevt.info.last_update) { // we have divergent log entries
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    pg->info.stats = infoevt.info.stats;
    pg->info.hit_set = infoevt.info.hit_set;
  }
  
  assert(infoevt.info.last_update == pg->info.last_update);
  assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(infoevt.info.last_epoch_started));
  return transit<ReplicaActive>();
}

// the primary shard send us the pg_query_t::INFO in 
boost::statechart::result PG::RecoveryState::Stray::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;

  // we only handle pg_query_t::INFO, pg_query_t::LOG, pg_query_t::FULLLOG here,
  // the pg_query_t::MISSING is handled in RecoveryState::ReplicaActive::react(MQuery)
  
  if (query.query.type == pg_query_t::INFO) { // pg_query_t::INFO
    pair<pg_shard_t, pg_info_t> notify_info;
    // update PG::info.history, i.e. merge with primary shard's pg history
    pg->update_history_from_master(query.query.history);

    // set notify_info to <query.from, PG::info>
    pg->fulfill_info(query.from, query.query, notify_info);

    // prepare notify, multiple pg shards (may belong to different pgs) on the 
    // same OSD can be sent bundled with only one MOSDPGNotify msg
    context< RecoveryMachine >().send_notify(
      notify_info.first,
      pg_notify_t(
	notify_info.first.shard, pg->pg_whoami.shard,
	query.query_epoch,
	pg->get_osdmap()->get_epoch(),
	notify_info.second), // PG::info
      pg->past_intervals); // PG::past_intervals
  } else { // pg_query_t::LOG, pg_query_t::FULLLOG
    // send MOSDPGLog with info + missing + log/fulllog 
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  }
  return discard_event();
}

// Stray is substate of Started, so the AdvMap is handled by Started
boost::statechart::result PG::RecoveryState::Stray::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) { // always send a MOSDPGNotity to current primary PG
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals); // PG info + past_intervals
  }
  pg->take_waiters();

  // remain in state Stray, can only be changed by evt MLogRec/MInfoRec
  return discard_event();
}

void PG::RecoveryState::Stray::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_stray_latency, dur);
}

/*--------GetInfo---------*/
PG::RecoveryState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetInfo")
{
  // got here means we are to start a new peering interval, everything starts
  // from new, all previous peering interval's imtermediate states are lost
        
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  // h.same_interval_since can only be changed by OSD::build_past_intervals_parallel(load), 
  // OSD::project_pg_history(create) and PG::start_peering_interval(advance osdmap)
  // TODO: why we need this ???
  pg->generate_past_intervals();
  
  unique_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  // PriorSet and PG both have a member named blocked_by
  assert(pg->blocked_by.empty());

  if (!prior_set.get()) // the first time we transit into
    pg->build_prior(prior_set); // build prior set by past intervals

  pg->reset_min_peer_features();

  // iterate prior set to prepare queries
  get_infos();

  // PriorSet::pg_down is set when there is not enough osds survived the 
  // interval, and we may have gone rw, refer to PriorSet::PriorSet,
  // PriorSet::pg_down never reset to false explicitly, we can only
  // set it to false by rebuilding the PriorSet (refer to RecoveryState::Peering::react(AdvMap))
  if (peer_info_requested.empty() && !prior_set->pg_down) {
    // newly created PG instance with pool size set to 1 or we got all queries 
    // replied (refer to RecoveryState::GetInfo::react(MNotifyRec))
    post_event(GotInfo()); // silently transit into state GetLog
  }
}

// only be called by RecoveryState::GetInfo::GetInfo and 
// RecoveryState::GetInfo::react(MNotifyRec) to query pg_query_t::INFO for
// pgs in prior set
void PG::RecoveryState::GetInfo::get_infos()
{
  PG *pg = context< RecoveryMachine >().pg;
  // we have built the prior set in RecoveryState::GetInfo::GetInfo
  unique_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  pg->blocked_by.clear();
  for (set<pg_shard_t>::const_iterator it = prior_set->probe.begin();
       it != prior_set->probe.end();
       ++it) { // iterate each probe target, i.e. PG replica
    pg_shard_t peer = *it;
    if (peer == pg->pg_whoami) { // skip myself
      continue;
    }
    if (pg->peer_info.count(peer)) { // we have queried and got a reply
      dout(10) << " have osd." << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }

    // we are blocked by those osds that we have sent query request and has not get a reply
    
    if (peer_info_requested.count(peer)) { // have queried, waiting for reply
      dout(10) << " already requested info from osd." << peer << dendl;
      pg->blocked_by.insert(peer.osd); // sent query and has not been replied
    } else if (!pg->get_osdmap()->is_up(peer.osd)) { // peer is down
      dout(10) << " not querying info from down osd." << peer << dendl;
    } else { // have not query yet, prepare to query
      dout(10) << " querying info from osd." << peer << dendl;
      context< RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO, // request pg info
			 it->shard, // to
			 pg->pg_whoami.shard, // from
			 pg->info.history, // our history
			 pg->get_osdmap()->get_epoch())); // epoch_sent
			 
      // mark we have prepared the query for this peer
      peer_info_requested.insert(peer);
      pg->blocked_by.insert(peer.osd); // new query to send
    }
  }

  pg->publish_stats_to_osd();
}

//struct pg_history_t {
//  epoch_t epoch_created;       // epoch in which PG was created
//  epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)
//  epoch_t last_epoch_clean;    // lower bound on last epoch the PG was completely clean.
//  epoch_t last_epoch_split;    // as parent
//  epoch_t last_epoch_marked_full;  // pool or cluster
//
//  /**
//   * In the event of a map discontinuity, same_*_since may reflect the first
//   * map the osd has seen in the new map sequence rather than the actual start
//   * of the interval.  This is ok since a discontinuity at epoch e means there
//   * must have been a clean interval between e and now and that we cannot be
//   * in the active set during the interval containing e.
//   */
//  epoch_t same_up_since;       // same acting set since
//  epoch_t same_interval_since;   // same acting AND up set since
//  epoch_t same_primary_since;  // same primary at least back through this epoch.
//
//  eversion_t last_scrub;
//  eversion_t last_deep_scrub;
//  utime_t last_scrub_stamp;
//  utime_t last_deep_scrub_stamp;
//  utime_t last_clean_scrub_stamp;
//}
boost::statechart::result PG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  PG *pg = context< RecoveryMachine >().pg;

  set<pg_shard_t>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) { // ok we got this peer's reply
    peer_info_requested.erase(p);
    pg->blocked_by.erase(infoevt.from.osd); // got a reply, the peer has a osdmap in the same interval as us
  }

  // this MNotifyRec may be not a reply for our active query, coz we (PG) may have not
  // been on this OSD before, this message drive us to be created, see OSD::handle_pg_peering_evt
  // if we have been on this OSD before, then the MNotifyRec must be in the
  // same interval as only, or we will drop this message, see OSD::handle_pg_peering_evt

  // PG::info.history.last_epoch_started is set in three places:
  // 1. RecoveryState::Active::react(AllReplicasActivated), for primary replica, see PG::_activate_committed
  // 2. PG::_activate_committed, for other replicas, see PG::_activate_committed
  // 3. PG::append_log, for other replicas
  epoch_t old_start = pg->info.history.last_epoch_started;
  
  // check if we can get something new from peers or prior set, if yes then merge PG::info.history
  if (pg->proc_replica_info(
	infoevt.from, infoevt.notify.info, infoevt.notify.epoch_sent)) { // got a new peer or old peer's new info
    // we got something new ...
    unique_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

    if (old_start < pg->info.history.last_epoch_started) { // peer has a updated pg history
      dout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;

      // rebuild prior set
      pg->build_prior(prior_set);

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<pg_shard_t>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) { // we can transit into next state only if peer_info_requested is empty
	if (prior_set->probe.count(*p) == 0) { // this OSD not in new prior set any more
	  dout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }

      // re-prepare pg_query_t::INFO queries to send for pgs in new PriorSet::probe 
      // set, coz we have rebuilt the prior set above
      get_infos();
    }

    dout(20) << "Adding osd: " << infoevt.from.osd << " peer features: "
      << hex << infoevt.features << dec << dendl;
    pg->apply_peer_features(infoevt.features);

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set->pg_down) {
      // ok, we got replied for all previous queries and we can survive every 
      // interval of PG::past_intervals
      /*
       * make sure we have at least one !incomplete() osd from the
       * last rw interval.  the incomplete (backfilling) replicas
       * get a copy of the log, but they don't get all the object
       * updates, so they are insufficient to recover changes during
       * that interval.
       */
      if (pg->info.history.last_epoch_started) { // last peering process finished
	for (map<epoch_t,pg_interval_t>::reverse_iterator p = pg->past_intervals.rbegin();
	     p != pg->past_intervals.rend();
	     ++p) { // ietarate past_intervals in reverse order
	  if (p->first < pg->info.history.last_epoch_started) // ok, have peered already
	    break;
	  if (!p->second.maybe_went_rw) // no write during this interval
	    continue;

          // ok, we have gone write during this interval
          
	  pg_interval_t& interval = p->second;
	  dout(10) << " last maybe_went_rw interval was " << interval << dendl;
	  OSDMapRef osdmap = pg->get_osdmap();

	  /*
	   * this mirrors the PriorSet calculation: we wait if we
	   * don't have an up (AND !incomplete) node AND there are
	   * nodes down that might be usable.
	   */
	  bool any_up_complete_now = false;
	  bool any_down_now = false;
	  for (unsigned i=0; i<interval.acting.size(); i++) {
	    int o = interval.acting[i];
	    if (o == CRUSH_ITEM_NONE)
	      continue;
            
	    pg_shard_t so(o, pg->pool.info.ec_pool() ? shard_id_t(i) : shard_id_t::NO_SHARD);
	    if (!osdmap->exists(o) || osdmap->get_info(o).lost_at > interval.first)
	      continue;  // dne or lost, we let the peering process continue
	      
	    if (osdmap->is_up(o)) { // up
	      pg_info_t *pinfo;
	      if (so == pg->pg_whoami) {
		pinfo = &pg->info;
	      } else {
		assert(pg->peer_info.count(so));
		pinfo = &pg->peer_info[so]; // peer info updated in PG::proc_replica_info
	      }

              // check if any peer has backfilled, i.e. not backfilling
	      if (!pinfo->is_incomplete())
		any_up_complete_now = true; // not backfilling, we are complete
	    } else { // peer is down
	      any_down_now = true;
	    }
	  }
          
	  if (!any_up_complete_now && any_down_now) {
            // during this interval we have no complete replicas (primary included),
            // and any useful osd in this interval currently is down, so we can not
            // continue the peering process
	    dout(10) << " no osds up+complete from interval " << interval << dendl;
	    pg->state_set(PG_STATE_DOWN);
	    return discard_event();
	  }

          // we can survive the latest interval that we have written something to,
          // so we can survive any interval, coz we can write to a pg only after
          // it has finished its previous peering process
	  break;
	}
      }
      dout(20) << "Common peer features: " << hex << pg->get_min_peer_features() << dec << dendl;
      dout(20) << "Common acting features: " << hex << pg->get_min_acting_features() << dec << dendl;
      dout(20) << "Common upacting features: " << hex << pg->get_min_upacting_features() << dec << dendl;

      // silently transit into state GetLog
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<pg_shard_t>::iterator p = peer_info_requested.begin();
       p != peer_info_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (pg->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      pg->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetInfo::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getinfo_latency, dur);
  pg->blocked_by.clear(); // ok, no blocker
}

/*------GetLog------------*/

// state GetLog can only be transitted from state GetInfo by evt GotInfo
PG::RecoveryState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetLog"),
    msg(0) // boost::intrusive_ptr<MOSDPGLog>
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  // ok, we are transitted from state GetInfo

  // adjust acting?
  if (!pg->choose_acting(auth_log_shard)) {
    // we need to adjust the acting set or the want_acting set is not recoverable
    if (!pg->want_acting.empty()) { // we want to change acting set, so wait new map
      post_event(NeedActingChange()); // transit into state WaitActingChange
    } else { // the want_acting set is not recoverable
      post_event(IsIncomplete()); // transit into state Incomplete
    }
    return;
  }

  // want_acting set is the same as current acting set, no need to wait mon
  // to change the acting set

  // am i the best?
  if (auth_log_shard == pg->pg_whoami) { // i has the authority log, no need to request from others
    post_event(GotLog()); // got the authority pg log (we are the authority)
    return;
  }

  // ok, i am not the best pg that has the best pg info, we will request pg
  // log from peer

  const pg_info_t& best = pg->peer_info[auth_log_shard]; // this peer has the best pg info

  // am i broken?
  if (pg->info.last_update < best.log_tail) {
    dout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete()); // transit into state InComplete
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->info.last_update;
  assert(!pg->actingbackfill.empty()); // set in PG::choose_acting
  for (set<pg_shard_t>::iterator p = pg->actingbackfill.begin();
       p != pg->actingbackfill.end();
       ++p) {
    if (*p == pg->pg_whoami) continue;
    pg_info_t& ri = pg->peer_info[*p];
    if (ri.last_update >= best.log_tail && ri.last_update < request_log_from)
      request_log_from = ri.last_update; // the min info.last_update
  }

  // prepare to request authority pg log
  dout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<RecoveryMachine>().send_query(
    auth_log_shard,
    pg_query_t(
      pg_query_t::LOG, // request authority pg log from the peer
      auth_log_shard.shard, pg->pg_whoami.shard,
      request_log_from, pg->info.history,
      pg->get_osdmap()->get_epoch()));

  assert(pg->blocked_by.empty());
  pg->blocked_by.insert(auth_log_shard.osd);
  pg->publish_stats_to_osd(); // blocked_by changed
}

boost::statechart::result PG::RecoveryState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) { // TODO: why we need to check explictily ???
    dout(10) << "GetLog: auth_log_shard osd."
	     << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const MLogRec& logevt)
{
  assert(!msg); // boost::intrusive_ptr<MOSDPGLog>, a member variable of GetLog
  
  if (logevt.from != auth_log_shard) {
    dout(10) << "GetLog: discarding log from "
	     << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }
  dout(10) << "GetLog: received master log from osd"
	   << logevt.from << dendl;
  
  msg = logevt.msg; // got authority pg log
  
  post_event(GotLog()); // actually, GotAuthorityLog is more accurate
  return discard_event();
}

// got pg authority log
boost::statechart::result PG::RecoveryState::GetLog::react(const GotLog&)
{
  dout(10) << "leaving GetLog" << dendl;
  PG *pg = context< RecoveryMachine >().pg;

  // if us, i.e. primary, has the authority pg log, RecoveryState::GetLog::GetLog 
  // will post a GotLog evt without having a MLogRec evt
  if (msg) {
    dout(10) << "processing master log" << dendl;

    // ok, we are to handle the authority log, merge the authority log into
    // us, update our missing and info
    pg->proc_master_log(*context<RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, 
			msg->missing, // read only use
			auth_log_shard);
  }

  // flush current pending and all previous txn(s) and prevent OSD::osd_op_tp from
  // handling client I/O
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(), // RecoveryState::rctx->transaction
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
  
  return transit< GetMissing >();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_stream("auth_log_shard") << auth_log_shard;
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetLog::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getlog_latency, dur);
  pg->blocked_by.clear();
  pg->publish_stats_to_osd();
}

/*------WaitActingChange--------*/
PG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/WaitActingChange")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  dout(10) << "verifying no want_acting " << pg->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->want_acting.begin(); p != pg->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) { // osd in want_acting changed to down
      dout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }

  // handle AdvMap in state Peering and may up to Started, after all these handling
  // we may transit into state Reset to start a new peering interval (we are waiting
  // the acting set to change, i.e. we are waiting for a new interval) or remains in 
  // state WaitActingChange
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitActingChange::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitactingchange_latency, dur);
}

/*------Incomplete--------*/
PG::RecoveryState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/Incomplete")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);

  unique_ptr<PriorSet> &prior_set = context< Peering >().prior_set;
  assert(pg->blocked_by.empty());
  pg->blocked_by.insert(prior_set->down.begin(), prior_set->down.end());
  pg->publish_stats_to_osd();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const AdvMap &advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  int64_t poolnum = pg->info.pgid.pool();

  // Reset if min_size changed, pg might now be able to go active
  if (advmap.lastmap->get_pools().find(poolnum)->second.min_size !=
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const MNotifyRec& notevt) {
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
    return discard_event();
  } else {
    // update peer's pg info and merge their pg history into us
    pg->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
    
    // try again!
    
    // we can only transit into state Incomplete from RecoveryState::GetLog::GetLog,
    // now we transit back to state GetLog to choose_acting again
    return transit< GetLog >();
  }
}

void PG::RecoveryState::Incomplete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_incomplete_latency, dur);

  pg->blocked_by.clear();
}

/*------GetMissing--------*/
PG::RecoveryState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetMissing")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->actingbackfill.empty());

  // PG::choose_acting will set the PG::actingbackfill
  for (set<pg_shard_t>::iterator i = pg->actingbackfill.begin();
       i != pg->actingbackfill.end();
       ++i) { // iterate each actingbackfill
    if (*i == pg->get_primary()) continue; // if want_acting is not empty, we may be a temp primary
    
    const pg_info_t& pi = pg->peer_info[*i];

    if (pi.is_empty()) // OSD::handle_pg_query may reply us with empty pg info
      continue; // no pg data, nothing divergent

    if (pi.last_update < pg->pg_log.get_tail()) {
      dout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->peer_missing[*i];
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      dout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->peer_missing[*i];
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i];
      continue;
    }

    // ok, now we prepare to request pg log from peer to update our PG::peer_info 
    // and PG::peer_missing

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    eversion_t since(pi.last_epoch_started, 0);
    assert(pi.last_update >= pg->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      dout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryMachine >().send_query(
	*i,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, pg->pg_whoami.shard,
	  since, pg->info.history,
	  pg->get_osdmap()->get_epoch()));
    } else {
      dout(10) << " requesting fulllog+missing from osd." << *i
	       << " (want since " << since << " < log.tail " << pi.log_tail << ")"
	       << dendl;
      context< RecoveryMachine >().send_query(
	*i, 
	pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, pg->pg_whoami.shard,
	  pg->info.history, pg->get_osdmap()->get_epoch()));
    }
    
    peer_missing_requested.insert(*i);
    pg->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) { // PG::need_up_thru is set in PG::build_prior
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru()); // transit into state WaitUpThru
      return;
    }

    // all good!
    post_event(Activate(pg->get_osdmap()->get_epoch())); // transit into state Active
  } else {
    pg->publish_stats_to_osd();
  }
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from); // got a MLogRec from peer

  // process the MLogRec the peer sent to us (we sent they queries for their pg
  // log to construct their missing map in RecoveryState::GetMissing::GetMissing) 
  // to update their missing map and pg info
  pg->proc_replica_log(*context<RecoveryMachine>().get_cur_transaction(),
		       logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) { // need to inform mon about our aliveness in OSDMap
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru()); // transit into state WaitUpThru
    } else {
      dout(10) << "Got last missing, don't need missing "
	       << "posting CheckRepops" << dendl;
      post_event(Activate(pg->get_osdmap()->get_epoch())); // transit into state Active
    }
  }

  // still more MLogRec to wait from peers
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<pg_shard_t>::iterator p = peer_missing_requested.begin();
       p != peer_missing_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (pg->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      pg->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetMissing::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getmissing_latency, dur);
  pg->blocked_by.clear();
  pg->publish_stats_to_osd();
}

/*------WaitUpThru--------*/
PG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/WaitUpThru")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const ActMap& am)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->need_up_thru) { // our OSDMap::up_thru passed info.history.same_interval_since
    // ok, 1) OSD::queue_want_up_thru has sent the MOSDAlive to mon (called in 
    // OSD::process_peering_events) and PG::adjust_need_up_thru has set
    // PG::need_up_thru to false or
    // 2) PG::build_prior did not set PG::need_up_thru to true
    post_event(Activate(pg->get_osdmap()->get_epoch())); // transit into state Active
  }
  return forward_event();
}

// TODO: why we can receive the MLogRec, we have handled all the requested 
// MLogRec in RecoveryState::GetMissing::react(MLogRec) ???
boost::statechart::result PG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "Noting missing from osd." << logevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->peer_missing[logevt.from].swap(logevt.msg->missing);
  pg->peer_info[logevt.from] = logevt.msg->info;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitUpThru::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitupthru_latency, dur);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void PG::RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  dout(5) << "enter " << state_name << dendl;
  pg->osd->pg_recovery_stats.log_enter(state_name);
}

void PG::RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  dout(5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, ceph_clock_now(pg->cct) - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix (*_dout << (debug_pg ? debug_pg->gen_prefix() : string()) << " PriorSet: ")

PG::PriorSet::PriorSet(bool ec_pool,
		       IsPGRecoverablePredicate *c,
		       const OSDMap &osdmap,
		       const map<epoch_t, pg_interval_t> &past_intervals,
		       const vector<int> &up,
		       const vector<int> &acting,
		       const pg_info_t &info,
		       const PG *debug_pg)
  : ec_pool(ec_pool), pg_down(false), pcontdec(c)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike) 
   *  4: A     i'm back, but i don't know if anyone has accepted client writes during the period i was out
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0, new interval, start peering process, PG::build_prior should notify monitor that the osd we are on is still alive
   *  3:   B   up_thru[B]=2, ok, in new osdmap the monitor has marked the osd we are on is alive in new interval
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i=0; i<acting.size(); i++) {
    if (acting[i] != CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(acting[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  // It may be possible to exlude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i=0; i<up.size(); i++) {
    if (up[i] != CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(up[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }

  // ok, now "probe" contains all osds in current interval's acting set and 
  // up set, now we want to search back if we can probe more osds to query

  // search back according past_intervals of this pg, for newly created PG
  // instance, its past_interals is empty, so the newly created PG always
  // contains acting set and up set
  // TODO: acting == up ??? refer to OSD::handle_pg_create
  
  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) { // iterate past_intervals in reverse order
    const pg_interval_t &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      // we have iterated back to info.history.last_epoch_started, i.e. the 
      // epoch that we finished previous peering process
      break;  // we don't care

    if (interval.acting.empty()) // no write could occur in this interval
      continue;

    // this field is set in pg_interval_t::check_new_interval
    if (!interval.maybe_went_rw) // no write occurred in this interval
      continue;

    // during this interval we may have done some writes to this pg

    // look at candidate osds during this interval.  each falls into
    // one of three categories: up, down (but potentially
    // interesting), or lost (down, but we won't wait for it).
    set<pg_shard_t> up_now;
    bool any_down_now = false;  // any candidates down now (that might have useful data)

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) { // iterate acting set of this interval, i.e. each replica
      int o = interval.acting[i];
      if (o == CRUSH_ITEM_NONE)
	continue;
      pg_shard_t so(o, ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD); // we need to query all PG replicas for each interval

      const osd_info_t *pinfo = 0; // osd info in current osdmap
      if (osdmap.exists(o))
	pinfo = &osdmap.get_info(o);

      if (osdmap.is_up(o)) { // currently exists and up, in latest map
	// include past acting osds if they are up.
	probe.insert(so);
	up_now.insert(so);
      } else if (!pinfo) { // currently does not exist
        // osd does not exist in latest osdmap, probably has been removed
	dout(10) << "build_prior  prior osd." << o << " no longer exists" << dendl;
	down.insert(o);
      } else if (pinfo->lost_at > interval.first) { // currently exists and down, and marked lost (the same effect as up)
        // we marked the osd as lost, i.e. we don't care whether the osd held our 
        // modified data or not during this interval
	dout(10) << "build_prior  prior osd." << o << " is down, but lost_at " << pinfo->lost_at << dendl;
	up_now.insert(so); // osd down, but we don't have to wait for it
	down.insert(o); // osd down
      } else { // currently exists and down, and did not mark lost
	dout(10) << "build_prior  prior osd." << o << " is down" << dendl;
	down.insert(o); // osd down, may have useful data
	any_down_now = true; // have down osd that we can wait and then get useful info from
      }
    }

    // may had data written during this interval

    // if not enough osds survived this interval, and we may have gone rw,
    // then we need to wait for one of those osds to recover to
    // ensure that we haven't lost any information.
    if (!(*pcontdec)(up_now) && any_down_now) {
      // currently we have not enough up osds to survive this interval but we
      // can wait for some down osds to get up and then survive this interval
      // fixme: how do we identify a "clean" shutdown anyway?
      dout(10) << "build_prior  possibly went active+rw, insufficient up;"
	       << " including down osds" << dendl;
      for (vector<int>::const_iterator i = interval.acting.begin();
	   i != interval.acting.end();
	   ++i) {
	if (osdmap.exists(*i) &&   // if it doesn't exist, we already consider it lost.
	    osdmap.is_down(*i)) { // osd exists and down
	  pg_down = true;

	  // make note of when any down osd in the cur set was lost, so that
	  // we can notice changes in prior_set_affected.
	  blocked_by[*i] = osdmap.get_info(*i).lost_at; // see OSDMoinitor::prepare_boot
	}
      }
    }
  }

  // iterated every interval from currently now back to last successful peering

  dout(10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
}

// true if the given map affects the prior set
bool PG::PriorSet::affected_by_map(const OSDMapRef osdmap, const PG *debug_pg) const
{
  for (set<pg_shard_t>::iterator p = probe.begin();
       p != probe.end();
       ++p) { // iterate each probe target
    int o = p->osd;

    // did someone in the prior set go down?
    if (osdmap->is_down(o) && down.count(o) == 0) { // up -> down
      dout(10) << "affected_by_map osd." << o << " now down" << dendl;
      return true;
    }

    // did a down osd in cur get (re)marked as lost?
    map<int, epoch_t>::const_iterator r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (!osdmap->exists(o)) { // exists -> does not exist
	dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
	return true;
      }

      // if we have never mark the osd as lost, then r->second is 0, refer to 
      // PG::PriorSet::PriorSet
      if (osdmap->get_info(o).lost_at != r->second) { // may marked lost -> marked lost
	dout(10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
	return true;
      }
    }
  }

  // did someone in the prior down set go up?
  for (set<int>::const_iterator p = down.begin();
       p != down.end();
       ++p) {
    int o = *p;

    if (osdmap->is_up(o)) {
      dout(10) << "affected_by_map osd." << *p << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap->exists(o)) {
      dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
      return true;
    }
  }

  return false;
}

void PG::RecoveryState::start_handle(RecoveryCtx *new_ctx) {
  // RecoveryState::end_handle will always invalidates these two variables
  assert(!rctx);
  assert(!orig_ctx);

  orig_ctx = new_ctx; // always note down the new RecoveryCtx

  if (new_ctx) {
    if (messages_pending_flush) {
      // if we are buffering, all RecoveryCtx info during this RecoveryState::start_handle
      // and RecoveryState::end_handle pair (i.e. RecoveryState::handle_event) will
      // be lost, so if we are processing an event with internal event(s), we may
      // want to store the cumulated info in RecoveryCtx instance, we implement 
      // this by using an memeber variable of RecoveryState, even if multiple
      // internal events get processed, i.e. RecoveryState::rctx get set and reset
      // multiple times, the member variable's info maintains
 
      // construct a RecoveryCtx from messages_pending_flush and new_ctx
      rctx = RecoveryCtx(*messages_pending_flush, *new_ctx);
    } else {
      rctx = *new_ctx;
    }
    rctx->start_time = ceph_clock_now(pg->cct);
  }
}

// PG::reset_interval_flush will call this
void PG::RecoveryState::begin_block_outgoing() {
  assert(!messages_pending_flush);
  assert(orig_ctx);
  assert(rctx);
  messages_pending_flush = BufferedRecoveryMessages();
  rctx = RecoveryCtx(*messages_pending_flush, *orig_ctx);
}

// PG::reset_interval_flush will call this
void PG::RecoveryState::clear_blocked_outgoing() {
  assert(orig_ctx);
  assert(rctx);

  // invalidate messages_pending_flush
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

// RecoveryState::Started and RecoveryState::Reset react to IntervalFlush
// event will call this
void PG::RecoveryState::end_block_outgoing() {
  assert(messages_pending_flush);
  assert(orig_ctx);
  assert(rctx);

  rctx = RecoveryCtx(*orig_ctx);
  rctx->accept_buffered_messages(*messages_pending_flush);
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

void PG::RecoveryState::end_handle() {
  if (rctx) {
    utime_t dur = ceph_clock_now(pg->cct) - rctx->start_time;
    machine.event_time += dur;
  }

  machine.event_count++;

  // invalidate these two variables, start_handle asserts they are invalid
  rctx = boost::optional<RecoveryCtx>();
  orig_ctx = NULL;
}

ostream& operator<<(ostream& out, const PG::BackfillInterval& bi)
{
  out << "BackfillInfo(" << bi.begin << "-" << bi.end
      << " " << bi.objects.size() << " objects";
  if (!bi.objects.empty())
    out << " " << bi.objects;
  out << ")";
  return out;
}

void intrusive_ptr_add_ref(PG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif
