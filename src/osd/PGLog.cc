// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "PGLog.h"
#include "PG.h"
#include "SnapMapper.h"
#include "../include/unordered_map.h"

#define dout_subsys ceph_subsys_osd

//////////////////// PGLog::IndexedLog ////////////////////

void PGLog::IndexedLog::advance_rollback_info_trimmed_to(
  eversion_t to, // eversion we want to trim to
  LogEntryHandler *h) // h is of type PG::PGLogEntryHandler
{
  assert(to <= can_rollback_to); // We can rollback rollback-able entries > can_rollback_to

  if (to > rollback_info_trimmed_to)
    // rollback_info_trimmed_to always <= can_rollback_to, indicates how 
    // far stashed rollback data can be found
    rollback_info_trimmed_to = to;

  // rollback_info_trimmed_to_riter points to the first log entry <=
  // rollback_info_trimmed_to

  while (rollback_info_trimmed_to_riter != log.rbegin()) {
    // iterate log entries in _positive_ order, i.e. log.tail -> log.head, from 
    // last updated riter to log.rbegin
    // update rollback_info_trimmed_to_riter to catch up rollback_info_trimmed_to,
    // i.e. trim the old entries and update the iterator
    --rollback_info_trimmed_to_riter;
    if (rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to) {
      ++rollback_info_trimmed_to_riter;
      break;
    }

    // push the entry back of PGLogEntryHandler::to_trim list
    h->trim(*rollback_info_trimmed_to_riter);
  }
}

void PGLog::IndexedLog::filter_log(spg_t pgid, const OSDMap &map, const string &hit_set_namespace)
{
  IndexedLog out;
  pg_log_t reject;

  pg_log_t::filter_log(pgid, map, hit_set_namespace, *this, out, reject);

  *this = out;
  index();
}

void PGLog::IndexedLog::split_into(
  pg_t child_pgid,
  unsigned split_bits,
  PGLog::IndexedLog *olog)
{
  list<pg_log_entry_t> oldlog;
  oldlog.swap(log);

  eversion_t old_tail;
  olog->head = head;
  olog->tail = tail;
  unsigned mask = ~((~0)<<split_bits);
  for (list<pg_log_entry_t>::iterator i = oldlog.begin();
       i != oldlog.end();
       ) {
    if ((i->soid.get_hash() & mask) == child_pgid.m_seed) {
      olog->log.push_back(*i);
    } else {
      log.push_back(*i);
    }
    oldlog.erase(i++);
  }


  olog->can_rollback_to = can_rollback_to;

  olog->index();
  index();
}

void PGLog::IndexedLog::trim(
  LogEntryHandler *handler,
  eversion_t s, // eversion we want to trim to
  set<eversion_t> *trimmed)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    // do not trim the log past IndexedLog::complete_to 
    // (type of list<pg_log_entry_t>::iterator), the caller has ensured that 
    // they will not trim the log past info.last_complete (type of eversion_t)
    generic_dout(0) << " bad trim to " << s << " when complete_to is "
		    << complete_to->version
		    << " on " << *this << dendl;
  }

  if (s > can_rollback_to)
    // we will trim the older log entries, apparently we can not rollback
    // to those old versions, so increase the can_rollback_to version
    can_rollback_to = s; // member variable derived from pg_info_t

  // update rollback_info_trimmed_to and rollback_info_trimmed_to_riter, note
  // down entries to be trimmed into handler
  advance_rollback_info_trimmed_to(s, handler);

  while (!log.empty()) { // iterate log entries in _positive_ order, i.e. log.tail -> log.head
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    if (trimmed)
      trimmed->insert(e.version); // note down entries to be deleted

    unindex(e);         // remove from index,

    if (rollback_info_trimmed_to_riter == log.rend() || // why need the first condition???
	e.version == rollback_info_trimmed_to_riter->version) {
      // we reached the last entry to trim, after pop_front, the reverse 
      // iterator will be invalidated
      log.pop_front();
      // validate the reverse iterator again
      rollback_info_trimmed_to_riter = log.rend(); // we should always be a valid iterator
    } else {
      log.pop_front();
    }
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

ostream& PGLog::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}

//////////////////// PGLog ////////////////////

void PGLog::reset_backfill()
{
  missing.clear();
  divergent_priors.clear();
  dirty_divergent_priors = true;
}

void PGLog::clear() {
  divergent_priors.clear();
  missing.clear();
  log.clear();
  log_keys_debug.clear();
  undirty();
}

void PGLog::clear_info_log(
  spg_t pgid,
  ObjectStore::Transaction *t) {
  coll_t coll(pgid);
  t->remove(coll, pgid.make_pgmeta_oid());
}

void PGLog::trim(
  LogEntryHandler *handler,
  eversion_t trim_to,
  pg_info_t &info)
{
  // trim?
  if (trim_to > log.tail) { // log.tail < trim_to <= info.last_complete
    /* If we are trimming, we must be complete up to trim_to, time
     * to throw out any divergent_priors
     */
    divergent_priors.clear(); // TODO: ???
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;

    // update can_rollback_to, rollback_info_trimmed_to and rollback_info_trimmed_to_riter,
    // remove the old entries from the index and note down the old entries to remove
    // in PGLog::trimmed, note down the old old entries for rollback to remove in "handler",
    // and raise log.tail after all these, the real remove action will be performed by the caller
    log.trim(handler, trim_to, &trimmed);

    // update log tail of pg info
    info.log_tail = log.tail;
  }
}

void PGLog::proc_replica_log(
  ObjectStore::Transaction& t,
  pg_info_t &oinfo, const pg_log_t &olog, pg_missing_t& omissing,
  pg_shard_t from) const
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  if (olog.head < log.tail) { // TODO: we have only requested peer that has oinfo.last_update > log.tail ??
    dout(10) << __func__ << ": osd." << from << " does not overlap, not looking "
	     << "for divergent objects" << dendl;
    return;
  }

  // no missing map and pg info to update for this peer
  if (olog.head == log.head) {
    dout(10) << __func__ << ": osd." << from << " same log head, not looking "
	     << "for divergent objects" << dendl;
    return;
  }
  
  assert(olog.head >= log.tail);

  /*
    basically what we're doing here is rewinding the remote log,
    dropping divergent entries, until we find something that matches
    our master log.  we then reset last_update to reflect the new
    point up to which missing is accurate.

    later, in activate(), missing will get wound forward again and
    we will send the peer enough log to arrive at the same state.
  */

  for (map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " before missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }

  list<pg_log_entry_t>::const_reverse_iterator first_non_divergent = log.log.rbegin();
  while (1) { // iterate our pg log entries in reverse order
    if (first_non_divergent == log.log.rend())
      break;
    
    if (first_non_divergent->version <= olog.head) {
      // 1) reached peer's head or 2) log.head < olog.head
      dout(20) << "merge_log point (usually last shared) is "
	       << *first_non_divergent << dendl;
      break;
    }
    
    ++first_non_divergent;
  }

  /* Because olog.head >= log.tail, we know that both pgs must at least have
   * the event represented by log.tail.  Thus, lower_bound >= log.tail.  It's
   * possible that olog/log contain no actual events between olog.head and
   * log.tail, however, since they might have been split out.  Thus, if
   * we cannot find an event e such that log.tail <= e.version <= log.head,
   * the last_update must actually be log.tail.
   */
  eversion_t lu = // last non-divergent update for peer, refer to PGLog::merge_log
    (first_non_divergent == log.log.rend() || first_non_divergent->version < log.tail) ?
        log.tail : first_non_divergent->version;

  list<pg_log_entry_t> divergent;
  list<pg_log_entry_t>::const_iterator pp = olog.log.end();
  while (true) { // iterate peer's log in reverse order to gather peer's divergent entries
    if (pp == olog.log.begin())
      break; // finished iterating

    --pp;
    const pg_log_entry_t& oe = *pp;

    // don't continue past the tail of our log.
    if (oe.version <= log.tail) { // TODO: duplicate with the next test ???
      ++pp;
      break;
    }

    if (oe.version <= lu) { // reached the first non-divergent entry of peer
      ++pp;
      break;
    }

    divergent.push_front(oe);
  }

  IndexedLog folog;
  folog.log.insert(folog.log.begin(), olog.log.begin(), pp); // all non-divergent entries
  folog.index();

  // merge divergent log entries to update peer's missing map
  _merge_divergent_entries(
    folog, // peer's non-divergent entries
    divergent, // divergent log entries
    oinfo,
    olog.can_rollback_to,
    omissing,
    0,
    0);

  if (lu < oinfo.last_update) {
    dout(10) << " peer osd." << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
  }

  if (omissing.have_missing()) { // pg_missing_t::missing is not empty
    eversion_t first_missing = omissing.missing[omissing.rmissing.begin()->second].need;
    oinfo.last_complete = eversion_t();
    list<pg_log_entry_t>::const_iterator i = olog.log.begin();
    for (;
	 i != olog.log.end();
	 ++i) { // iterate each log entry the peer has
      if (i->version < first_missing) // info.last_complete means at this version the PG is not divergent from the acting primary
 	oinfo.last_complete = i->version;
      else
	break;
    }
  } else {
    oinfo.last_complete = oinfo.last_update;
  }
}

/**
 * _merge_object_divergent_entries
 *
 * There are 5 distinct cases:
 * 1) There is a more recent update: in this case we assume we adjusted the
 *    store and missing during merge_log
 * 2) The first entry in the divergent sequence is a create.  This might
 *    either be because the object is a clone or because prior_version is
 *    eversion_t().  In this case the object does not exist and we must
 *    adjust missing and the store to match.
 * 3) We are currently missing the object.  In this case, we adjust the
 *    missing to our prior_version taking care to add a divergent_prior
 *    if necessary
 * 4) We can rollback all of the entries.  In this case, we do so using
 *    the rollbacker and return -- the object does not go into missing.
 * 5) We cannot rollback at least 1 of the entries.  In this case, we
 *    clear the object out of the store and add a missing entry at
 *    prior_version taking care to add a divergent_prior if
 *    necessary.
 */
void PGLog::_merge_object_divergent_entries(
  const IndexedLog &log,
  const hobject_t &hoid,
  const list<pg_log_entry_t> &entries,
  const pg_info_t &info,
  eversion_t olog_can_rollback_to,
  pg_missing_t &missing,
  boost::optional<pair<eversion_t, hobject_t> > *new_divergent_prior, // only used when entries.begin()->prior_version <= info.log_tail
  LogEntryHandler *rollbacker
  )
{
  dout(10) << __func__ << ": merging hoid " << hoid
	   << " entries: " << entries << dendl;

  // TODO: ???
  if (cmp(hoid, info.last_backfill, info.last_backfill_bitwise) > 0) {
    dout(10) << __func__ << ": hoid " << hoid << " after last_backfill"
	     << dendl;
    return;
  }

  // divergent log entries is non-empty
  assert(!entries.empty());
  
  eversion_t last;
  for (list<pg_log_entry_t>::const_iterator i = entries.begin();
       i != entries.end();
       ++i) { // iterate each divergent log entry of this object to trim, from older to newer
    assert(i->soid == hoid);
    
    if (i != entries.begin() && i->prior_version != eversion_t()) {
      // in increasing order of version
      assert(i->version > last);
      // prior_version correct
      assert(i->prior_version == last); // every pg_log_entry_t records the version of PG when last modifying the object
    }
    
    last = i->version; // object version, i.e. PG version when we last modifying the object
    
    if (rollbacker)
      rollbacker->trim(*i); // gather the divergent log entries to trim
  }

  const eversion_t prior_version = entries.begin()->prior_version;
  const eversion_t first_divergent_update = entries.begin()->version;
  const eversion_t last_divergent_update = entries.rbegin()->version;
  
  // auth log told us that the store does not need this hobject_t anymore, and the divergent entries
  // told us that it has deleted the hobject_t in its last operation, so the object does not need to 
  // be in the store anymore
  const bool object_not_in_store = !missing.is_missing(hoid) 
        && entries.rbegin()->is_delete(); // op == DELETE || op == LOST_DELETE
        
  dout(10) << __func__ << ": hoid " << hoid
	   << " prior_version: " << prior_version
	   << " first_divergent_update: " << first_divergent_update
	   << " last_divergent_update: " << last_divergent_update
	   << dendl;

  // find the latest log entry from updated log, i.e. synced with auth log, for this object
  ceph::unordered_map<hobject_t, pg_log_entry_t*>::const_iterator objiter = log.objects.find(hoid);
  
  if (objiter != log.objects.end() &&
      objiter->second->version >= first_divergent_update) { // object modified both in divergent and recent auth entries
    /// Case 1)
    assert(objiter->second->version > last_divergent_update);

    dout(10) << __func__ << ": more recent entry found: "
	     << *objiter->second << ", already merged" << dendl;

    // ensure missing has been updated appropriately, PGLog::merge_log has
    // updated the missing for us, refer to pg_missing_t::add_next_event
    if (objiter->second->is_update()) { // non LOST_DELETE
      assert(missing.is_missing(hoid) &&
	     missing.missing[hoid].need == objiter->second->version);
    } else { // LOST_DELETE
      assert(!missing.is_missing(hoid));
    }

    // our have version is divergent, so the previous calculated missing is deprecated, update it
    missing.revise_have(hoid, eversion_t());
    
    if (rollbacker && !object_not_in_store)
      rollbacker->remove(hoid);
    return;
  }

  dout(10) << __func__ << ": hoid " << hoid
	   <<" has no more recent entries in log" << dendl;

  if (prior_version == eversion_t() || entries.front().is_clone()) { // object modified only in divergent entries and new 
                                                                     // object created on the first divergent entry
    /// Case 2)
    dout(10) << __func__ << ": hoid " << hoid
	     << " prior_version or op type indicates creation, deleting"
	     << dendl;
    
    if (missing.is_missing(hoid)) // new created object in divergent entries, not referenced by extended entries, remove it
      missing.rm(missing.missing.find(hoid));
    
    if (rollbacker && !object_not_in_store)
      rollbacker->remove(hoid);
    return;
  }

  if (missing.is_missing(hoid)) { // object modified only in divergent entries, and the first divergent entry is not a creation
    /// Case 3)
    dout(10) << __func__ << ": hoid " << hoid
	     << " missing, " << missing.missing[hoid]
	     << " adjusting" << dendl;

    if (missing.missing[hoid].have == prior_version) { // we have got what we want
      dout(10) << __func__ << ": hoid " << hoid
	       << " missing.have is prior_version " << prior_version
	       << " removing from missing" << dendl;
      
      missing.rm(missing.missing.find(hoid));
    } else { // divergent entries modified the object that has already been in pg_missing_t
      dout(10) << __func__ << ": hoid " << hoid
	       << " missing.have is " << missing.missing[hoid].have
	       << ", adjusting" << dendl;
      
      missing.revise_need(hoid, prior_version);
      
      if (prior_version <= info.log_tail) { // the last modification of this object is long time ago
	dout(10) << __func__ << ": hoid " << hoid
		 << " prior_version " << prior_version << " <= info.log_tail "
		 << info.log_tail << dendl;
        
	if (new_divergent_prior)
	  *new_divergent_prior = make_pair(prior_version, hoid);
      }
    }
    
    return;
  }

  // the object only modified in the divergent entries, but it is not in pg_missing_t, i.e. it has been
  // deleted by the last divergent entry or the divergent entries have no missing set

  dout(10) << __func__ << ": hoid " << hoid
	   << " must be rolled back or recovered, attempting to rollback"
	   << dendl;
  
  bool can_rollback = true;
  /// Distinguish between 4) and 5)
  for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
       i != entries.rend();
       ++i) {
    if (!i->mod_desc.can_rollback() || i->version <= olog_can_rollback_to) {
      dout(10) << __func__ << ": hoid " << hoid << " cannot rollback "
	       << *i << dendl;
      can_rollback = false;
      break;
    }
  }

  if (can_rollback) {
    /// Case 4)
    for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
	 i != entries.rend();
	 ++i) {
      assert(i->mod_desc.can_rollback() && i->version > olog_can_rollback_to);
      dout(10) << __func__ << ": hoid " << hoid
	       << " rolling back " << *i << dendl;
      
      if (rollbacker)
	rollbacker->rollback(*i);
    }
    dout(10) << __func__ << ": hoid " << hoid << " rolled back" << dendl;
    return;
  } else { // can not rollback
    /// Case 5)
    dout(10) << __func__ << ": hoid " << hoid << " cannot roll back, "
	     << "removing and adding to missing" << dendl;
    
    if (rollbacker && !object_not_in_store) // the last divergent entry is: op != DELETE && op != LOST_DELETE
      rollbacker->remove(hoid);
    
    missing.add(hoid, prior_version, eversion_t()); // we need a not updated version, i.e. not modified by the divergent entries
    
    if (prior_version <= info.log_tail) {
      dout(10) << __func__ << ": hoid " << hoid
	       << " prior_version " << prior_version << " <= info.log_tail "
	       << info.log_tail << dendl;
      
      if (new_divergent_prior)
	*new_divergent_prior = make_pair(prior_version, hoid);
    }
  }
}

/**
 * rewind divergent entries at the head of the log
 *
 * This rewinds entries off the head of our log that are divergent.
 * This is used by replicas during activation.
 *
 * @param t transaction
 * @param newhead new head to rewind to
 */
void PGLog::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
				 pg_info_t &info, LogEntryHandler *rollbacker,
				 bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "rewind_divergent_log truncate divergent future " << newhead << dendl;
  assert(newhead >= log.tail);

  list<pg_log_entry_t>::iterator p = log.log.end();
  list<pg_log_entry_t> divergent;
  while (true) {
    if (p == log.log.begin()) { // reached the back of our tail 
      // yikes, the whole thing is divergent!
      divergent.swap(log.log);
      break;
    }
    --p;
    mark_dirty_from(p->version); // mark PGLog::dirty_from to p->version
    
    if (p->version <= newhead) {
      // ok, reached back of the head of authority log's head
      ++p;
      
      // move divergent log entries from p to log.log.end to this temp list, later we will
      // use it in _merge_divergent_entries
      divergent.splice(divergent.begin(), log.log, p, log.log.end());
      break;
    }
    assert(p->version > newhead);
    dout(10) << "rewind_divergent_log future divergent " << *p << dendl;
  }

  log.head = newhead; // truncate our log on head
  info.last_update = newhead;
  if (info.last_complete > newhead)
    info.last_complete = newhead;

  log.index(); // reindex pg log

  map<eversion_t, hobject_t> new_priors;
  // merge divergent pg log entries to update our missing map
  _merge_divergent_entries(
    log, // trunctated log entries
    divergent, // divergent log entries we have but the authority does not have
    info,
    log.can_rollback_to,
    missing,
    &new_priors,
    rollbacker);
  
  for (map<eversion_t, hobject_t>::iterator i = new_priors.begin();
       i != new_priors.end();
       ++i) {
    add_divergent_prior(
      i->first,
      i->second); // add this to PGLog::divergent_priors
  }

  if (info.last_update < log.can_rollback_to)
    log.can_rollback_to = info.last_update;

  dirty_info = true;
  dirty_big_info = true;
}

// 1) extend on tail, 
// 2) truncate on head/extend on head
void PGLog::merge_log(ObjectStore::Transaction& t,
                      pg_info_t &oinfo, pg_log_t &olog, pg_shard_t fromosd,
                      pg_info_t &info, LogEntryHandler *rollbacker,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "merge_log " << olog << " from osd." << fromosd
           << " into " << log << dendl;

  // Check preconditions

  // If our log is empty, the incoming log needs to have not been trimmed.
  assert(!log.null() || olog.tail == eversion_t());
  // The logs must overlap.
  assert(log.head >= olog.tail && olog.head >= log.tail);

  for (map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::iterator i = missing.missing.begin();
       i != missing.missing.end();
       ++i) {
    dout(20) << "pg_missing_t sobject: " << i->first << dendl;
  }

  bool changed = false;

  // extend on tail?
  //  this is just filling in history.  it does not affect our
  //  missing set, as that should already be consistent with our
  //  current log.
  if (olog.tail < log.tail) { // auth log longer than us, we have to extend our log backward
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<pg_log_entry_t>::iterator from = olog.log.begin(); // pg_log_t::list<pg_log_entry_t> olog.log
    list<pg_log_entry_t>::iterator to;
    eversion_t last;
    for (to = from; to != olog.log.end(); ++to) { // iterate each authority log entry up to our log's tail
      if (to->version > log.tail) // add the log entries we do not have
	break;
      
      log.index(*to); // add the missing log entry to our index
      dout(15) << *to << dendl;
      last = to->version;
    }

    // mark PGLog::dirty_to to last, which means those log entries from log.log.begin() to
    // last are dirty
    mark_dirty_to(last);

    // splice into our log. really add the missing log entries into our log entry list, i.e. PGLog::log
    log.log.splice(log.log.begin(),
		   olog.log, from, to);
      
    info.log_tail = log.tail = olog.tail; // extend out log on the tail
    changed = true;
  }

  if (oinfo.stats.reported_seq < info.stats.reported_seq ||   // make sure reported always increases
      oinfo.stats.reported_epoch < info.stats.reported_epoch) {
    oinfo.stats.reported_seq = info.stats.reported_seq;
    oinfo.stats.reported_epoch = info.stats.reported_epoch;
  }
  
  if (info.last_backfill.is_max())
    info.stats = oinfo.stats;
  
  info.hit_set = oinfo.hit_set;

  // do we have divergent entries to throw out?
  if (olog.head < log.head) { // our log are ahead of authority log
    // throw out the divergent pg log entries (we have, peer does not)
    rewind_divergent_log(t, olog.head, info, rollbacker, dirty_info, dirty_big_info);
    changed = true;
  }

  // extend on head?
  if (olog.head > log.head) {
    dout(10) << "merge_log extending head to " << olog.head << dendl;
      
    // find start extending point in olog
    list<pg_log_entry_t>::iterator to = olog.log.end();
    list<pg_log_entry_t>::iterator from = olog.log.end();
    eversion_t lower_bound = olog.tail;
    while (1) { // iterate back to the first non-divergent entry
      if (from == olog.log.begin())
	break;
      --from;
      dout(20) << "  ? " << *from << dendl;
      
      if (from->version <= log.head) { // ok, reached my head
	dout(20) << "merge_log cut point (usually last shared) is " << *from << dendl;
	lower_bound = from->version; // last non-divergent entry for us
	++from;
	break;
      }
    }

    // for this version on, the missing log entries are extended from the auth log
    mark_dirty_from(lower_bound);

    for (list<pg_log_entry_t>::iterator p = from; p != to; ++p) { // iterate entries to be extended to update missing set
      pg_log_entry_t &ne = *p;
      dout(20) << "merge_log " << ne << dendl;
      
      log.index(ne); // classify the pg_log_entry_t by hboject_t and osd_reqid_t
      
      if (cmp(ne.soid, info.last_backfill, info.last_backfill_bitwise) <= 0) {
        
	missing.add_next_event(ne); // update missing status, we may be based on the divergent entries (if we have divergent entries)
	
	if (ne.is_delete()) // delete deleted
	  rollbacker->remove(ne.soid);
      }
    }

    // ok, now we prepare to merge the divergent pg log entries (peer has, we do not), first we need
    // to throw out the divergent entries
      
    // move aside divergent items
    list<pg_log_entry_t> divergent;
    while (!log.empty()) { // gather our divergent log entries
      pg_log_entry_t &oe = *log.log.rbegin();
      /*
       * look at eversion.version here.  we want to avoid a situation like:
       *  our log: 100'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  new log: 122'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  lower_bound = 100'9
       * i.e, same request, different version.  If the eversion.version is > the
       * lower_bound, we take it is divergent.
       */
      if (oe.version.version <= lower_bound.version)
	break;
      
      dout(10) << "merge_log divergent " << oe << dendl;
      divergent.push_front(oe); // note down the divergent log entries, older -> newer
      
      log.log.pop_back(); // throw out the divergent log entries
    }

    // ok, extend on head
    log.log.splice(log.log.end(), olog.log, from, to);
    log.index(); // reindex 

    info.last_update = log.head = olog.head; // update pg info

    info.last_user_version = oinfo.last_user_version;
    info.purged_snaps = oinfo.purged_snaps;

    // we are extending our log entries on head, and we may have divergent entries from the
    // auth log to thow out

    map<eversion_t, hobject_t> new_priors;
    // merge divergent log entries to update our missing map
    _merge_divergent_entries(
      log, // log that has been extended
      divergent, // divergent log entries
      info, // the final info of us, i.e. the updated PG info that keep pace with auth log
      log.can_rollback_to,
      missing,
      &new_priors,
      rollbacker);
    
    for (map<eversion_t, hobject_t>::iterator i = new_priors.begin();
	 i != new_priors.end();
	 ++i) {
      add_divergent_prior(
	i->first,
	i->second);
    }

    // We cannot rollback into the new log entries
    log.can_rollback_to = log.head;

    changed = true;
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
  }
}

void PGLog::check() {
  if (!pg_log_debug)
    return;
  if (log.log.size() != log_keys_debug.size()) {
    derr << "log.log.size() != log_keys_debug.size()" << dendl;
    derr << "actual log:" << dendl;
    for (list<pg_log_entry_t>::iterator i = log.log.begin();
	 i != log.log.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
    derr << "log_keys_debug:" << dendl;
    for (set<string>::const_iterator i = log_keys_debug.begin();
	 i != log_keys_debug.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
  }
  assert(log.log.size() == log_keys_debug.size());
  for (list<pg_log_entry_t>::iterator i = log.log.begin();
       i != log.log.end();
       ++i) {
    assert(log_keys_debug.count(i->get_key_name()));
  }
}

// write the pg log that the PGLog contains
void PGLog::write_log(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  const coll_t& coll, const ghobject_t &log_oid)
{
  if (is_dirty()) { // pg log is dirty, need to write the log
    dout(5) << "write_log with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", dirty_divergent_priors: "
	     << (dirty_divergent_priors ? "true" : "false")
	     << ", divergent_priors: " << divergent_priors.size()
	     << ", writeout_from: " << writeout_from
	     << ", trimmed: " << trimmed
	     << dendl;

    // remove old dirty log entries and encode new dirty log entries into km
    _write_log(
      t, km, log, coll, log_oid, divergent_priors,
      dirty_to, // clear [eversion_t(), dirty_to), then write entries [..dirty_to)
      dirty_from, // clear [dirty_from, eversion_t::max()), then write entries [min(dirty_from, writeout_from)..]
      writeout_from, // used for determine the start of the write entry
      trimmed, // explicitly marked to clear
      dirty_divergent_priors,
      !touched_log,
      (pg_log_debug ? &log_keys_debug : 0));
    
    undirty(); // mark that there is no dirty log entries
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

// write the specified pg log which provided in the input parameters, this
// is a static method of PGLog, it is only used in ceph_objectstore_tool.cc
void PGLog::write_log(
    ObjectStore::Transaction& t,
    map<string,bufferlist> *km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors)
{
  _write_log(
    t, km, log, coll, log_oid,
    divergent_priors, eversion_t::max(), eversion_t(), eversion_t(),
    set<eversion_t>(),
    true, true, 0);
}

void PGLog::_write_log(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  pg_log_t &log,
  const coll_t& coll, const ghobject_t &log_oid,
  map<eversion_t, hobject_t> &divergent_priors,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  const set<eversion_t> &trimmed,
  bool dirty_divergent_priors,
  bool touch_log,
  set<string> *log_keys_debug
  )
{
  set<string> to_remove;
  for (set<eversion_t>::const_iterator i = trimmed.begin();
       i != trimmed.end();
       ++i) { // note down those entries (identified by the key) to be removed
    to_remove.insert(i->get_key_name()); // sprintf(key, "%010u.%020llu", epoch, version)
    if (log_keys_debug) {
      assert(log_keys_debug->count(i->get_key_name()));
      log_keys_debug->erase(i->get_key_name());
    }
  }

//dout(10) << "write_log, clearing up to " << dirty_to << dendl;
  if (touch_log)
    t.touch(coll, log_oid); // ensure the pg meta oid exist

  // Log is clean on [dirty_to, dirty_from), we only write those entries 
  // that are dirty, e.g. [e3 - e6] are excluded to write:
  // [e1 e2 dirty_to(e3) e4 e5 e6 dirty_from(e7) e8 e9]

  // remove the old entries [eversion_t(), dirty_to) that are dirty
  if (dirty_to != eversion_t()) { 
    t.omap_rmkeyrange(
      coll, log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    
    clear_up_to(log_keys_debug, dirty_to.get_key_name()); // for debug use only
  }
  // remove the old entries [dirty_from, eversion_t::max()) that are dirty
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    
    clear_after(log_keys_debug, dirty_from.get_key_name()); // for debug use only
  }

  // encode the dirty entries up to dirty_to
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version <= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl); // encode log entry
    (*km)[p->get_key_name()].claim(bl);
  }
  // encode the dirty entries down to min(dirty_from, writeout_from)
  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = (*km).begin();
	 i != (*km).end();
	 ++i) {
      if (i->first[0] == '_')
	continue;
      assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  if (dirty_divergent_priors) {
    //dout(10) << "write_log: writing divergent_priors" << dendl;
    ::encode(divergent_priors, (*km)["divergent_priors"]);
  }
  ::encode(log.can_rollback_to, (*km)["can_rollback_to"]);
  ::encode(log.rollback_info_trimmed_to, (*km)["rollback_info_trimmed_to"]);

  // remove the entries must be trimmed
  if (!to_remove.empty())
    t.omap_rmkeys(coll, log_oid, to_remove); // remove those log entries identified by their keys
}

/* IndexedLog is derived from pg_log_t
 *
 * struct pg_log_t {
 *     // head - newest entry (update|delete)
 *     // tail - entry previous to oldest (update|delete) for which we have
 *               complete negative information.  
 *     // i.e. we can infer pg contents for any store whose last_update >= tail.
 *  
 *      eversion_t head;    // newest entry
 *      eversion_t tail;    // version prior to oldest
 *
 *      // We can rollback rollback-able entries > can_rollback_to
 *      eversion_t can_rollback_to;
 *
 *      // always <= can_rollback_to, indicates how far stashed rollback
 *      // data can be found
 *      eversion_t rollback_info_trimmed_to;
 *
 *      list<pg_log_entry_t> log;  // the actual log.
 * };
 *
 * struct PGLog {
 *     map<eversion_t, hobject_t> divergent_priors;
 *     pg_missing_t     missing;
 *     IndexedLog  log;
 *
 *     eversion_t dirty_to;         ///< must clear/writeout all keys <= dirty_to
 *     eversion_t dirty_from;       ///< must clear/writeout all keys >= dirty_from
 *     eversion_t writeout_from;    ///< must writout keys >= writeout_from
 *     set<eversion_t> trimmed;     ///< must clear keys in trimmed
 * };
 */

void PGLog::read_log(ObjectStore *store, coll_t pg_coll,
		     coll_t log_coll,
		    ghobject_t log_oid,
		    const pg_info_t &info,
		    map<eversion_t, hobject_t> &divergent_priors,
		    IndexedLog &log, // IndexedLog derives from pg_log_t
		    pg_missing_t &missing,
		    ostringstream &oss,
		    set<string> *log_keys_debug)
{
  dout(20) << "read_log coll " << pg_coll << " log_oid " << log_oid << dendl;

  // legacy?
  struct stat st;
  int r = store->stat(log_coll, log_oid, &st); // stat pg meta object, only for sanity checks
  assert(r == 0);
  assert(st.st_size == 0); // pg meta object has no data, only omap

  log.tail = info.log_tail;
  // will get overridden below if it had been recorded
  log.can_rollback_to = info.last_update;
  log.rollback_info_trimmed_to = eversion_t();

  // get an iterator to iterate the omap of the pg meta object
  ObjectMap::ObjectMapIterator p = store->get_omap_iterator(log_coll, log_oid);
  if (p) {
    for (p->seek_to_first(); p->valid() ; p->next()) { // iterate each omap
      // non-log pgmeta_oid keys are prefixed with _; skip those
      if (p->key()[0] == '_')
	continue;

      // ok, a log related item to handle
      
      bufferlist bl = p->value();//Copy bufferlist before creating iterator
      bufferlist::iterator bp = bl.begin();
      if (p->key() == "divergent_priors") {
	::decode(divergent_priors, bp);
	dout(20) << "read_log " << divergent_priors.size() << " divergent_priors" << dendl;
      } else if (p->key() == "can_rollback_to") {
	bufferlist bl = p->value();
	bufferlist::iterator bp = bl.begin();
	::decode(log.can_rollback_to, bp);
      } else if (p->key() == "rollback_info_trimmed_to") {
	bufferlist bl = p->value();
	bufferlist::iterator bp = bl.begin();
	::decode(log.rollback_info_trimmed_to, bp);
      } else {
        // a log entry, the key is constructed from pg_log_entry_t::version, i.e.
        // sprintf(key, "%010u.%020llu", epoch, version)
	pg_log_entry_t e;
	e.decode_with_checksum(bp);
	dout(20) << "read_log " << e << dendl;
	if (!log.log.empty()) { // list<pg_log_entry_t> IndexedLog::log;
	  pg_log_entry_t last_e(log.log.back());
	  assert(last_e.version.version < e.version.version); // pg log entry is logged in ascending order
	  assert(last_e.version.epoch <= e.version.epoch);
	}
	log.log.push_back(e); // push the log entry to log list
	log.head = e.version; // update log head
	if (log_keys_debug)
	  log_keys_debug->insert(e.get_key_name()); // sprintf(key, "%010u.%020llu", epoch, version)
      }
    }
  }

  // all log entries read

  // log.tail and log.head both are set by pg info
  
  log.head = info.last_update;

  // classify log entries by hobject_t and osd_reqid_t, then update rollback_info_trimmed_to down to 
  // log entry <= rollback_info_trimmed_to
  log.index();

  // build missing
  if (info.last_complete < info.last_update) { // we extended our log from auth log previously and then we restarted
    dout(10) << "read_log checking for missing items over interval (" << info.last_complete
	     << "," << info.last_update << "]" << dendl;

    set<hobject_t, hobject_t::BitwiseComparator> did;
    for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 ++i) { // reverse iterate log entries down to info.last_complete
      if (i->version <= info.last_complete) 
        break;

      // refer to: http://dachary.org/?p=2009
      // The last_backfill attribute of the placement group draws the limit 
      // separating the objects that have already been copied from other OSDs 
      // and those in the process of being copied. The objects that are lower 
      // than last_backfill have been copied and the objects that are greater 
      // than last_backfill are going to be copied.
      if (cmp(i->soid, info.last_backfill, info.last_backfill_bitwise) > 0) // cmp for hobject_t defined in hobject.h
        // i->soid > info.last_backfill
	continue;
      
      if (did.count(i->soid)) continue;
      
      did.insert(i->soid); // only have to read the newest log entry of the object 

      // op == DELETE || op == LOST_DELETE, the object does not exist now
      if (i->is_delete()) continue;

      // the last time the OSD was alive may have peered its PGLog, but the 
      // objects (from last_complete to last_update) it contains may have not synced
      
      bufferlist bv;
      int r = store->getattr(
	pg_coll,
	ghobject_t(i->soid, ghobject_t::NO_GEN, info.pgid.shard),
	OI_ATTR,
	bv);
      if (r >= 0) { // get current info of the modified object
        
	object_info_t oi(bv);
	if (oi.version < i->version) {
          // the log entry recorded a newer object
	  dout(15) << "read_log  missing " << *i << " (have " << oi.version << ")" << dendl;
	  missing.add(i->soid, i->version, oi.version);
	}
      } else { // no info found for the modified object, object does not exist in store
	dout(15) << "read_log  missing " << *i << dendl;
	missing.add(i->soid, i->version, eversion_t());
      }
    }

    // the log entries are not backward longer enough to cover this
    for (map<eversion_t, hobject_t>::reverse_iterator i =
	   divergent_priors.rbegin();
	 i != divergent_priors.rend();
	 ++i) {
      if (i->first <= info.last_complete) break;
      
      // skip those objects past backfill line
      if (cmp(i->second, info.last_backfill, info.last_backfill_bitwise) > 0)
	continue;
      
      if (did.count(i->second)) continue;
      
      did.insert(i->second); // ok, mark that we have handled this object with a newer version
      bufferlist bv;
      int r = store->getattr(
	pg_coll,
	ghobject_t(i->second, ghobject_t::NO_GEN, info.pgid.shard),
	OI_ATTR,
	bv);
      if (r >= 0) { // this oid does not have an entry in the log, i.e. log trimmed and the needed object is modified back to the trimmed entries
	object_info_t oi(bv);
	/**
	 * 1) we see this entry in the divergent priors mapping
	 * 2) we didn't see an entry for this object in the log
	 *
	 * From 1 & 2 we know that either the object does not exist
	 * or it is at the version specified in the divergent_priors
	 * map since the object would have been deleted atomically
	 * with the addition of the divergent_priors entry, an older
	 * version would not have been recovered, and a newer version
	 * would show up in the log above.
	 */
	assert(oi.version == i->first); // TODO: ???
      } else {
	dout(15) << "read_log  missing " << *i << dendl;
	missing.add(i->second, i->first, eversion_t());
      }
    }
  }
  dout(10) << "read_log done" << dendl;
}

