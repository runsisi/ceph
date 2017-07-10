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
#ifndef CEPH_PG_LOG_H
#define CEPH_PG_LOG_H

// re-include our assert to clobber boost's
#include "include/assert.h"
#include "osd_types.h"
#include "os/ObjectStore.h"
#include <list>
using namespace std;

#define PGLOG_INDEXED_OBJECTS          (1 << 0)
#define PGLOG_INDEXED_CALLER_OPS       (1 << 1)
#define PGLOG_INDEXED_EXTRA_CALLER_OPS (1 << 2)
#define PGLOG_INDEXED_ALL              (PGLOG_INDEXED_OBJECTS | PGLOG_INDEXED_CALLER_OPS | PGLOG_INDEXED_EXTRA_CALLER_OPS)

class CephContext;

// created by
// as a member of PG, initialized by PG::PG
struct PGLog : DoutPrefixProvider {
  DoutPrefixProvider *prefix_provider;
  string gen_prefix() const override {
    return prefix_provider ? prefix_provider->gen_prefix() : "";
  }
  unsigned get_subsys() const override {
    return prefix_provider ? prefix_provider->get_subsys() :
      (unsigned)ceph_subsys_osd;
  }
  CephContext *get_cct() const override {
    return cct;
  }

  ////////////////////////////// sub classes //////////////////////////////

  // derived by
  // struct PGLogEntryHandler
  struct LogEntryHandler {
    virtual void rollback(
      const pg_log_entry_t &entry) = 0; // PGBackend::rollback
    virtual void rollforward(
      const pg_log_entry_t &entry) = 0; // PGBackend::rollforward
    virtual void trim(
      const pg_log_entry_t &entry) = 0; // PGBackend::trim
    virtual void remove(
      const hobject_t &hoid) = 0;       // PGBackend::remove
    virtual void try_stash(
      const hobject_t &hoid,
      version_t v) = 0;                 // PGBackend::try_stash

    virtual ~LogEntryHandler() {}
  };

  /* Exceptions */
  class read_log_and_missing_error : public buffer::error {
  public:
    explicit read_log_and_missing_error(const char *what) {
      snprintf(buf, sizeof(buf), "read_log_and_missing_error: %s", what);
    }
    const char *what() const throw () override {
      return buf;
    }
  private:
    char buf[512];
  };

public:
  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public pg_log_t {
    mutable ceph::unordered_map<hobject_t,pg_log_entry_t*> objects;  // ptrs into log.  be careful!
    mutable ceph::unordered_map<osd_reqid_t,pg_log_entry_t*> caller_ops;

    // for cache only, see PrimaryLogPG::finish_ctx
    mutable ceph::unordered_multimap<osd_reqid_t,pg_log_entry_t*> extra_caller_ops;

    // recovery pointers
    list<pg_log_entry_t>::iterator complete_to; // not inclusive of referenced item
    version_t last_requested = 0;               // last object requested by primary

    //
  private:
    // will be set by IndexedLog::index(__u16 to_index = PGLOG_INDEXED_ALL)
    mutable __u16 indexed_data = 0;

    /**
     * rollback_info_trimmed_to_riter points to the first log entry <=
     * rollback_info_trimmed_to
     *
     * It's a reverse_iterator because rend() is a natural representation for
     * tail, and rbegin() works nicely for head.
     */
    mempool::osd_pglog::list<pg_log_entry_t>::reverse_iterator
      rollback_info_trimmed_to_riter;

    // called by
    // IndexedLog::trim_rollback_info_to, which called by PGLog::reset_backfill_claim_log
    // IndexedLog::roll_forward_to
    // IndexedLog::skip_can_rollback_to_to_head
    template <typename F>
    void advance_can_rollback_to(eversion_t to, F &&f) {
      if (to > can_rollback_to)
	can_rollback_to = to;

      if (to > rollback_info_trimmed_to)
	rollback_info_trimmed_to = to;

      while (rollback_info_trimmed_to_riter != log.rbegin()) {
        // update reverse iterator to reach version rollback_info_trimmed_to

        --rollback_info_trimmed_to_riter;

	if (rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to) {
	  ++rollback_info_trimmed_to_riter;
	  break;
	}

	// LogEntryHandler::rollforward(entry)
	f(*rollback_info_trimmed_to_riter);
      }
    }

    // called by
    // PGLog::IndexedLog::split_out_child
    // IndexedLog::IndexedLog
    // IndexedLog::rewind_from_head
    void reset_rollback_info_trimmed_to_riter() {
      rollback_info_trimmed_to_riter = log.rbegin();
      while (rollback_info_trimmed_to_riter != log.rend() &&
	     rollback_info_trimmed_to_riter->version > rollback_info_trimmed_to)
	++rollback_info_trimmed_to_riter; // back to older entries
    }

    // indexes objects, caller ops and extra caller ops
  public:
    // created by
    // as a member of PGLog
    IndexedLog() :
      complete_to(log.end()),
      last_requested(0),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin())
      {}

    // called by
    // IndexedLog::claim_log_and_clear_rollback_info
    // read_log_and_missing
    template <typename... Args>
    IndexedLog(Args&&... args) :
      pg_log_t(std::forward<Args>(args)...), // pod type
      complete_to(log.end()),
      last_requested(0),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin()) {
      reset_rollback_info_trimmed_to_riter();
      // (re)init index, init IndexedLog::indexed_data to PGLOG_INDEXED_ALL
      index();
    }

    // called by
    // IndexedLog &operator=(const IndexedLog &rhs)
    IndexedLog(const IndexedLog &rhs) :
      pg_log_t(rhs), // pod type
      complete_to(log.end()),
      last_requested(rhs.last_requested),
      indexed_data(0),
      rollback_info_trimmed_to_riter(log.rbegin()) {
      reset_rollback_info_trimmed_to_riter();
      index(rhs.indexed_data);
    }

    // called by
    // IndexedLog::claim_log_and_clear_rollback_info
    IndexedLog &operator=(const IndexedLog &rhs) {
      this->~IndexedLog();

      // IndexedLog(const IndexedLog &rhs)
      new (this) IndexedLog(rhs);
      return *this;
    }

    // called by
    // PGLog::reset_backfill_claim_log
    void trim_rollback_info_to(eversion_t to, LogEntryHandler *h) {
      advance_can_rollback_to(
	to,
	[&](pg_log_entry_t &entry) {
	  h->trim(entry);
	});
    }

    // called by
    // PGLog::roll_forward_to
    void roll_forward_to(eversion_t to, LogEntryHandler *h) {
      // advance pg_log_t::can_rollback_to, pg_log_t::rollback_info_trimmed_to
      // IndexedLog::rollback_info_trimmed_to_riter
      advance_can_rollback_to(
	to,
	[&](pg_log_entry_t &entry) {
	  h->rollforward(entry); // PGLogEntryHandler::rollforward, PG::pgbackend->rollforward
	});
    }

    // called by
    // IndexedLog::claim_log_and_clear_rollback_info
    // IndexedLog::clear, which called by PGLog::clear, which never used
    // IndexedLog::add
    // PGLog::merge_log
    // PG::append_log, which called by PrimaryLogPG::log_operation
    void skip_can_rollback_to_to_head() {
      // advance pg_log_t::can_rollback_to and pg_log_t::rollback_info_trimmed_to
      // to current pg_log_t::head
      advance_can_rollback_to(head, [&](const pg_log_entry_t &entry) {});
    }

    // called by
    // PGLog::proc_replica_log
    // PGLog::rewind_divergent_log
    // PGLog::merge_log
    mempool::osd_pglog::list<pg_log_entry_t> rewind_from_head(eversion_t newhead) {
      // step back pg_log_t::can_rollback_to and pg_log_t::rollback_info_trimmed_to
      auto divergent = pg_log_t::rewind_from_head(newhead);

      index();

      // step back rollback_info_trimmed_to_riter point to IndexedLog::rollback_info_trimmed_to
      // which stepped back by pg_log_t::rewind_from_head called above
      reset_rollback_info_trimmed_to_riter();

      return divergent;
    }

    // called by
    // PrimaryLogPG::update_range, which called by PrimaryLogPG::recover_backfill
    template <typename T>
    void scan_log_after(
      const eversion_t &bound, ///< [in] scan entries > bound
      T &&f) const {
      auto iter = log.rbegin();
      while (iter != log.rend() && iter->version > bound)
	++iter;

      while (true) {
	if (iter == log.rbegin())
	  break;
	f(*(--iter));
      }
    }

    // called by
    // PGLog::reset_backfill_claim_log, which called by PG::RecoveryState::Stray::react(const MLogRec)
    void claim_log_and_clear_rollback_info(const pg_log_t& o) {
      // we must have already trimmed the old entries
      assert(rollback_info_trimmed_to == head);
      assert(rollback_info_trimmed_to_riter == log.rbegin());

      // IndexedLog(Args&&... args) -> IndexedLog &operator=(const IndexedLog &rhs)
      *this = IndexedLog(o);

      skip_can_rollback_to_to_head();
      index();
    }

    void split_out_child(
      pg_t child_pgid,
      unsigned split_bits,
      IndexedLog *target);

    // called by
    // IndexedLog::clear, i.e., below, i.e., never used
    void zero() {
      // we must have already trimmed the old entries
      assert(rollback_info_trimmed_to == head);
      assert(rollback_info_trimmed_to_riter == log.rbegin());

      unindex();
      pg_log_t::clear();
      rollback_info_trimmed_to_riter = log.rbegin();
      reset_recovery_pointers();
    }

    // called by
    // PGLog::clear, which never used
    void clear() {
      skip_can_rollback_to_to_head();
      zero();
    }

    void reset_recovery_pointers() {
      complete_to = log.end();
      last_requested = 0;
    }

    // never called
    bool logged_object(const hobject_t& oid) const {
      if (!(indexed_data & PGLOG_INDEXED_OBJECTS)) {
         index_objects();
      }
      return objects.count(oid);
    }

    // called by
    // IndexedLog::print, for assert only
    bool logged_req(const osd_reqid_t &r) const {
      if (!(indexed_data & PGLOG_INDEXED_CALLER_OPS)) {
        index_caller_ops();
      }
      if (!caller_ops.count(r)) {
        if (!(indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS)) {
          index_extra_caller_ops();
        }
        return extra_caller_ops.count(r);
      }
      return true;
    }

    // called by
    // PG::check_in_progress_op
    bool get_request(
      const osd_reqid_t &r,
      eversion_t *version,
      version_t *user_version,
      int *return_code) const {
      assert(version);
      assert(user_version);
      assert(return_code);

      ceph::unordered_map<osd_reqid_t,pg_log_entry_t*>::const_iterator p;
      if (!(indexed_data & PGLOG_INDEXED_CALLER_OPS)) {
        index_caller_ops();
      }
      p = caller_ops.find(r);
      if (p != caller_ops.end()) {
	*version = p->second->version;
	*user_version = p->second->user_version;
	*return_code = p->second->return_code;
	return true;
      }

      // warning: we will return *a* request for this reqid, but not
      // necessarily the most recent.
      if (!(indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS)) {
        index_extra_caller_ops();
      }
      p = extra_caller_ops.find(r);
      if (p != extra_caller_ops.end()) { // for cache only, see PrimaryLogPG::finish_ctx
	for (auto i = p->second->extra_reqids.begin();
	     i != p->second->extra_reqids.end();
	     ++i) {
	  if (i->first == r) {
	    *version = p->second->version;
	    *user_version = i->second;
	    *return_code = p->second->return_code;
	    return true;
	  }
	}
	assert(0 == "in extra_caller_ops but not extra_reqids");
      }

      return false;
    }

    // called by
    // PrimaryLogPG::fill_in_copy_get
    // PrimaryLogPG::fill_in_copy_get_noent
    /// get a (bounded) list of recent reqids for the given object
    void get_object_reqids(const hobject_t& oid, unsigned max,
			   mempool::osd_pglog::vector<pair<osd_reqid_t, version_t> > *pls) const {
       // make sure object is present at least once before we do an
       // O(n) search.
      if (!(indexed_data & PGLOG_INDEXED_OBJECTS)) {
        index_objects();
      }
      if (objects.count(oid) == 0)
	return;
      for (list<pg_log_entry_t>::const_reverse_iterator i = log.rbegin();
           i != log.rend();
           ++i) {
	if (i->soid == oid) {
	  if (i->reqid_is_indexed())
	    pls->push_back(make_pair(i->reqid, i->user_version));
	  pls->insert(pls->end(), i->extra_reqids.begin(), i->extra_reqids.end());
	  if (pls->size() >= max) {
	    if (pls->size() > max) {
	      pls->resize(max);
	    }
	    return;
	  }
	}
      }
    }
    
    // called by
    // IndexedLog::index_objects
    // IndexedLog::index_caller_ops
    // IndexedLog::index_extra_caller_ops
    // IndexedLog::IndexedLog::split_out_child
    // IndexedLog::IndexedLog
    // IndexedLog::rewind_from_head
    // IndexedLog::claim_log_and_clear_rollback_info
    // (re)init index
    void index(__u16 to_index = PGLOG_INDEXED_ALL) const {
      if (to_index & PGLOG_INDEXED_OBJECTS)
	objects.clear();
      if (to_index & PGLOG_INDEXED_CALLER_OPS)
	caller_ops.clear();
      if (to_index & PGLOG_INDEXED_EXTRA_CALLER_OPS)
	extra_caller_ops.clear();

      for (list<pg_log_entry_t>::const_iterator i = log.begin();
	   i != log.end();
	   ++i) {
	if (to_index & PGLOG_INDEXED_OBJECTS) {
	  if (i->object_is_indexed()) { // !is_error()
	    objects[i->soid] = const_cast<pg_log_entry_t*>(&(*i));
	  }
	}

	if (to_index & PGLOG_INDEXED_CALLER_OPS) {
	  if (i->reqid_is_indexed()) {
	    caller_ops[i->reqid] = const_cast<pg_log_entry_t*>(&(*i));
	  }
	}
        
	if (to_index & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
	  for (auto j = i->extra_reqids.begin();
	       j != i->extra_reqids.end();
	       ++j) {
            extra_caller_ops.insert(
	      make_pair(j->first, const_cast<pg_log_entry_t*>(&(*i))));
	  }
	}
      }
        
      indexed_data |= to_index;
    }

    // called by
    // IndexedLog::logged_object, which never called
    // IndexedLog::get_object_reqids, for cache tier only
    void index_objects() const {
      index(PGLOG_INDEXED_OBJECTS);
    }

    // called by
    // IndexedLog::logged_req, debug only
    // IndexedLog::get_request, which called by PG::check_in_progress_op
    void index_caller_ops() const {
      index(PGLOG_INDEXED_CALLER_OPS);
    }

    // called by
    // IndexedLog::logged_req, debug only
    // IndexedLog::get_request, which called by PG::check_in_progress_op
    void index_extra_caller_ops() const {
      index(PGLOG_INDEXED_EXTRA_CALLER_OPS);
    }

    // called by
    // PGLog::merge_log
    void index(pg_log_entry_t& e) {
      if ((indexed_data & PGLOG_INDEXED_OBJECTS) && e.object_is_indexed()) {
        if (objects.count(e.soid) == 0 ||
            objects[e.soid]->version < e.version)
          objects[e.soid] = &e;
      }
      if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
	// divergent merge_log indexes new before unindexing old
        if (e.reqid_is_indexed()) {
	  caller_ops[e.reqid] = &e;
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (auto j = e.extra_reqids.begin();
	     j != e.extra_reqids.end();
	     ++j) {
	  extra_caller_ops.insert(make_pair(j->first, &e));
        }
      }
    }

    // called by
    // IndexedLog::zero, IndexedLog::clear <- PGLog::clear, which never used
    // IndexedLog::split_out_child
    void unindex() {
      objects.clear();
      caller_ops.clear();
      extra_caller_ops.clear();
      indexed_data = 0;
    }

    // called by
    // IndexedLog::trim
    void unindex(pg_log_entry_t& e) {
      // NOTE: this only works if we remove from the _tail_ of the log!
      if (indexed_data & PGLOG_INDEXED_OBJECTS) {
        if (objects.count(e.soid) && objects[e.soid]->version == e.version)
          objects.erase(e.soid);
      }
      if (e.reqid_is_indexed()) {
        if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
	  // divergent merge_log indexes new before unindexing old
          if (caller_ops.count(e.reqid) && caller_ops[e.reqid] == &e)
            caller_ops.erase(e.reqid);    
        }
      }
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (auto j = e.extra_reqids.begin();
             j != e.extra_reqids.end();
             ++j) {
          for (ceph::unordered_multimap<osd_reqid_t,pg_log_entry_t*>::iterator k =
		 extra_caller_ops.find(j->first);
               k != extra_caller_ops.end() && k->first == j->first;
               ++k) {
            if (k->second == &e) {
              extra_caller_ops.erase(k);
              break;
            }
          }
        }
      }
    }

    // called by
    // IndexedLog::append_log_entries_update_missing
    // PGLog::add, which called by PG::add_log_entry <- PG::append_log <- PrimaryLogPG::log_operation
    // PrimaryLogPG::issue_repop, for PG::projected_log, applied = true
    void add(const pg_log_entry_t& e, bool applied = true) {
      if (!applied) {
	assert(get_can_rollback_to() == head);
      }

      // make sure our buffers don't pin bigger buffers
      e.mod_desc.trim_bl();

      // add to log
      log.push_back(e); // mempool::osd::list<pg_log_entry_t> pg_log_t::log

      // riter previously pointed to the previous entry
      if (rollback_info_trimmed_to_riter == log.rbegin())
	++rollback_info_trimmed_to_riter;

      assert(e.version > head);
      assert(head.version == 0 || e.version.version > head.version);

      head = e.version;

      // to our index
      if ((indexed_data & PGLOG_INDEXED_OBJECTS) && e.object_is_indexed()) {
        // this is the only difference from IndexedLog::index(pg_log_entry_t& e), i.e., no
        // need to check the update condition
        objects[e.soid] = &(log.back());
      }

      if (indexed_data & PGLOG_INDEXED_CALLER_OPS) {
        if (e.reqid_is_indexed()) {
	  caller_ops[e.reqid] = &(log.back());
        }
      }
      
      if (indexed_data & PGLOG_INDEXED_EXTRA_CALLER_OPS) {
        for (auto j = e.extra_reqids.begin();
	     j != e.extra_reqids.end();
	     ++j) {
	  extra_caller_ops.insert(make_pair(j->first, &(log.back())));
        }
      }

      if (!applied) {
        // advance pg_log_t::can_rollback_to and pg_log_t::rollback_info_trimmed_to
        // to updated pg_log_t::head
	skip_can_rollback_to_to_head();
      }
    }

    // called by
    // PG::append_log
    // PGLog::trim
    void trim(
      CephContext* cct,
      eversion_t s,
      set<eversion_t> *trimmed);

    ostream& print(ostream& out) const;
  }; // struct IndexedLog : public pg_log_t


protected:
  //////////////////// data members ////////////////////

  pg_missing_tracker_t missing;

  // PG::projected_log has another instance of IndexedLog
  IndexedLog  log;

  eversion_t dirty_to;         ///< must clear/writeout all keys <= dirty_to
  eversion_t dirty_from;       ///< must clear/writeout all keys >= dirty_from
  eversion_t writeout_from;    ///< must writout keys >= writeout_from
  set<eversion_t> trimmed;     ///< must clear keys in trimmed
  CephContext *cct;
  bool pg_log_debug;
  /// Log is clean on [dirty_to, dirty_from)
  bool touched_log;
  bool clear_divergent_priors;

  void mark_dirty_to(eversion_t to) {
    if (to > dirty_to)
      dirty_to = to;
  }
  void mark_dirty_from(eversion_t from) {
    if (from < dirty_from)
      dirty_from = from;
  }
  void mark_writeout_from(eversion_t from) {
    if (from < writeout_from)
      writeout_from = from;
  }
public:
  bool is_dirty() const {
    return !touched_log ||
      (dirty_to != eversion_t()) ||
      (dirty_from != eversion_t::max()) ||
      (writeout_from != eversion_t::max()) ||
      !(trimmed.empty()) ||
      !missing.is_clean();
  }
  void mark_log_for_rewrite() {
    mark_dirty_to(eversion_t::max());
    mark_dirty_from(eversion_t());
    touched_log = false;
  }
protected:

  /// DEBUG
  set<string> log_keys_debug;
  static void clear_after(set<string> *log_keys_debug, const string &lb) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->lower_bound(lb);
	 i != log_keys_debug->end();
	 log_keys_debug->erase(i++));
  }
  static void clear_up_to(set<string> *log_keys_debug, const string &ub) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->begin();
	 i != log_keys_debug->end() && *i < ub;
	 log_keys_debug->erase(i++));
  }

  void check();

  void undirty() {
    dirty_to = eversion_t();
    dirty_from = eversion_t::max();
    touched_log = true;
    trimmed.clear();
    writeout_from = eversion_t::max();
    check(); // debug use only
    missing.flush();
  }

public:
  // called by
  // PG::PG
  // cppcheck-suppress noExplicitConstructor
  PGLog(CephContext *cct, DoutPrefixProvider *dpp = 0) :
    prefix_provider(dpp),
    dirty_from(eversion_t::max()),
    writeout_from(eversion_t::max()),
    cct(cct),
    pg_log_debug(!(cct && !(cct->_conf->osd_debug_pg_log_writeout))),
    touched_log(false),
    clear_divergent_priors(false) {}


  void reset_backfill();

  void clear();

  //////////////////// get or set missing ////////////////////

  const pg_missing_tracker_t& get_missing() const { return missing; } // pg_missing_tracker_t

  // never called
  void revise_have(hobject_t oid, eversion_t have) {
    missing.revise_have(oid, have);
  }

  void revise_need(hobject_t oid, eversion_t need) {
    missing.revise_need(oid, need);
  }

  // called by
  // PG::repair_object
  // PrimaryLogPG::prep_object_replica_pushes
  void missing_add(const hobject_t& oid, eversion_t need, eversion_t have) {
    missing.add(oid, need, have);
  }

  //////////////////// get or set log ////////////////////

  const IndexedLog &get_log() const { return log; }

  const eversion_t &get_tail() const { return log.tail; }

  // never called
  void set_tail(eversion_t tail) { log.tail = tail; }

  const eversion_t &get_head() const { return log.head; }

  // never called
  void set_head(eversion_t head) { log.head = head; }

  // called by
  // PrimaryLogPG::recover_primary
  // PrimaryLogPG::cancel_pull, set to 0
  // PG::repair_object, set to 0
  void set_last_requested(version_t last_requested) {
    log.last_requested = last_requested;
  }

  // never called
  void index() { log.index(); }

  // never called
  void unindex() { log.unindex(); }

  // called by
  // PG::add_log_entry, which called by PG::append_log, which called by PrimaryLogPG::log_operation
  void add(const pg_log_entry_t& e, bool applied = true) {
    mark_writeout_from(e.version);

    log.add(e, applied); // push back of log entry list, update pg_log_t::head
  }

  // called by
  // PG::clear_primary_state
  // PG::clear_recovery_state
  // PG::activate
  void reset_recovery_pointers() { log.reset_recovery_pointers(); }

  // called by
  // OSD::RemoveWQ::_process
  static void clear_info_log(
    spg_t pgid,
    ObjectStore::Transaction *t);

  void trim(
    eversion_t trim_to,
    pg_info_t &info);

  // called by
  // PG::append_log
  // PrimaryLogPG::on_local_recover
  // PGLog::roll_forward, i.e., below, which set roll_forward_to to log.head
  void roll_forward_to(
    eversion_t roll_forward_to,
    LogEntryHandler *h) {
    // advance pg_log_t::can_rollback_to, pg_log_t::rollback_info_trimmed_to
    // IndexedLog::rollback_info_trimmed_to_riter
    log.roll_forward_to(
      roll_forward_to,
      h);
  }

  // called by
  // PG::activate
  // PG::append_log
  // PrimaryLogPG::on_local_recover
  // PrimaryLogPG::calc_trim_to
  eversion_t get_can_rollback_to() const {
    // pg_log_t::can_rollback_to
    return log.get_can_rollback_to();
  }

  // called by
  // PG::activate
  // PG::append_log
  // PrimaryLogPG::on_removal
  void roll_forward(LogEntryHandler *h) {
    // advance pg_log_t::can_rollback_to, pg_log_t::rollback_info_trimmed_to
    // IndexedLog::rollback_info_trimmed_to_riter
    roll_forward_to(
      log.head,
      h);
  }

  //////////////////// get or set log & missing ////////////////////

  // called by
  // PG::RecoveryState::Stray::react(const MLogRec)
  void reset_backfill_claim_log(const pg_log_t &o, LogEntryHandler *h) {
    log.trim_rollback_info_to(log.head, h);
    log.claim_log_and_clear_rollback_info(o);
    missing.clear();
    mark_dirty_to(eversion_t::max());
  }

  // called by
  // PG::split_into
  void split_into(
      pg_t child_pgid,
      unsigned split_bits,
      PGLog *opg_log) { 
    log.split_out_child(child_pgid, split_bits, &opg_log->log);
    missing.split_into(child_pgid, split_bits, &(opg_log->missing));
    opg_log->mark_dirty_to(eversion_t::max());
    mark_dirty_to(eversion_t::max());
  }

  // called by
  // PrimaryLogPG::recover_got
  void recover_got(hobject_t oid, eversion_t v, pg_info_t &info) {
    if (missing.is_missing(oid, v)) {
      missing.got(oid, v);
      
      // raise last_complete?
      if (missing.get_items().empty()) {
	log.complete_to = log.log.end();
	info.last_complete = info.last_update;
      }

      while (log.complete_to != log.log.end()) {
	if (missing.get_items().at(
	      missing.get_rmissing().begin()->second
	      ).need <= log.complete_to->version)
	  break;

	if (info.last_complete < log.complete_to->version)
	  info.last_complete = log.complete_to->version;

	++log.complete_to;
      }
    }

    assert(log.get_can_rollback_to() >= v);
  }

  // called by
  // PG::activate, when missing.num_missing() != 0
  void activate_not_complete(pg_info_t &info) {
    // log entries are pushed back of IndexedLog::log, IndexedLog::head points to
    // the newest entry

    log.complete_to = log.log.begin(); // points to the oldest entry

    while (log.complete_to->version <
	   missing.get_items().at(
	     missing.get_rmissing().begin()->second // hobject_t
	     ).need)
      ++log.complete_to;

    assert(log.complete_to != log.log.end());

    if (log.complete_to == log.log.begin()) {
      info.last_complete = eversion_t();
    } else {
      --log.complete_to;
      info.last_complete = log.complete_to->version;
      ++log.complete_to;
    }

    log.last_requested = 0;
  }

  void proc_replica_log(pg_info_t &oinfo,
			const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from) const;

protected:
  // called by
  // PGLog::_merge_divergent_entries
  static void split_by_object(
    mempool::osd_pglog::list<pg_log_entry_t> &entries,
    map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t>> *out_entries) {
    while (!entries.empty()) {
      auto &out_list = (*out_entries)[entries.front().soid];
      out_list.splice(out_list.end(), entries, entries.begin());
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
  // called by
  // PGLog::_merge_divergent_entries
  template <typename missing_type>
  static void _merge_object_divergent_entries(
    const IndexedLog &log,               ///< [in] log to merge against
    const hobject_t &hoid,               ///< [in] object we are merging
    const mempool::osd_pglog::list<pg_log_entry_t> &orig_entries, ///< [in] entries for hoid to merge
    const pg_info_t &info,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &missing,              ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    ldpp_dout(dpp, 20) << __func__ << ": merging hoid " << hoid
		       << " entries: " << orig_entries << dendl;

    if (hoid > info.last_backfill) {
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " after last_backfill"
			 << dendl;
      return;
    }

    // entries is non-empty
    assert(!orig_entries.empty());
    // strip out and ignore ERROR entries
    mempool::osd_pglog::list<pg_log_entry_t> entries;
    eversion_t last;
    for (list<pg_log_entry_t>::const_iterator i = orig_entries.begin();
	 i != orig_entries.end();
	 ++i) {
      // all entries are on hoid
      assert(i->soid == hoid);
      if (i != orig_entries.begin() && i->prior_version != eversion_t()) {
	// in increasing order of version
	assert(i->version > last);
	// prior_version correct (unless it is an ERROR entry)
	assert(i->prior_version == last || i->is_error());
      }

      last = i->version;
      if (i->is_error()) {
	ldpp_dout(dpp, 20) << __func__ << ": ignoring " << *i << dendl;
      } else {
	ldpp_dout(dpp, 20) << __func__ << ": keeping " << *i << dendl;
	entries.push_back(*i);
      }
    }
    if (entries.empty()) {
      ldpp_dout(dpp, 10) << __func__ << ": no non-ERROR entries" << dendl;
      return;
    }

    const eversion_t prior_version = entries.begin()->prior_version;
    const eversion_t first_divergent_update = entries.begin()->version;
    const eversion_t last_divergent_update = entries.rbegin()->version;

    const bool object_not_in_store =
      !missing.is_missing(hoid) &&
      entries.rbegin()->is_delete();

    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       << " prior_version: " << prior_version
		       << " first_divergent_update: " << first_divergent_update
		       << " last_divergent_update: " << last_divergent_update
		       << dendl;

    ceph::unordered_map<hobject_t, pg_log_entry_t*>::const_iterator objiter =
      log.objects.find(hoid);
    if (objiter != log.objects.end() &&
	objiter->second->version >= first_divergent_update) {
      /// Case 1)
      ldpp_dout(dpp, 10) << __func__ << ": more recent entry found: "
			 << *objiter->second << ", already merged" << dendl;

      assert(objiter->second->version > last_divergent_update);

      // ensure missing has been updated appropriately
      if (objiter->second->is_update()) {
	assert(missing.is_missing(hoid) &&
	       missing.get_items().at(hoid).need == objiter->second->version);
      } else {
	assert(!missing.is_missing(hoid));
      }

      missing.revise_have(hoid, eversion_t());

      if (rollbacker) {
	if (!object_not_in_store) {
	  rollbacker->remove(hoid);
	}

	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      return;
    }

    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       <<" has no more recent entries in log" << dendl;

    if (prior_version == eversion_t() || entries.front().is_clone()) {
      /// Case 2)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " prior_version or op type indicates creation,"
			 << " deleting"
			 << dendl;

      if (missing.is_missing(hoid))
	missing.rm(missing.get_items().find(hoid));

      if (rollbacker) {
	if (!object_not_in_store) {
	  rollbacker->remove(hoid);
	}

	for (auto &&i: entries) {
	  // remove rollback object
	  rollbacker->trim(i);
	}
      }
      return;
    }

    if (missing.is_missing(hoid)) {
      /// Case 3)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " missing, " << missing.get_items().at(hoid)
			 << " adjusting" << dendl;

      if (missing.get_items().at(hoid).have == prior_version) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " missing.have is prior_version " << prior_version
			   << " removing from missing" << dendl;

	missing.rm(missing.get_items().find(hoid));
      } else {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " missing.have is " << missing.get_items().at(hoid).have
			   << ", adjusting" << dendl;

	missing.revise_need(hoid, prior_version);

	if (prior_version <= info.log_tail) {
	  ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			     << " prior_version " << prior_version
			     << " <= info.log_tail "
			     << info.log_tail << dendl;
	}
      }

      if (rollbacker) {
	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }
      return;
    }

    ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
		       << " must be rolled back or recovered,"
		       << " attempting to rollback"
		       << dendl;

    bool can_rollback = true;
    /// Distinguish between 4) and 5)
    for (list<pg_log_entry_t>::const_reverse_iterator i = entries.rbegin();
	 i != entries.rend();
	 ++i) {
      if (!i->can_rollback() || i->version <= olog_can_rollback_to) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " cannot rollback "
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
	assert(i->can_rollback() && i->version > olog_can_rollback_to);

	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " rolling back " << *i << dendl;

	if (rollbacker)
	  rollbacker->rollback(*i);
      }

      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			 << " rolled back" << dendl;
      return;
    } else {
      /// Case 5)
      ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid << " cannot roll back, "
			 << "removing and adding to missing" << dendl;
      if (rollbacker) {
	if (!object_not_in_store)
	  rollbacker->remove(hoid);

	for (auto &&i: entries) {
	  rollbacker->trim(i);
	}
      }

      missing.add(hoid, prior_version, eversion_t());

      if (prior_version <= info.log_tail) {
	ldpp_dout(dpp, 10) << __func__ << ": hoid " << hoid
			   << " prior_version " << prior_version
			   << " <= info.log_tail "
			   << info.log_tail << dendl;
      }
    }
  } // static void _merge_object_divergent_entries

  // called by
  // PGLog::proc_replica_log, which called by PG::proc_replica_log, with rollbacker = nullptr
  // PGLog::rewind_divergent_log, which called by PG::rewind_divergent_log/PGLog::merge_log
  // PGLog::merge_log, which called by PG::merge_log <- PG::proc_master_log
  /// Merge all entries using above
  template <typename missing_type>
  static void _merge_divergent_entries(
    const IndexedLog &log,               ///< [in] log to merge against
    mempool::osd_pglog::list<pg_log_entry_t> &entries,       ///< [in] entries to merge
    const pg_info_t &oinfo,              ///< [in] info for merging entries
    eversion_t olog_can_rollback_to,     ///< [in] rollback boundary
    missing_type &omissing,              ///< [in,out] missing to adjust, use
    LogEntryHandler *rollbacker,         ///< [in] optional rollbacker object
    const DoutPrefixProvider *dpp        ///< [in] logging provider
    ) {
    map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t> > split;
    // entries classified by hobject_t
    split_by_object(entries, &split);
    for (map<hobject_t, mempool::osd_pglog::list<pg_log_entry_t>>::iterator i = split.begin();
	 i != split.end();
	 ++i) {
      _merge_object_divergent_entries(
	log,
	i->first,       // hobject_t
	i->second,      // log entries on this hobject_t
	oinfo,
	olog_can_rollback_to,
	omissing,
	rollbacker,
	dpp);
    }
  }

  /**
   * Exists for use in TestPGLog for simply testing single divergent log
   * cases
   */
  void merge_old_entry(
    ObjectStore::Transaction& t,
    const pg_log_entry_t& oe,
    const pg_info_t& info,
    LogEntryHandler *rollbacker) {
    mempool::osd_pglog::list<pg_log_entry_t> entries;
    entries.push_back(oe);
    _merge_object_divergent_entries(
      log,
      oe.soid,
      entries,
      info,
      log.get_can_rollback_to(),
      missing,
      rollbacker,
      this);
  }

public:
  // called by
  // PG::rewind_divergent_log
  // PGLog::merge_log
  void rewind_divergent_log(eversion_t newhead,
                            pg_info_t &info,
                            LogEntryHandler *rollbacker,
                            bool &dirty_info,
                            bool &dirty_big_info);

  // called by
  // PG::merge_log
  void merge_log(pg_info_t &oinfo,
		 pg_log_t &olog,
		 pg_shard_t from,
		 pg_info_t &info, LogEntryHandler *rollbacker,
		 bool &dirty_info, bool &dirty_big_info);

  // called by
  // PGLog::append_new_log_entries, maintain_rollback=true, which called by PG::append_log_entries_update_missing
  // PGLog::merge_log, maintain_rollback=false, which called by PG::merge_log
  // PG::merge_new_log_entries, maintain_rollback=true, which called by PrimaryLogPG::submit_log_entries
  template <typename missing_type>
  static bool append_log_entries_update_missing(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    bool maintain_rollback,
    IndexedLog *log,
    missing_type &missing,
    LogEntryHandler *rollbacker,
    const DoutPrefixProvider *dpp) {
    bool invalidate_stats = false;

    if (log && !entries.empty()) {
      assert(log->head < entries.begin()->version);
    }

    for (list<pg_log_entry_t>::const_iterator p = entries.begin();
	 p != entries.end();
	 ++p) {
      invalidate_stats = invalidate_stats || !p->is_error();

      if (log) {
	ldpp_dout(dpp, 20) << "update missing, append " << *p << dendl;
	log->add(*p);
      }

      if (p->soid <= last_backfill &&
	  !p->is_error()) {
	missing.add_next_event(*p);

	if (rollbacker) {
	  // hack to match PG::mark_all_unfound_lost
	  if (maintain_rollback && p->is_lost_delete() && p->can_rollback()) {
	    rollbacker->try_stash(p->soid, p->version.version);
	  } else if (p->is_delete()) {
	    rollbacker->remove(p->soid);
	  }
	}
      }
    }

    return invalidate_stats;
  }

  // called by
  // PG::append_log_entries_update_missing
  bool append_new_log_entries(
    const hobject_t &last_backfill,
    bool last_backfill_bitwise,
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    LogEntryHandler *rollbacker) {
    bool invalidate_stats = append_log_entries_update_missing(
      last_backfill,
      last_backfill_bitwise,
      entries,
      true,
      &log,
      missing,
      rollbacker,
      this);

    if (!entries.empty()) {
      mark_writeout_from(entries.begin()->version);
    }

    return invalidate_stats;
  }

  void write_log_and_missing(ObjectStore::Transaction& t,
		 map<string,bufferlist> *km,
		 const coll_t& coll,
		 const ghobject_t &log_oid,
		 bool require_rollback);

  static void write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors,
    bool require_rollback);

  static void write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid,
    const pg_missing_tracker_t &missing,
    bool require_rollback);

  static void _write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors,
    eversion_t dirty_to,
    eversion_t dirty_from,
    eversion_t writeout_from,
    const set<eversion_t> &trimmed,
    bool dirty_divergent_priors,
    bool touch_log,
    bool require_rollback,
    set<string> *log_keys_debug
    );

  static void _write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist>* km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    eversion_t dirty_to,
    eversion_t dirty_from,
    eversion_t writeout_from,
    const set<eversion_t> &trimmed,
    const pg_missing_tracker_t &missing,
    bool touch_log,
    bool require_rollback,
    bool clear_divergent_priors,
    set<string> *log_keys_debug
    );

  // called by
  // PG::read_state
  void read_log_and_missing(
    ObjectStore *store,
    coll_t pg_coll,     // PG::coll
    coll_t log_coll,    // PG::coll
    ghobject_t log_oid, // PG::pgmeta_oid
    const pg_info_t &info,
    ostringstream &oss,
    bool tolerate_divergent_missing_log,        // false
    bool debug_verify_stored_missing = false    // false
    ) {
    return read_log_and_missing(
      store, pg_coll, log_coll, log_oid, info,
      log, missing, oss,
      tolerate_divergent_missing_log,
      &clear_divergent_priors,
      this,
      (pg_log_debug ? &log_keys_debug : 0),
      debug_verify_stored_missing);
  }

  // called by
  // PGLog::read_log_and_missing, i.e., above
  // ceph_objectstore_tool.cc/get_log
  template <typename missing_type>
  static void read_log_and_missing(ObjectStore *store,
    coll_t pg_coll,
    coll_t log_coll,
    ghobject_t log_oid,
    const pg_info_t &info,
    IndexedLog &log,
    missing_type &missing, ostringstream &oss,
    bool tolerate_divergent_missing_log, // _conf->osd_ignore_stale_divergent_priors, i.e., false, for both caller
    bool *clear_divergent_priors = NULL,
    const DoutPrefixProvider *dpp = NULL,
    set<string> *log_keys_debug = 0,
    bool debug_verify_stored_missing = false // false, for both caller
    ) {
    ldpp_dout(dpp, 20) << "read_log_and_missing coll " << pg_coll
		       << " log_oid " << log_oid << dendl;

    // legacy?
    struct stat st;
    int r = store->stat(log_coll, log_oid, &st);
    assert(r == 0);
    assert(st.st_size == 0);

    // will get overridden below if it had been recorded
    eversion_t on_disk_can_rollback_to = info.last_update;
    eversion_t on_disk_rollback_info_trimmed_to = eversion_t();

    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(log_coll, log_oid);

    map<eversion_t, hobject_t> divergent_priors;
    bool has_divergent_priors = false;
    list<pg_log_entry_t> entries;
    if (p) {
      for (p->seek_to_first(); p->valid() ; p->next(false)) {
	// non-log pgmeta_oid keys are prefixed with _; skip those
	if (p->key()[0] == '_') // i.e., "_infover"/"_info"/"_biginfo"/"_epoch"/"_fastinfo"
	  continue;

	bufferlist bl = p->value();//Copy bufferlist before creating iterator
	bufferlist::iterator bp = bl.begin();

	if (p->key() == "divergent_priors") {
	  ::decode(divergent_priors, bp);

	  ldpp_dout(dpp, 20) << "read_log_and_missing " << divergent_priors.size()
			     << " divergent_priors" << dendl;

	  has_divergent_priors = true;
	  debug_verify_stored_missing = false;
	} else if (p->key() == "can_rollback_to") {
	  ::decode(on_disk_can_rollback_to, bp);
	} else if (p->key() == "rollback_info_trimmed_to") {
	  ::decode(on_disk_rollback_info_trimmed_to, bp);
	} else if (p->key().substr(0, 7) == string("missing")) {
	  pair<hobject_t, pg_missing_item> p;
	  ::decode(p, bp);

	  missing.add(p.first, p.second.need, p.second.have);
	} else {
	  pg_log_entry_t e;
	  e.decode_with_checksum(bp);

	  ldpp_dout(dpp, 20) << "read_log_and_missing " << e << dendl;

	  if (!entries.empty()) {
	    pg_log_entry_t last_e(entries.back());

	    assert(last_e.version.version < e.version.version);
	    assert(last_e.version.epoch <= e.version.epoch);
	  }

	  entries.push_back(e);

	  if (log_keys_debug)
	    log_keys_debug->insert(e.get_key_name());
	}
      }
    } // p

    log = IndexedLog(
      info.last_update,
      info.log_tail,
      on_disk_can_rollback_to,
      on_disk_rollback_info_trimmed_to,
      std::move(entries));

    if (has_divergent_priors || debug_verify_stored_missing) {
      // build missing
      if (debug_verify_stored_missing || info.last_complete < info.last_update) {
	ldpp_dout(dpp, 10) << "read_log_and_missing checking for missing items over interval ("
			   << info.last_complete
			   << "," << info.last_update << "]" << dendl;

	set<hobject_t> did;
	set<hobject_t> checked;
	set<hobject_t> skipped;

	for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	     i != log.log.rend();
	     ++i) { // start from the newest entry
	  if (!debug_verify_stored_missing && i->version <= info.last_complete) break;
	  if (i->soid > info.last_backfill)
	    continue;
	  if (i->is_error())
	    continue;
	  if (did.count(i->soid)) continue;

	  did.insert(i->soid);

	  if (i->is_delete()) continue;

	  bufferlist bv;
	  int r = store->getattr(
	    pg_coll,
	    ghobject_t(i->soid, ghobject_t::NO_GEN, info.pgid.shard),
	    OI_ATTR,
	    bv);
	  if (r >= 0) {
	    object_info_t oi(bv);
	    if (oi.version < i->version) {
	      ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i
				 << " (have " << oi.version << ")" << dendl;

	      if (debug_verify_stored_missing) {
		auto miter = missing.get_items().find(i->soid);
		assert(miter != missing.get_items().end());
		assert(miter->second.need == i->version);
		assert(miter->second.have == oi.version);
		checked.insert(i->soid);
	      } else {
		missing.add(i->soid, i->version, oi.version);
	      }
	    }
	  } else {
	    ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;

	    if (debug_verify_stored_missing) {
	      auto miter = missing.get_items().find(i->soid);
	      assert(miter != missing.get_items().end());
	      assert(miter->second.need == i->version);
	      assert(miter->second.have == eversion_t());
	      checked.insert(i->soid);
	    } else {
	      missing.add(i->soid, i->version, eversion_t());
	    }
	  }
	}

	if (debug_verify_stored_missing) {
	  for (auto &&i: missing.get_items()) {
	    if (checked.count(i.first))
	      continue;
	    if (i.second.need > log.tail ||
	      i.first > info.last_backfill) {
	      ldpp_dout(dpp, -1) << __func__ << ": invalid missing set entry found "
				 << i.first
				 << dendl;
	      assert(0 == "invalid missing set entry found");
	    }
	    bufferlist bv;
	    int r = store->getattr(
	      pg_coll,
	      ghobject_t(i.first, ghobject_t::NO_GEN, info.pgid.shard),
	      OI_ATTR,
	      bv);
	    if (r >= 0) {
	      object_info_t oi(bv);
	      assert(oi.version == i.second.have);
	    } else {
	      assert(eversion_t() == i.second.have);
	    }
	  }
	} else {
	  assert(has_divergent_priors);

	  for (map<eversion_t, hobject_t>::reverse_iterator i =
		 divergent_priors.rbegin();
	       i != divergent_priors.rend();
	       ++i) {
	    if (i->first <= info.last_complete) break;
	    if (i->second > info.last_backfill)
	      continue;
	    if (did.count(i->second)) continue;

	    did.insert(i->second);

	    bufferlist bv;
	    int r = store->getattr(
	      pg_coll,
	      ghobject_t(i->second, ghobject_t::NO_GEN, info.pgid.shard),
	      OI_ATTR,
	      bv);
	    if (r >= 0) {
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
	      	/**
		 * Unfortunately the assessment above is incorrect because of
		 * http://tracker.ceph.com/issues/17916 (we were incorrectly
		 * not removing the divergent_priors set from disk state!),
		 * so let's check that.
		 */
	      if (oi.version > i->first && tolerate_divergent_missing_log) {
		ldpp_dout(dpp, 0) << "read_log divergent_priors entry (" << *i
				  << ") inconsistent with disk state (" <<  oi
				  << "), assuming it is tracker.ceph.com/issues/17916"
				  << dendl;
	      } else {
		assert(oi.version == i->first);
	      }
	    } else {
	      ldpp_dout(dpp, 15) << "read_log_and_missing  missing " << *i << dendl;
	      missing.add(i->second, i->first, eversion_t());
	    }
	  }
	}

	if (clear_divergent_priors)
	  (*clear_divergent_priors) = true;
      }
    } // if (has_divergent_priors || debug_verify_stored_missing)

    if (!has_divergent_priors) {
      if (clear_divergent_priors)
	(*clear_divergent_priors) = false;

      missing.flush();
    }

    ldpp_dout(dpp, 10) << "read_log_and_missing done" << dendl;
  }
}; // struct PGLog : DoutPrefixProvider

#endif // CEPH_PG_LOG_H
