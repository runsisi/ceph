// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"
#include "common/EventTrace.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

namespace {
// created by
// ReplicatedBackend::_do_push
// ReplicatedBackend::_do_pull_response
// ReplicatedBackend::sub_op_push
class PG_SendMessageOnConn: public Context {
  PGBackend::Listener *pg;
  Message *reply;
  ConnectionRef conn;
  public:
  PG_SendMessageOnConn(
    PGBackend::Listener *pg,
    Message *reply,
    ConnectionRef conn) : pg(pg), reply(reply), conn(conn) {}
  void finish(int) override {
    pg->send_message_osd_cluster(reply, conn.get());
  }
};

// created by
// ReplicatedBackend::_do_pull_response
// ReplicatedBackend::sub_op_push
class PG_RecoveryQueueAsync : public Context {
  PGBackend::Listener *pg;
  unique_ptr<GenContext<ThreadPool::TPHandle&>> c;
  public:
  PG_RecoveryQueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c) : pg(pg), c(c) {}
  void finish(int) override {
    pg->schedule_recovery_work(c.release());
  }
};
}

// created by
// ReplicatedBackend::do_repop
struct ReplicatedBackend::C_OSD_RepModifyApply : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyApply(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) override {
    pg->repop_applied(rm);
  }
};

// created by
// ReplicatedBackend::do_repop
struct ReplicatedBackend::C_OSD_RepModifyCommit : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyCommit(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) override {
    pg->repop_commit(rm);
  }
};

// called by
// ReplicatedBackend::sub_op_modify_commit
// ReplicatedBackend::sub_op_pull
static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int subop)
{
  utime_t now = ceph_clock_now();
  utime_t latency = now;
  latency -= op->get_req()->get_recv_stamp();


  logger->inc(l_osd_sop);
  logger->tinc(l_osd_sop_lat, latency);
  logger->inc(subop);

  if (subop != l_osd_sop_pull) {
    uint64_t inb = op->get_req()->get_data().length();
    logger->inc(l_osd_sop_inb, inb);
    if (subop == l_osd_sop_w) {
      logger->inc(l_osd_sop_w_inb, inb);
      logger->tinc(l_osd_sop_w_lat, latency);
    } else if (subop == l_osd_sop_push) {
      logger->inc(l_osd_sop_push_inb, inb);
      logger->tinc(l_osd_sop_push_lat, latency);
    } else
      assert("no support subop" == 0);
  } else {
    logger->tinc(l_osd_sop_pull_lat, latency);
  }
}

// created by
// PGBackend::build_pg_backend, which called by PrimaryLogPG::PrimaryLogPG, which
// created by OSD::_make_pg
ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  ObjectStore::CollectionHandle &c,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(cct, pg, store, coll, c) {}

// called by
// PrimaryLogPG::maybe_kick_recovery
// PrimaryLogPG::recover_primary, which called by PrimaryLogPG::start_recovery_ops
// PrimaryLogPG::recover_replicas, which called by PrimaryLogPG::start_recovery_ops
// PrimaryLogPG::recover_backfill, which called by PrimaryLogPG::start_recovery_ops
// C_ReplicatedBackend_OnPullComplete::finish
// ReplicatedBackend::sub_op_push
void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  RPGHandle *h = static_cast<RPGHandle *>(_h);

  // send MOSDPGPush
  send_pushes(priority, h->pushes);

  // send MOSDPGPull
  send_pulls(priority, h->pulls);

  delete h;
}

/*
 * PrimaryLogPG::recover_primary                 // iterate PG::missing to select primary objects to recover
 *      PrimaryLogPG::recover_missing            // recover single primary object
 *              pgbackend->recover_objct         // test if to recover primary object or replica object
 *                                               // then prepare pull or push accordingly
 *
 * PrimaryLogPG::recover_replicas                // iterate PG::peer_missing to select replica objects to recover
 *      PrimaryLogPG::prep_object_replica_pushes // recover single replica object
 *              pgbackend->recover_objct         // test if to recover primary object or replica object
 *                                               // then prepare pull or push accordingly
 */

// called by
// PrimaryLogPG::recover_missing, i.e., recover primary object
//      head:   not null to recover snap object, null to recover head
//      obc:    null
// PrimaryLogPG::prep_object_replica_pushes
//      head:   null
//      obc:    not null
// PrimaryLogPG::prep_backfill_object_push
//      head:   null
//      obc:    not null
int ReplicatedBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;

  RPGHandle *h = static_cast<RPGHandle *>(_h);

  // PrimaryLogPG::maybe_kick_recovery will do the same test to
  // kick the primary object to recovery
  if (get_parent()->get_local_missing().is_missing(hoid)) {
    // primary missing it, so the caller is PrimaryLogPG::recover_missing

    assert(!obc);

    // pull, either pull head/snapdir or snap object, if head is null which
    // means we are pull the head/snapdir, else we are pll the snap object
    prepare_pull(
      v,
      hoid,
      head, // null or not null determined by if we are to pull head/snapdir or not
      h);
  } else {
    // replica missing it, so the caller is PrimaryLogPG::prep_object_replica_pushes or
    // PrimaryLogPG::prep_backfill_object_push

    assert(obc);

    int started = start_pushes(
      hoid,
      obc, // not null
      h);
    if (started < 0) {
      pushing[hoid].clear();
      return started;
    }
  }
  return 0;
}

// called by
// PrimaryLogPG::check_recovery_sources
void ReplicatedBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  for(map<pg_shard_t, set<hobject_t> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first.osd)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	get_parent()->cancel_pull(*j);
	clear_pull(pulling.find(*j), false);
      }

      pull_from_peer.erase(i++);
    } else {
      ++i;
    }
  }
}

// called by
// PrimaryLogPG::do_request
bool ReplicatedBackend::can_handle_while_inactive(OpRequestRef op)
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PULL:
    return true;
  default:
    return false;
  }
}

// called by
// PrimaryLogPG::do_request, i.e., the Op has been dequeued from
// the OSD::OpShardedWQ, so the method name may be a little misleading
bool ReplicatedBackend::handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << op << dendl;

  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    do_push(op);
    return true;

  case MSG_OSD_PG_PULL:
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY:
    do_push_reply(op);
    return true;

  case MSG_OSD_SUBOP: {
    const MOSDSubOp *m = static_cast<const MOSDSubOp*>(op->get_req());
    if (m->ops.size() == 0) {
      assert(0);
    }
    break;
  }

  case MSG_OSD_REPOP: {
    do_repop(op);
    return true;
  }

  case MSG_OSD_REPOPREPLY: {
    do_repop_reply(op);
    return true;
  }

  default:
    break;
  }
  return false;
}

// called by
// ReplicatedBackend::on_change
// PrimaryLogPG::_clear_recovery_state, which called by PG::clear_recovery_state
void ReplicatedBackend::clear_recovery_state()
{
  // clear pushing/pulling maps
  for (auto &&i: pushing) {
    for (auto &&j: i.second) {
      get_parent()->release_locks(j.second.lock_manager);
    }
  }
  pushing.clear();

  for (auto &&i: pulling) {
    get_parent()->release_locks(i.second.lock_manager);
  }
  pulling.clear();
  pull_from_peer.clear();
}

// called by
// PrimaryLogPG::on_shutdown
// PrimaryLogPG::on_change, called by PG::start_peering_interval
void ReplicatedBackend::on_change()
{
  dout(10) << __func__ << dendl;

  for (map<ceph_tid_t, InProgressOp>::iterator i = in_progress_ops.begin();
       i != in_progress_ops.end();
       in_progress_ops.erase(i++)) {
    if (i->second.on_commit)
      delete i->second.on_commit;

    if (i->second.on_applied)
      delete i->second.on_applied;
  }

  clear_recovery_state();
}

// called by
// PrimaryLogPG::on_flushed
void ReplicatedBackend::on_flushed()
{
}

// called by
// PrimaryLogPG::do_osd_ops, for CEPH_OSD_OP_SYNC_READ, CEPH_OSD_OP_READ,
// CEPH_OSD_OP_MAPEXT
// PrimaryLogPG::fill_in_copy_get
int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return store->read(ch, ghobject_t(hoid), off, len, *bl, op_flags);
}

// created by
// ReplicatedBackend::objects_read_async
struct AsyncReadCallback : public GenContext<ThreadPool::TPHandle&> {
  int r;
  Context *c;
  AsyncReadCallback(int r, Context *c) : r(r), c(c) {}
  void finish(ThreadPool::TPHandle&) override {
    c->complete(r);
    c = NULL;
  }
  ~AsyncReadCallback() override {
    delete c;
  }
};

// called by
// PrimaryLogPG::OpContext::start_async_reads
void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  // There is no fast read implementation for replication backend yet
  assert(!fast_read);

  int r = 0;

  for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	   to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = store->read(ch, ghobject_t(hoid), i->first.get<0>(),
			 i->first.get<1>(), *(i->second.first),
			 i->first.get<2>());

    if (i->second.second) {
      get_parent()->schedule_recovery_work(
	get_parent()->bless_gencontext(
	  new AsyncReadCallback(_r, i->second.second)));
    }

    if (_r < 0)
      r = _r;
  }

  get_parent()->schedule_recovery_work(
    get_parent()->bless_gencontext(
      new AsyncReadCallback(r, on_complete)));
}

// created by
// ReplicatedBackend::submit_transaction
class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}

  void finish(int) override {
    pg->op_commit(op);
  }
};

// created by
// ReplicatedBackend::submit_transaction
// primary PG local txs onreadable callback
class C_OSD_OnOpApplied : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpApplied(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}

  void finish(int) override {
    pg->op_applied(op);
  }
};

// called by
// ReplicatedBackend::submit_transaction, which called by PrimaryLogPG::issue_repop, which
//      can be called by PrimaryLogPG::execute_ctx/PrimaryLogPG::simple_opc_submit
void generate_transaction(
  PGTransactionUPtr &pgt,
  const coll_t &coll,
  bool legacy_log_entries,
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction *t,
  set<hobject_t> *added,
  set<hobject_t> *removed)
{
  assert(t);
  assert(added);
  assert(removed);

  for (auto &&le: log_entries) {
    le.mark_unrollbackable();

    auto oiter = pgt->op_map.find(le.soid);

    // updated by PGTransaction::update_snaps, which called by PrimaryLogPG::trim_object,
    // which called by PrimaryLogPG::AwaitAsyncWork::react(const DoSnapWork)
    if (oiter != pgt->op_map.end() && oiter->second.updated_snaps) { // we are called by PrimaryLogPG::simple_opc_submit
      bufferlist bl(oiter->second.updated_snaps->second.size() * 8 + 8);
      ::encode(oiter->second.updated_snaps->second, bl);
      // log_entry_t::snaps was set by PrimaryLogPG::make_writeable/PrimaryLogPG::finish_ctx
      le.snaps.swap(bl);
      le.snaps.reassign_to_mempool(mempool::mempool_osd_pglog); // used by PG::update_snap_map <- PG::append_log
                                                                // <- PrimaryLogPG::log_operation <- ReplicatedBackend::submit_transaction
    }
  }

  // iterate through PGTransaction::op_map<hobject_t, PGTransaction::ObjectOperation>
  pgt->safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &obj_op) {
      const hobject_t &oid = obj_op.first;
      const ghobject_t goid =
	ghobject_t(oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
      const PGTransaction::ObjectOperation &op = obj_op.second;

      // temp object was created by
      // spg_t::make_temp_hobject
      // spg_t::make_temp_ghobject
      if (oid.is_temp()) { // pool <= POOL_TEMP_START && pool != INT64_MIN;
	if (op.is_fresh_object()) {
	  added->insert(oid);
	} else if (op.is_delete()) {
	  removed->insert(oid);
	}
      }

      if (op.delete_first) {
	t->remove(coll, goid);
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {
	},
	[&](const PGTransaction::ObjectOperation::Init::Create &op) {
	  t->touch(coll, goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Clone &op) {
	  t->clone(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &op) {
	  assert(op.source.is_temp());
	  t->collection_move_rename(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    coll,
	    goid);
	});

      if (op.truncate) {
	t->truncate(coll, goid, op.truncate->first);
	if (op.truncate->first != op.truncate->second)
	  t->truncate(coll, goid, op.truncate->second);
      }

      if (!op.attr_updates.empty()) {
	map<string, bufferlist> attrs;
	for (auto &&p: op.attr_updates) {
	  if (p.second)
	    attrs[p.first] = *(p.second);
	  else
	    t->rmattr(coll, goid, p.first);
	}
	t->setattrs(coll, goid, attrs);
      }

      if (op.clear_omap)
	t->omap_clear(coll, goid);
      if (op.omap_header)
	t->omap_setheader(coll, goid, *(op.omap_header));

      for (auto &&up: op.omap_updates) {
	using UpdateType = PGTransaction::ObjectOperation::OmapUpdateType;
	switch (up.first) {
	case UpdateType::Remove:
	  t->omap_rmkeys(coll, goid, up.second);
	  break;
	case UpdateType::Insert:
	  t->omap_setkeys(coll, goid, up.second);
	  break;
	}
      }

      // updated_snaps doesn't matter since we marked unrollbackable

      if (op.alloc_hint) {
	auto &hint = *(op.alloc_hint);
	t->set_alloc_hint(
	  coll,
	  goid,
	  hint.expected_object_size,
	  hint.expected_write_size,
	  hint.flags);
      }

      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	// https://github.com/exclipy/inline_variant_visitor
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    t->write(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len(),
	      op.buffer);
	  },
	  [&](const BufferUpdate::Zero &op) {
	    t->zero(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &op) {
	    assert(op.len == extent.get_len());
	    t->clone_range(
	      coll,
	      ghobject_t(op.from, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	      goid,
	      op.offset,
	      extent.get_len(),
	      extent.get_off());
	  });
      }
    }); // end of lambda
}

// called by
// PrimaryLogPG::issue_repop
void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,         // ctx->at_version
  PGTransactionUPtr &&_t,
  const eversion_t &trim_to,            // pg_trim_to
  const eversion_t &roll_forward_to,    // min_last_complete_ondisk, not used
  const vector<pg_log_entry_t> &_log_entries,
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync, // PrimaryLogPG.cc/C_OSD_OndiskWriteUnlock
  Context *on_all_acked, // PrimaryLogPG.cc/C_OSD_RepopApplied
  Context *on_all_commit, // PrimaryLogPG.cc/C_OSD_RepopCommit
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  parent->apply_stats(
    soid,
    delta_stats);

  vector<pg_log_entry_t> log_entries(_log_entries);
  ObjectStore::Transaction op_t;
  PGTransactionUPtr t(std::move(_t));
  set<hobject_t> added, removed;

  // from logged modification to ObjectStore tx
  generate_transaction(
    t,
    coll,
    (get_osdmap()->require_osd_release < CEPH_RELEASE_KRAKEN),
    log_entries,
    &op_t,
    &added,
    &removed);
  assert(added.size() <= 1);
  assert(removed.size() <= 1);

  assert(!in_progress_ops.count(tid));

  // on_all_commit -> on_commit
  // on_all_acked -> on_applied
  InProgressOp &op = in_progress_ops.insert(
    make_pair(
      tid,
      InProgressOp(
	tid,
	on_all_commit,  // PrimaryLogPG.cc/C_OSD_RepopCommit
	on_all_acked,   // PrimaryLogPG.cc/C_OSD_RepopApplied
	orig_op, at_version) // ctx->at_version
      )
    ).first->second;

  op.waiting_for_applied.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());
  op.waiting_for_commit.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());

  // send MOSDRepOp to PG::actingbackfill
  issue_op(
    soid,
    at_version, // ctx->at_version
    tid,
    reqid,
    trim_to,    // pg_trim_to
    at_version, // ctx->at_version
    added.size() ? *(added.begin()) : hobject_t(),
    removed.size() ? *(removed.begin()) : hobject_t(),
    log_entries,
    hset_history,
    &op,
    op_t);

  // temp objects are for PGBackend::on_change_cleanup
  add_temp_objs(added);
  clear_temp_objs(removed);

  // call PG::append_log to update pg log and record in op txn
  parent->log_operation(
    log_entries,
    hset_history,
    trim_to,    // pg_trim_to
    at_version, // ctx->at_version
    true,       // transaction_applied
    op_t);

  // ObjectStore::Transaction::on_applied_sync
  op_t.register_on_applied_sync(on_local_applied_sync);

  //push back of ObjectStore::Transaction::on_applied
  //callback ReplicatedBackend::op_applied will call on_all_acked if all replicas applied
  op_t.register_on_applied(
    parent->bless_context(
      new C_OSD_OnOpApplied(this, &op))); // pg->op_applied(op);

  // push back of ObjectStore::Transaction::on_commit
  // callback ReplicatedBackend::op_commit will call on_all_commit if all replicas committed
  op_t.register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op))); // pg->op_commit(op);

  vector<ObjectStore::Transaction> tls;
  tls.push_back(std::move(op_t));

  // calls osd->store->queue_transactions
  parent->queue_transactions(tls, op.op);
}

// called by
// C_OSD_OnOpApplied::finish
// primary PG txs have been applied
void ReplicatedBackend::op_applied(
  InProgressOp *op)
{
  FUNCTRACE();
  OID_EVENT_TRACE_WITH_MSG((op && op->op) ? op->op->get_req() : NULL, "OP_APPLIED_BEGIN", true);
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op) {
    op->op->mark_event("op_applied");
    op->op->pg_trace.event("op applied");
  }

  op->waiting_for_applied.erase(get_parent()->whoami_shard());

  // update PG::last_update_applied and queue/requeue scrub
  parent->op_applied(op->v);

  if (op->waiting_for_applied.empty()) {
    // submit_transaction(on_all_acked), i.e., all replicas applied
    op->on_applied->complete(0);

    op->on_applied = 0;
  }

  if (op->done()) { // waiting_for_commit.empty() && waiting_for_applied.empty();
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

// called by
// C_OSD_OnOpCommit::finish
// primary PG txs committed
void ReplicatedBackend::op_commit(
  InProgressOp *op)
{
  FUNCTRACE();
  OID_EVENT_TRACE_WITH_MSG((op && op->op) ? op->op->get_req() : NULL, "OP_COMMIT_BEGIN", true);
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op) {
    op->op->mark_event("op_commit");
    op->op->pg_trace.event("op commit");
  }

  op->waiting_for_commit.erase(get_parent()->whoami_shard());

  if (op->waiting_for_commit.empty()) {
    // submit_transaction(on_all_commit), i.e., all replicas journaled
    op->on_commit->complete(0);

    op->on_commit = 0;
  }

  if (op->done()) { // waiting_for_commit.empty() && waiting_for_applied.empty();
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

// called by
// ReplicatedBackend::handle_message, for MSG_OSD_REPOPREPLY
// message from replicas
void ReplicatedBackend::do_repop_reply(OpRequestRef op)
{
  // sent by
  // ReplicatedBackend::sub_op_modify_applied
  // ReplicatedBackend::sub_op_modify_commit
  static_cast<MOSDRepOpReply*>(op->get_nonconst_req())->finish_decode();
  const MOSDRepOpReply *r = static_cast<const MOSDRepOpReply *>(op->get_req());
  assert(r->get_header().type == MSG_OSD_REPOPREPLY);

  op->mark_started();

  // must be replication.
  ceph_tid_t rep_tid = r->get_tid();
  pg_shard_t from = r->from;

  if (in_progress_ops.count(rep_tid)) {
    map<ceph_tid_t, InProgressOp>::iterator iter =
      in_progress_ops.find(rep_tid);

    InProgressOp &ip_op = iter->second;
    const MOSDOp *m = NULL;
    if (ip_op.op)
      m = static_cast<const MOSDOp *>(ip_op.op->get_req());

    if (m)
      dout(7) << __func__ << ": tid " << ip_op.tid << " op " //<< *m
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;
    else
      dout(7) << __func__ << ": tid " << ip_op.tid << " (no op) "
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;

    // oh, good.

    if (r->ack_type & CEPH_OSD_FLAG_ONDISK) {

      // repop localt committed
        
      assert(ip_op.waiting_for_commit.count(from));
      
      ip_op.waiting_for_commit.erase(from);
      
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_commit_rec from " << from;
	ip_op.op->mark_event_string(ss.str());
	ip_op.op->pg_trace.event("sub_op_commit_rec");
      }
    } else {

      // repop op_t applied
    
      assert(ip_op.waiting_for_applied.count(from));
      
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_applied_rec from " << from;
	ip_op.op->mark_event_string(ss.str());
	ip_op.op->pg_trace.event("sub_op_applied_rec");
      }
    }

    // if the repop op_t has been applied, then the ONDISK reply will never be sent, 
    // see ReplicatedBackend::sub_op_modify_applied
    ip_op.waiting_for_applied.erase(from);

    parent->update_peer_last_complete_ondisk(
      from,
      r->get_last_complete_ondisk());

    if (ip_op.waiting_for_applied.empty() &&
        ip_op.on_applied) {
      // submit_transaction(on_all_acked), i.e., all replicas written and synced
      ip_op.on_applied->complete(0);
      ip_op.on_applied = 0;
    }
    if (ip_op.waiting_for_commit.empty() &&
        ip_op.on_commit) {
      // submit_transaction(on_all_commit), i.e., all replicas journaled
      ip_op.on_commit->complete(0);
      ip_op.on_commit= 0;
    }

    if (ip_op.done()) {
      assert(!ip_op.on_commit && !ip_op.on_applied);
      in_progress_ops.erase(iter);
    }
  }
}

// called by
// PGBackend::be_scan_list
void ReplicatedBackend::be_deep_scrub(
  const hobject_t &poid,
  uint32_t seed,
  ScrubMap::object &o,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " " << poid << " seed " 
	   << std::hex << seed << std::dec << dendl;
  bufferhash h(seed), oh(seed);
  bufferlist bl, hdrbl;
  int r;
  __u64 pos = 0;

  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  while (true) {
    handle.reset_tp_timeout();
    r = store->read(
	  ch,
	  ghobject_t(
	    poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	  pos,
	  cct->_conf->osd_deep_scrub_stride, bl,
	  fadvise_flags);
    if (r <= 0)
      break;

    h << bl;
    pos += bl.length();
    bl.clear();
  }
  if (r == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on read, read_error" << dendl;
    o.read_error = true;
    return;
  }
  o.digest = h.digest();
  o.digest_present = true;

  bl.clear();
  r = store->omap_get_header(
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    &hdrbl, true);
  // NOTE: bobtail to giant, we would crc the head as (len, head).
  // that changes at the same time we start using a non-zero seed.
  if (r == 0 && hdrbl.length()) {
    dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
    if (seed == 0) {
      // legacy
      bufferlist bl;
      ::encode(hdrbl, bl);
      oh << bl;
    } else {
      oh << hdrbl;
    }
  } else if (r == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on omap header read, read_error" << dendl;
    o.read_error = true;
    return;
  }

  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  assert(iter);
  for (iter->seek_to_first(); iter->status() == 0 && iter->valid();
    iter->next(false)) {
    handle.reset_tp_timeout();

    dout(25) << "CRC key " << iter->key() << " value:\n";
    iter->value().hexdump(*_dout);
    *_dout << dendl;

    ::encode(iter->key(), bl);
    ::encode(iter->value(), bl);
    oh << bl;
    bl.clear();
  }

  if (iter->status() < 0) {
    dout(25) << __func__ << "  " << poid
             << " on omap scan, db status error" << dendl;
    o.read_error = true;
    return;
  }

  //Store final calculated CRC32 of omap header & key/values
  o.omap_digest = oh.digest();
  o.omap_digest_present = true;
  dout(20) << __func__ << "  " << poid << " omap_digest "
	   << std::hex << o.omap_digest << std::dec << dendl;
}

// PushOp to from primary PG to replica PG
/*
  void do_push(OpRequestRef op) {
    if (is_primary()) {
      _do_pull_response(op);
    } else {
      _do_push(op);
    }
  }
 */
// called by
// ReplicatedBackend::handle_message, for MSG_OSD_PG_PUSH and we are replica
void ReplicatedBackend::_do_push(OpRequestRef op)
{
  // sent by ReplicatedBackend::send_pushes
  const MOSDPGPush *m = static_cast<const MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  vector<PushReplyOp> replies;
  ObjectStore::Transaction t;
  ostringstream ss;
  if (get_parent()->check_failsafe_full(ss)) {
    dout(10) << __func__ << " Out of space (failsafe) processing push request: " << ss.str() << dendl;
    ceph_abort();
  }
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());

    handle_push(from, *i, &(replies.back()), &t);
  }

  // will be handled by ReplicatedBackend::do_push_reply
  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->min_epoch = m->min_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t.register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  get_parent()->queue_transaction(std::move(t));
}

// created by
// ReplicatedBackend::_do_pull_response
// ReplicatedBackend::sub_op_push
struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<ReplicatedBackend::pull_complete_info> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(ReplicatedBackend *bc, int priority)
    : bc(bc), priority(priority) {}

  void finish(ThreadPool::TPHandle &handle) override {
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (auto &&i: to_continue) {
      auto j = bc->pulling.find(i.hoid);
      assert(j != bc->pulling.end());
      ObjectContextRef obc = j->second.obc;
      bc->clear_pull(j, false /* already did it */);
      int started = bc->start_pushes(i.hoid, obc, h);
      if (started < 0) {
	bc->pushing[i.hoid].clear();
	bc->get_parent()->primary_failed(i.hoid);
	bc->get_parent()->primary_error(i.hoid, obc->obs.oi.version);
      } else if (!started) {
	bc->get_parent()->on_global_recover(
	  i.hoid, i.stat);
      }
      handle.reset_tp_timeout();
    }

    bc->run_recovery_op(h, priority);
  }
};

// PushOp from replica PG to primary PG, i.e., a response of a previous
// pull request from primary PG
/*
  void do_push(OpRequestRef op) {
    if (is_primary()) {
      _do_pull_response(op);
    } else {
      _do_push(op);
    }
  }
 */
// called by
// ReplicatedBackend::do_push, if we are the primary PG
void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  // sent by ReplicatedBackend::send_pushes
  const MOSDPGPush *m = static_cast<const MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  // the op is replica push to primary

  op->mark_started();

  vector<PullOp> replies(1);

  ostringstream ss;
  if (get_parent()->check_failsafe_full(ss)) {
    dout(10) << __func__ << " Out of space (failsafe) processing pull response (push): " << ss.str() << dendl;
    ceph_abort();
  }

  ObjectStore::Transaction t;
  list<pull_complete_info> to_continue;
  for (vector<PushOp>::const_iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, &t);

    if (more)
      // the current object has still need to pull something, i.e.,
      // omap entries, object data, etc. from the replica PG
      replies.push_back(PullOp());
  }

  if (!to_continue.empty()) {

    // the objects that have finished pulling, we can try to push to
    // replica other replica PGs that missing the object

    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority());
    c->to_continue.swap(to_continue);

    t.register_on_complete(
      new PG_RecoveryQueueAsync(
	get_parent(),
	get_parent()->bless_gencontext(c)));
  }

  // pop the last item which is empty
  replies.erase(replies.end() - 1);

  if (replies.size()) {

    // those objects that their pulling has not finished

    // will be handled by ReplicatedBackend::do_pull
    MOSDPGPull *reply = new MOSDPGPull;

    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->min_epoch = m->min_epoch;
    reply->set_pulls(&replies);
    reply->compute_cost(cct);

    // send to the replica PG to continue the pulling on received data
    // has been written to ObjectStore
    t.register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  get_parent()->queue_transaction(std::move(t));
}

// called by
// ReplicatedBackend::handle_message, for MSG_OSD_PG_PULL
void ReplicatedBackend::do_pull(OpRequestRef op)
{
  // sent by
  // ReplicatedBackend::_do_pull_response
  // ReplicatedBackend::send_pulls
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_nonconst_req());
  assert(m->get_type() == MSG_OSD_PG_PULL);
  pg_shard_t from = m->from;

  map<pg_shard_t, vector<PushOp> > replies;
  vector<PullOp> pulls;
  m->take_pulls(&pulls);
  for (auto& i : pulls) {
    replies[from].push_back(PushOp());

    // call build_push_op to build a PushOp for push
    handle_pull(from, i, &(replies[from].back()));
  }

  // send MOSDPGPush
  send_pushes(m->get_priority(), replies);
}

// called by
// ReplicatedBackend::handle_message, for MSG_OSD_PG_PUSH_REPLY
void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  // sent by ReplicatedBackend::_do_push
  const MOSDPGPushReply *m = static_cast<const MOSDPGPushReply *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH_REPLY);
  pg_shard_t from = m->from;

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::const_iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more)
      replies.push_back(PushOp());
  }
  replies.erase(replies.end() - 1);

  map<pg_shard_t, vector<PushOp> > _replies;
  _replies[from].swap(replies);

  // send MOSDPGPush
  send_pushes(m->get_priority(), _replies);
}

// called by
// ReplicatedBackend::issue_op, which called by ReplicatedBackend::submit_transaction
Message * ReplicatedBackend::generate_subop(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_roll_forward_to, // ctx->at_version
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  ObjectStore::Transaction &op_t,
  pg_shard_t peer,
  const pg_info_t &pinfo)
{
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

  // forward the write/update/whatever
  MOSDRepOp *wr = new MOSDRepOp(
    reqid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard),
    soid, acks_wanted,
    get_osdmap()->get_epoch(),
    parent->get_last_peering_reset_epoch(),
    tid, at_version);

  // ship resulting transaction, log entries, and pg_stats
  if (!parent->should_send_op(peer, soid)) { // send op_t? if the peer is a backfill target, do not send op_t
    dout(10) << "issue_repop shipping empty opt to osd." << peer
	     <<", object " << soid
	     << " beyond MAX(last_backfill_started "
	     << ", pinfo.last_backfill "
	     << pinfo.last_backfill << ")" << dendl;

    ObjectStore::Transaction t;
    ::encode(t, wr->get_data()); // empty tx
  } else {
    ::encode(op_t, wr->get_data());

    wr->get_header().data_off = op_t.get_data_alignment();
  }

  ::encode(log_entries, wr->logbl);

  if (pinfo.is_incomplete())
    wr->pg_stats = pinfo.stats;  // reflects backfill progress
  else
    wr->pg_stats = get_info().stats;

  wr->pg_trim_to = pg_trim_to;
  wr->pg_roll_forward_to = pg_roll_forward_to;

  // see generate_transaction
  wr->new_temp_oid = new_temp_oid;
  wr->discard_temp_oid = discard_temp_oid;

  wr->updated_hit_set_history = hset_hist;
  return wr;
}

// called by
// ReplicatedBackend::submit_transaction
void ReplicatedBackend::issue_op(
  const hobject_t &soid,
  const eversion_t &at_version, // ctx->at_version
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_roll_forward_to, // ctx->at_version
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction &op_t)
{
  if (op->op)
    op->op->pg_trace.event("issue replication ops");

  if (parent->get_actingbackfill_shards().size() > 1) {
    ostringstream ss;
    set<pg_shard_t> replicas = parent->get_actingbackfill_shards();
    replicas.erase(parent->whoami_shard());
    ss << "waiting for subops from " << replicas;
    if (op->op)
      op->op->mark_sub_op_sent(ss.str());
  }

  for (set<pg_shard_t>::const_iterator i =
	 parent->get_actingbackfill_shards().begin();
       i != parent->get_actingbackfill_shards().end();
       ++i) { // iterate PG::actingbackfill
    if (*i == parent->whoami_shard()) continue;

    pg_shard_t peer = *i;
    const pg_info_t &pinfo = parent->get_shard_info().find(peer)->second;

    Message *wr;
    // construct MOSDRepOp
    wr = generate_subop(
      soid,
      at_version,
      tid,
      reqid,
      pg_trim_to,
      pg_roll_forward_to,       // ctx->at_version
      new_temp_oid,             // see generate_transaction
      discard_temp_oid,         // see generate_transaction
      log_entries,
      hset_hist,
      op_t,
      peer,
      pinfo);

    if (op->op)
      wr->trace.init("replicated op", nullptr, &op->op->pg_trace);

    get_parent()->send_message_osd_cluster(
      peer.osd, wr, get_osdmap()->get_epoch());
  }
}

// called by
// ReplicatedBackend::handle_message, for MSG_OSD_REPOP
void ReplicatedBackend::do_repop(OpRequestRef op)
{
  static_cast<MOSDRepOp*>(op->get_nonconst_req())->finish_decode();
  const MOSDRepOp *m = static_cast<const MOSDRepOp *>(op->get_req());
  int msg_type = m->get_type();
  assert(MSG_OSD_REPOP == msg_type);

  const hobject_t& soid = m->poid;

  dout(10) << __func__ << " " << soid
           << " v " << m->version
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;

  // sanity checks
  assert(m->map_epoch >= get_info().history.same_interval_since);

  // we better not be missing this.
  assert(!parent->get_log().get_missing().is_missing(soid));

  int ackerosd = m->get_source().num();

  op->mark_started();

  RepModifyRef rm(std::make_shared<RepModify>());
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  assert(m->logbl.length());
  // shipped transaction and log entries
  vector<pg_log_entry_t> log;

  bufferlist::iterator p = const_cast<bufferlist&>(m->get_data()).begin();
  ::decode(rm->opt, p);

  if (m->new_temp_oid != hobject_t()) {
    dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;

    add_temp_obj(m->new_temp_oid);
  }

  if (m->discard_temp_oid != hobject_t()) {
    dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;

    if (rm->opt.empty()) {
      dout(10) << __func__ << ": removing object " << m->discard_temp_oid
	       << " since we won't get the transaction" << dendl;

      rm->localt.remove(coll, ghobject_t(m->discard_temp_oid));
    }

    clear_temp_obj(m->discard_temp_oid);
  }

  p = const_cast<bufferlist&>(m->logbl).begin();
  ::decode(log, p);
  
  rm->opt.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  bool update_snaps = false;
  if (!rm->opt.empty()) { // we may sent an empty tx, see PrimaryLogPG::should_send_op
    // If the opt is non-empty, we infer we are before
    // last_backfill (according to the primary, not our
    // not-quite-accurate value), and should update the
    // collections now.  Otherwise, we do it later on push.
    update_snaps = true;
  }
  
  parent->update_stats(m->pg_stats);
  
  // call PrimaryLogPG::log_operation, which calls PG::append_log
  parent->log_operation(
    log,
    m->updated_hit_set_history,
    m->pg_trim_to,
    m->pg_roll_forward_to,
    update_snaps,
    rm->localt);

  // ReplicatedBackend::sub_op_modify_commit
  rm->opt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));
  
  // ReplicatedBackend::sub_op_modify_applied
  rm->localt.register_on_applied(
    parent->bless_context(
      new C_OSD_RepModifyApply(this, rm)));
  
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(rm->localt));
  tls.push_back(std::move(rm->opt));
  
  parent->queue_transactions(tls, op);
  // op is cleaned up by oncommit/onapply when both are executed
}

// called by
// C_OSD_RepModifyApply::finish which means repop localt tx applied, i.e., onreadable
void ReplicatedBackend::repop_applied(RepModifyRef rm)
{
  rm->op->mark_event("sub_op_applied");
  
  rm->applied = true;
  rm->op->pg_trace.event("sup_op_applied");

  dout(10) << __func__ << " on " << rm << " op "
	   << *rm->op->get_req() << dendl;
  const Message *m = rm->op->get_req();
  const MOSDRepOp *req = static_cast<const MOSDRepOp*>(m);
  eversion_t version = req->version;

  // send ack to acker only if we haven't sent a commit already
  if (!rm->committed) {
    Message *ack = new MOSDRepOpReply(
      req, parent->whoami_shard(),
      0, get_osdmap()->get_epoch(), req->min_epoch, CEPH_OSD_FLAG_ACK);
    ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
    ack->trace = rm->op->pg_trace;
    get_parent()->send_message_osd_cluster(
      rm->ackerosd, ack, get_osdmap()->get_epoch());
  }

  parent->op_applied(version);
}

// called by
// C_OSD_RepModifyCommit::finish, which means repop op_t tx committed, i.e., ondisk
void ReplicatedBackend::repop_commit(RepModifyRef rm)
{
  rm->op->mark_commit_sent();
  rm->op->pg_trace.event("sup_op_commit");
  rm->committed = true;

  // send commit.
  const MOSDRepOp *m = static_cast<const MOSDRepOp*>(rm->op->get_req());
  assert(m->get_type() == MSG_OSD_REPOP);
  dout(10) << __func__ << " on op " << *m
	   << ", sending commit to osd." << rm->ackerosd
	   << dendl;
  assert(get_osdmap()->is_up(rm->ackerosd));

  get_parent()->update_last_complete_ondisk(rm->last_complete);

  MOSDRepOpReply *reply = new MOSDRepOpReply(
    m,
    get_parent()->whoami_shard(),
    0, get_osdmap()->get_epoch(), m->get_min_epoch(), CEPH_OSD_FLAG_ONDISK);
  reply->set_last_complete_ondisk(rm->last_complete);
  reply->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  reply->trace = rm->op->pg_trace;
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, reply, get_osdmap()->get_epoch());

  log_subop_stats(get_parent()->get_logger(), rm->op, l_osd_sop_w);
}


// ===========================================================

// called by
// ReplicatedBackend::prep_push_to_replica
void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing, // PG::peer_missing[peer]
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) { // cache pool
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  // default true
  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_head_subsets " << head << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }


  interval_set<uint64_t> cloning;

  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);

  for (int j=snapset.clones.size()-1; j>=0; j--) { // search down to lower snaps
    hobject_t c = head;
    c.snap = snapset.clones[j];

    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);

    if (!missing.is_missing(c) && // PG::peer_missing[peer]
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;

      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }

    // try older snap, NOTE: prev has been updated to cover only the overlapped area

    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // for head, no newer snaps to search

  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) { // default 10
    dout(10) << "skipping clone, too many holes" << dendl;

    get_parent()->release_locks(manager);
    clone_subsets.clear();
    cloning.clear();
  }

  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

// called by
// ReplicatedBackend::prepare_pull
// ReplicatedBackend::prep_push_to_replica
// ReplicatedBackend::recalc_subsets
void ReplicatedBackend::calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing, // for pull PG::pg_log.missing, for push PG::peer_missing[peer]
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  ObcLockManager &manager)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) { // cache pool
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  // default true
  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_clone_subsets " << soid << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++) // ascending order, locate the snapid
    if (snapset.clones[i] == soid.snap)
      break;

  interval_set<uint64_t> cloning;

  // any overlap with next older clone?
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);

  for (int j=i-1; j>=0; j--) { // search down to the older snaps
    hobject_t c = soid;
    c.snap = snapset.clones[j];

    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);

    if (!missing.is_missing(c) &&
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;

      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }

    // try older snap, NOTE: prev has been updated to cover only the overlapped area

    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // overlap with next newest?
  interval_set<uint64_t> next;

  if (size)
    next.insert(0, size);

  for (unsigned j=i+1; j<snapset.clones.size(); j++) { // search up to the newer snaps
    hobject_t c = soid;
    c.snap = snapset.clones[j];

    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);

    if (!missing.is_missing(c) &&
	c < last_backfill &&
	get_parent()->try_lock_for_read(c, manager)) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;

      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }

    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  // default 10
  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;

    get_parent()->release_locks(manager);
    clone_subsets.clear();
    cloning.clear();
  }


  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

// called by
// ReplicatedBackend::recover_object
void ReplicatedBackend::prepare_pull(
  eversion_t v, // from PG::MissingLoc::needs_recovery_map
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  // PG::pg_log.missing, i.e., pg_missing_tracker_t
  assert(get_parent()->get_local_missing().get_items().count(soid));

  // from PG::pg_log.missing.missing
  eversion_t _v = get_parent()->get_local_missing().get_items().find(
    soid)->second.need;
  assert(_v == v);

  const map<hobject_t, set<pg_shard_t>> &missing_loc(
    get_parent()->get_missing_loc_shards()); // PG::MissingLoc::missing_loc
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing()); // PG::peer_missing

  map<hobject_t, set<pg_shard_t>>::const_iterator q = missing_loc.find(soid);
  assert(q != missing_loc.end());
  assert(!q->second.empty());

  // pick a pullee
  vector<pg_shard_t> shuffle(q->second.begin(), q->second.end());
  random_shuffle(shuffle.begin(), shuffle.end());
  vector<pg_shard_t>::iterator p = shuffle.begin();
  assert(get_osdmap()->is_up(p->osd));

  // pull from this random shard
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << q->second
	  << " from osd." << fromshard
	  << dendl;

  assert(peer_missing.count(fromshard));

  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) { // peer also missing this object and its need <= v
    assert(pmissing.get_items().find(soid)->second.have != v);

    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.get_items().find(soid)->second.have
	     << " rather than at version " << v << dendl;

    v = pmissing.get_items().find(soid)->second.have;

    assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }

  ObjectRecoveryInfo recovery_info;
  ObcLockManager lock_manager;

  if (soid.is_snap()) { // pulling a snap object

    // to recover a snap object on primary, we must have the head obc, most
    // important the ssc, so we know how to use the local snap object to
    // reduce remote copy, i.e., to calc clone_subset and copy_subset
    assert(!get_parent()->get_local_missing().is_missing(
	     soid.get_head()) ||
	   !get_parent()->get_local_missing().is_missing(
	     soid.get_snapdir()));

    assert(headctx); // head exists, we are to recover a snap object

    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    assert(ssc);

    dout(10) << " snapset " << ssc->snapset << dendl;

    recovery_info.ss = ssc->snapset;

    calc_clone_subsets(
      ssc->snapset, soid, get_parent()->get_local_missing(), // PG::pg_log.missing
      get_info().last_backfill,
      recovery_info.copy_subset,
      recovery_info.clone_subset,
      lock_manager);

    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;

    assert(ssc->snapset.clone_size.count(soid.snap));

    recovery_info.size = ssc->snapset.clone_size[soid.snap];
  } else {
    // to recover a head object on primary, do full copy

    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    recovery_info.size = ((uint64_t)-1);
  }

  // will be handled by ReplicatedBackend::sub_op_pull
  h->pulls[fromshard].push_back(PullOp());

  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = false;
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);

  // register a pulling request, for response handle, i.e., ReplicatedBackend::handle_pull_response
  // and clearing work
  PullInfo &pi = pulling[soid];

  pi.from = fromshard;
  pi.soid = soid;
  pi.head_ctx = headctx;
  pi.recovery_info = op.recovery_info;
  pi.recovery_progress = op.recovery_progress;
  pi.cache_dont_need = h->cache_dont_need;
  pi.lock_manager = std::move(lock_manager);
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
// called by
// ReplicatedBackend::start_pushes, which called by ReplicatedBackend::recover_object
// and C_ReplicatedBackend_OnPullComplete::finish
int ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop, bool cache_dont_need)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t>> clone_subsets;
  interval_set<uint64_t> data_subset;

  ObcLockManager lock_manager;

  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) { // replica missing a snap object

    // --- SNAP -----------------------------------------------------------------

    // PrimaryLogPG::recover_replicas only allow the snap object to recover
    // if the primary head/snapdir object is not missing, so the tests here
    // should not be needed

    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;

      // no ssc available, use empty clone_subset and full data_subset
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }

    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (get_parent()->get_local_missing().is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir
	       << ", pushing raw clone" << dendl;

      // no ssc available, use empty clone_subset and full data_subset
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }

    // we have head/snapdir for this snap object the replica is missing,
    // i.e., we know the SnapSet of the object, so we can calc if we
    // could try to construct the snap object's data from other snap objects
    // and head so reduce the data transfer

    SnapSetContext *ssc = obc->ssc;
    assert(ssc);

    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;

    pop->recovery_info.ss = ssc->snapset;

    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer); // PG::peer_missing[peer]
    assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer); // PG::peer_info[peer]
    assert(pi != get_parent()->get_shard_info().end());

    calc_clone_subsets(
      ssc->snapset, soid,
      pm->second,
      pi->second.last_backfill,
      data_subset, clone_subsets,
      lock_manager);
  } else if (soid.snap == CEPH_NOSNAP) { // replica missing head/snapdir

    // --- HEAD -----------------------------------------------------------------

    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = obc->ssc;
    assert(ssc);

    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;

    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second, // PG::peer_missing[peer]
      get_parent()->get_shard_info().find(peer)->second.last_backfill, // PG::peer_info[peer]
      data_subset, clone_subsets,
      lock_manager);
  }

  // the push op will be handled by ReplicatedBackend::sub_op_push
  return prep_push(
    obc,
    soid,
    peer,
    oi.version,
    data_subset,
    clone_subsets,
    pop,
    cache_dont_need,
    std::move(lock_manager));
}

// called by
// ReplicatedBackend::prep_push_to_replica
int ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop, bool cache_dont_need)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);

  map<hobject_t, interval_set<uint64_t>> clone_subsets;

  return prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop, cache_dont_need, ObcLockManager());
}

// called by
// ReplicatedBackend::prep_push_to_replica
// ReplicatedBackend::prep_push, i.e., the method above, with empty clone_subsets
int ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version, // obc->obs.oi.version
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t>>& clone_subsets,
  PushOp *pop,
  bool cache_dont_need,
  ObcLockManager &&lock_manager)
{
  get_parent()->begin_peer_recover(peer, soid);

  // register a pushing request on ReplicatedBackend::pushing
  // take note.
  PushInfo &pi = pushing[soid][peer];

  pi.obc = obc;
  pi.recovery_info.size = obc->obs.oi.size;
  pi.recovery_info.copy_subset = data_subset;
  pi.recovery_info.clone_subset = clone_subsets;
  pi.recovery_info.soid = soid;
  pi.recovery_info.oi = obc->obs.oi;
  pi.recovery_info.ss = pop->recovery_info.ss;
  pi.recovery_info.version = version;
  pi.lock_manager = std::move(lock_manager);

  ObjectRecoveryProgress new_progress;

  int r = build_push_op(pi.recovery_info,
			pi.recovery_progress,
			&new_progress,
			pop,
			&(pi.stat), cache_dont_need);
  if (r < 0)
    return r;
  pi.recovery_progress = new_progress;
  return 0;
}

// called by
// ReplicatedBackend::handle_pull_response
// ReplicatedBackend::handle_push
void ReplicatedBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool cache_dont_need,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  const map<string, bufferlist> &attrs,
  const map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_parent()->get_temp_recovery_object(recovery_info.soid,
							recovery_info.version);
    if (first) {
      dout(10) << __func__ << ": Adding oid "
	       << target_oid << " in the temp collection" << dendl;
      add_temp_obj(target_oid);
    }
  }

  if (first) {
    t->remove(coll, ghobject_t(target_oid));
    t->touch(coll, ghobject_t(target_oid));
    t->truncate(coll, ghobject_t(target_oid), recovery_info.size);
    if (omap_header.length()) 
      t->omap_setheader(coll, ghobject_t(target_oid), omap_header);

    bufferlist bv = attrs.at(OI_ATTR);
    object_info_t oi(bv);
    t->set_alloc_hint(coll, ghobject_t(target_oid),
		      oi.expected_object_size,
		      oi.expected_write_size,
		      oi.alloc_hint_flags);
  }

  uint64_t off = 0;
  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (cache_dont_need)
    fadvise_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(coll, ghobject_t(target_oid),
	     p.get_start(), p.get_len(), bit, fadvise_flags);
    off += p.get_len();
  }

  if (!omap_entries.empty())
    t->omap_setkeys(coll, ghobject_t(target_oid), omap_entries);
  if (!attrs.empty())
    t->setattrs(coll, ghobject_t(target_oid), attrs);

  if (complete) {
    if (!first) {
      dout(10) << __func__ << ": Removing oid "
	       << target_oid << " from the temp collection" << dendl;
      clear_temp_obj(target_oid);
      t->remove(coll, ghobject_t(recovery_info.soid));
      t->collection_move_rename(coll, ghobject_t(target_oid),
				coll, ghobject_t(recovery_info.soid));
    }

    submit_push_complete(recovery_info, t);
  }
}

// called by
// ReplicatedBackend::submit_push_data
void ReplicatedBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t>>::const_iterator p =
	 recovery_info.clone_subset.begin();
       p != recovery_info.clone_subset.end();
       ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(15) << " clone_range " << p->first << " "
	       << q.get_start() << "~" << q.get_len() << dendl;
      t->clone_range(coll, ghobject_t(p->first), ghobject_t(recovery_info.soid),
		     q.get_start(), q.get_len(), q.get_start());
    }
  }
}

// called by
// ReplicatedBackend::handle_pull_response, which called by ReplicatedBackend::_do_pull_response
ObjectRecoveryInfo ReplicatedBackend::recalc_subsets(
  const ObjectRecoveryInfo& recovery_info,
  SnapSetContext *ssc,
  ObcLockManager &manager)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  assert(ssc);
  get_parent()->release_locks(manager); // might already have locks
  calc_clone_subsets(
    ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
    get_info().last_backfill,
    new_info.copy_subset, new_info.clone_subset,
    manager);
  return new_info;
}

// called by
// ReplicatedBackend::_do_pull_response
bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, const PushOp &pop, PullOp *response,
  list<pull_complete_info> *to_continue,
  ObjectStore::Transaction *t)
{
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data = pop.data;
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;

  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    _failed_pull(from, pop.soid);
    return false;
  }

  const hobject_t &hoid = pop.soid;
  assert((data_included.empty() && data.length() == 0) ||
	 (!data_included.empty() && data.length() > 0));

  // ReplicatedBackend::pulling is inserted by ReplicatedBackend::prepare_pull
  auto piter = pulling.find(hoid);
  if (piter == pulling.end()) {
    return false;
  }

  PullInfo &pi = piter->second;
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }
  // If primary doesn't have object info and didn't know version
  if (pi.recovery_info.version == eversion_t()) {
    pi.recovery_info.version = pop.version;
  }

  bool first = pi.recovery_progress.first;
  if (first) {
    // attrs only reference the origin bufferlist (decode from
    // MOSDPGPush message) whose size is much greater than attrs in
    // recovery. If obc cache it (get_obc maybe cache the attr), this
    // causes the whole origin bufferlist would not be free until obc
    // is evicted from obc cache. So rebuild the bufferlists before
    // cache it.
    auto attrset = pop.attrset;
    for (auto& a : attrset) {
      a.second.rebuild();
    }
    pi.obc = get_parent()->get_obc(pi.recovery_info.soid, attrset);
    pi.recovery_info.oi = pi.obc->obs.oi;
    pi.recovery_info = recalc_subsets(
      pi.recovery_info,
      pi.obc->ssc,
      pi.lock_manager);
  }


  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data.claim(usable_data);


  pi.recovery_progress = pop.after_progress;

  dout(10) << "new recovery_info " << pi.recovery_info
	   << ", new progress " << pi.recovery_progress
	   << dendl;

  bool complete = pi.is_complete();

  // prepare txn for this object
  submit_push_data(pi.recovery_info, first,
		   complete, pi.cache_dont_need,
		   data_included, data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  pi.stat.num_keys_recovered += pop.omap_entries.size();
  pi.stat.num_bytes_recovered += data.length();

  if (complete) {

    // pulled all things related to this object

    pi.stat.num_objects_recovered++;
    clear_pull_from(piter);
    to_continue->push_back({hoid, pi.stat});
    get_parent()->on_local_recover(
      hoid, pi.recovery_info, pi.obc, t);
    return false;
  } else {

    // still need to pull from the replica PG

    response->soid = pop.soid;
    response->recovery_info = pi.recovery_info;
    response->recovery_progress = pi.recovery_progress;
    return true;
  }
}

// called by
// ReplicatedBackend::_do_push
// ReplicatedBackend::sub_op_push
void ReplicatedBackend::handle_push(
  pg_shard_t from, const PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t)
{
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;

  bufferlist data;
  data = pop.data;
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;

  response->soid = pop.recovery_info.soid;

  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   true, // must be replicate
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete)
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      t);
}

// called by
// ReplicatedBackend::run_recovery_op
// ReplicatedBackend::do_pull
// ReplicatedBackend::do_push_reply
void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());

    if (!con)
      continue;

    vector<PushOp>::iterator j = i->second.begin();
    while (j != i->second.end()) {
      uint64_t cost = 0;
      uint64_t pushes = 0;

      // will be handled by
      // ReplicatedBackend::_do_push
      // ReplicatedBackend::_do_pull_response
      MOSDPGPush *msg = new MOSDPGPush();
      msg->from = get_parent()->whoami_shard();
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap()->get_epoch();
      msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
      msg->set_priority(prio);
      for (;
           (j != i->second.end() &&
	    cost < cct->_conf->osd_max_push_cost &&
	    pushes < cct->_conf->osd_max_push_objects) ;
	   ++j) {
	dout(20) << __func__ << ": sending push " << *j
		 << " to osd." << i->first << dendl;

	cost += j->cost(cct);
	pushes += 1;
	msg->pushes.push_back(*j);
      }

      msg->set_cost(cost);

      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

// called by
// ReplicatedBackend::run_recovery_op
void ReplicatedBackend::send_pulls(int prio, map<pg_shard_t, vector<PullOp> > &pulls)
{
  for (map<pg_shard_t, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;

    dout(20) << __func__ << ": sending pulls " << i->second
	     << " to osd." << i->first << dendl;

    // will be handled by ReplicatedBackend::do_pull
    MOSDPGPull *msg = new MOSDPGPull();
    msg->from = parent->whoami_shard();
    msg->set_priority(prio);
    msg->pgid = get_parent()->primary_spg_t();
    msg->map_epoch = get_osdmap()->get_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->set_pulls(&i->second);
    msg->compute_cost(cct);

    get_parent()->send_message_osd_cluster(msg, con);
  }
}

// called by
// ReplicatedBackend::prep_push
// ReplicatedBackend::handle_push_reply
// ReplicatedBackend::handle_pull
int ReplicatedBackend::build_push_op(const ObjectRecoveryInfo &recovery_info,
				     const ObjectRecoveryProgress &progress,
				     ObjectRecoveryProgress *out_progress,
				     PushOp *out_op,
				     object_stat_sum_t *stat,
                                     bool cache_dont_need)
{
  ObjectRecoveryProgress _new_progress;
  if (!out_progress)
    out_progress = &_new_progress;

  ObjectRecoveryProgress &new_progress = *out_progress;
  // assignment
  new_progress = progress;

  dout(7) << __func__ << " " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  eversion_t v  = recovery_info.version;
  if (progress.first) {

    // copy omap_header and attr set

    int r = store->omap_get_header(coll, ghobject_t(recovery_info.soid), &out_op->omap_header);
    if(r < 0) {
      dout(1) << __func__ << " get omap header failed: " << cpp_strerror(-r) << dendl; 
      return r;
    }

    r = store->getattrs(ch, ghobject_t(recovery_info.soid), out_op->attrset);
    if(r < 0) {
      dout(1) << __func__ << " getattrs failed: " << cpp_strerror(-r) << dendl;
      return r;
    }

    // Debug
    bufferlist bv = out_op->attrset[OI_ATTR];
    object_info_t oi;
    try {
     bufferlist::iterator bliter = bv.begin();
     ::decode(oi, bliter);
    } catch (...) {
      dout(0) << __func__ << ": bad object_info_t: " << recovery_info.soid << dendl;
      return -EINVAL;
    }

    // If requestor didn't know the version, use ours
    if (v == eversion_t()) {
      v = oi.version;
    } else if (oi.version != v) {
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version;
      return -EINVAL;
    }

    new_progress.first = false;
  }
  // Once we provide the version subsequent requests will have it, so
  // at this point it must be known.
  assert(v != eversion_t());

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {

    // copy omap entries

    ObjectMap::ObjectMapIterator iter =
      store->get_omap_iterator(coll,
			       ghobject_t(recovery_info.soid));
    assert(iter);

    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next(false)) {
      if (!out_op->omap_entries.empty() &&
	  ((cct->_conf->osd_recovery_max_omap_entries_per_chunk > 0 &&
	    out_op->omap_entries.size() >= cct->_conf->osd_recovery_max_omap_entries_per_chunk) ||
	   available <= iter->key().size() + iter->value().length()))
	break;

      out_op->omap_entries.insert(make_pair(iter->key(), iter->value()));

      if ((iter->key().size() + iter->value().length()) <= available)
	available -= (iter->key().size() + iter->value().length());
      else
	available = 0;
    }

    if (!iter->valid())
      new_progress.omap_complete = true;
    else
      new_progress.omap_recovered_to = iter->key();
  }

  if (available > 0) {

    // object data ranges to read

    if (!recovery_info.copy_subset.empty()) {
      interval_set<uint64_t> copy_subset = recovery_info.copy_subset;
      map<uint64_t, uint64_t> m;
      int r = store->fiemap(ch, ghobject_t(recovery_info.soid), 0,
                            copy_subset.range_end(), m);
      if (r >= 0)  {
        interval_set<uint64_t> fiemap_included(m);
        copy_subset.intersection_of(fiemap_included);
      } else {
        // intersection of copy_subset and empty interval_set would be empty anyway
        copy_subset.clear();
      }

      out_op->data_included.span_of(copy_subset, progress.data_recovered_to,
                                    available);

      if (out_op->data_included.empty()) // zero filled section, skip to end!
        new_progress.data_recovered_to = recovery_info.copy_subset.range_end();
      else
        new_progress.data_recovered_to = out_op->data_included.range_end();
    }
  } else {
    out_op->data_included.clear();
  }

  for (interval_set<uint64_t>::iterator p = out_op->data_included.begin();
       p != out_op->data_included.end();
       ++p) {

    // copy object data

    bufferlist bit;
    int r = store->read(ch, ghobject_t(recovery_info.soid),
		p.get_start(), p.get_len(), bit,
                cache_dont_need ? CEPH_OSD_OP_FLAG_FADVISE_DONTNEED: 0);
    if (cct->_conf->osd_debug_random_push_read_error &&
        (rand() % (int)(cct->_conf->osd_debug_random_push_read_error * 100.0)) == 0) {
      dout(0) << __func__ << ": inject EIO " << recovery_info.soid << dendl;
      r = -EIO;
    }
    if (r < 0) {
      return r;
    }
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length()
	       << dendl;

      interval_set<uint64_t>::iterator save = p++;
      if (bit.length() == 0)
        out_op->data_included.erase(save);     //Remove this empty interval
      else
        save.set_len(bit.length());

      // Remove any other intervals present
      while (p != out_op->data_included.end()) {
        interval_set<uint64_t>::iterator save = p++;
        out_op->data_included.erase(save);
      }

      new_progress.data_complete = true;
      out_op->data.claim_append(bit);
      break;
    }

    out_op->data.claim_append(bit);
  }

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;

    if (stat)
      stat->num_objects_recovered++;
  }

  if (stat) {
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());

  // send
  out_op->version = v;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;

  return 0;
}

// called by
// ReplicatedBackend::handle_pull
void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

// called by
// ReplicatedBackend::do_push_reply
bool ReplicatedBackend::handle_push_reply(
  pg_shard_t peer, const PushReplyOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;

  // ReplicatedBackend::pushing is inserted by ReplicatedBackend::prep_push
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;

    return false;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;

    return false;
  } else {

    // we sent a PushOp to the peer previously, check if we need to send
    // another PushOp of this object to the peer to continue the recovery

    PushInfo *pi = &pushing[soid][peer];
    bool error = pushing[soid].begin()->second.recovery_progress.error;

    if (!pi->recovery_progress.data_complete && !error) {
      // recover this object to the specified peer has not finished yet
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;

      // build the next PushOp to continue the recovery of the object to
      // the specified peer
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	pi->recovery_info,
	pi->recovery_progress, &new_progress, reply,
	&(pi->stat));
      // Handle the case of a read error right after we wrote, which is
      // hopefuilly extremely rare.
      if (r < 0) {
        dout(5) << __func__ << ": oid " << soid << " error " << r << dendl;

	error = true;
	goto done;
      }
      pi->recovery_progress = new_progress;
      return true;
    } else {

      // has recovered the object on the specified peer

      // done!
done:
      if (!error)
	get_parent()->on_peer_recover( peer, soid, pi->recovery_info);

      get_parent()->release_locks(pi->lock_manager);
      object_stat_sum_t stat = pi->stat;
      eversion_t v = pi->recovery_info.version;
      pushing[soid].erase(peer);
      pi = NULL;

      if (pushing[soid].empty()) {
	if (!error)
	  get_parent()->on_global_recover(soid, stat);
	else
	  get_parent()->on_primary_error(soid, v);

	pushing.erase(soid);
      } else {
	// This looks weird, but we erased the current peer and need to remember
	// the error on any other one, while getting more acks.
	if (error)
	  pushing[soid].begin()->second.recovery_progress.error = true;
	dout(10) << "pushed " << soid << ", still waiting for push ack from "
		 << pushing[soid].size() << " others" << dendl;
      }

      return false;
    }
  }
}

// called by
// ReplicatedBackend::do_pull
void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = store->stat(ch, ghobject_t(soid), &st);

  if (r != 0) {

    // the object to pull does not exist on this replica PG

    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r);
    prep_push_op_blank(soid, reply);
  } else {
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;

    if (progress.first && recovery_info.size == ((uint64_t)-1)) {

      // the first request of the pulling sequence of the object
      // and pulling the whole object, see ReplicatedBackend::prepare_pull

      // Adjust size and copy_subset
      recovery_info.size = st.st_size;

      recovery_info.copy_subset.clear();
      if (st.st_size)
        recovery_info.copy_subset.insert(0, st.st_size);

      assert(recovery_info.clone_subset.empty());
    }

    // build the first PushOp of the object to the peer, the next
    // PushOps will continue in ReplicatedBackend::handle_push_reply
    r = build_push_op(recovery_info, progress, 0, reply);
    if (r < 0)
      prep_push_op_blank(soid, reply);
  }
}

// static
// called by
// ReplicatedBackend::handle_pull_response
/**
 * trim received data to remove what we don't want
 *
 * @param copy_subset intervals we want
 * @param data_included intervals we got
 * @param data_recieved data we got
 * @param intervals_usable intervals we want to keep
 * @param data_usable matching data we want to keep
 */
void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset,
				    intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
       p != intervals_received.end();
       ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin();
	 q != x.end();
	 ++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

void ReplicatedBackend::_failed_pull(pg_shard_t from, const hobject_t &soid)
{
  dout(20) << __func__ << ": " << soid << " from " << from << dendl;
  list<pg_shard_t> fl = { from };
  get_parent()->failed_push(fl, soid);

  clear_pull(pulling.find(soid));
}

void ReplicatedBackend::clear_pull_from(
  map<hobject_t, PullInfo>::iterator piter)
{
  auto from = piter->second.from;
  pull_from_peer[from].erase(piter->second.soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
}

void ReplicatedBackend::clear_pull(
  map<hobject_t, PullInfo>::iterator piter,
  bool clear_pull_from_peer)
{
  if (clear_pull_from_peer) {
    clear_pull_from(piter);
  }
  get_parent()->release_locks(piter->second.lock_manager);
  pulling.erase(piter);
}

// called by
// ReplicatedBackend::recover_object
// C_ReplicatedBackend_OnPullComplete::finish
int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  list< map<pg_shard_t, pg_missing_t>::const_iterator > shards;

  dout(20) << __func__ << " soid " << soid << dendl;
  // who needs it?
  assert(get_parent()->get_actingbackfill_shards().size() > 0);

  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {

    // iterate set<pg_shard_t> PG::actingbackfill

    if (*i == get_parent()->whoami_shard()) continue;

    pg_shard_t peer = *i;

    // map<pg_shard_t, pg_missing_t> PG::peer_missing
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);
    assert(j != get_parent()->get_shard_missing().end());

    if (j->second.is_missing(soid)) {
      shards.push_back(j);
    }
  }

  // If more than 1 read will occur ignore possible request to not cache
  bool cache = shards.size() == 1 ? h->cache_dont_need : false;

  for (auto j : shards) {
    pg_shard_t peer = j->first;
    h->pushes[peer].push_back(PushOp());
    int r = prep_push_to_replica(obc, soid, peer,
	    &(h->pushes[peer].back()), cache);
    if (r < 0) {
      // Back out all failed reads
      for (auto k : shards) {
	pg_shard_t p = k->first;
	dout(10) << __func__ << " clean up peer " << p << dendl;
	h->pushes[p].pop_back();
	if (p == peer) break;
      }
      return r;
    }
  }
  return shards.size();
}
