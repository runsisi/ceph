// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "OSDMapMapping.h"
#include "OSDMap.h"

#define dout_subsys ceph_subsys_mon

#include "common/debug.h"

MEMPOOL_DEFINE_OBJECT_FACTORY(OSDMapMapping, osdmapmapping,
			      osdmap_mapping);

// called by
// OSDMapMapping::_start, which called by MappingJob::MappingJob
// ensure that we have a PoolMappings for each pool and that
// the dimensions (pg_num and size) match up.
void OSDMapMapping::_init_mappings(const OSDMap& osdmap)
{
  num_pgs = 0;
  auto q = pools.begin();
  for (auto& p : osdmap.get_pools()) {
    num_pgs += p.second.get_pg_num();

    // drop unneeded pools
    while (q != pools.end() && q->first < p.first) {
      q = pools.erase(q);
    }

    if (q != pools.end() && q->first == p.first) {
      if (q->second.pg_num != p.second.get_pg_num() ||
	  q->second.size != p.second.get_size()) {
	// pg_num changed
	q = pools.erase(q);
      } else {
	// keep it
	++q;
	continue;
      }
    }

    pools.emplace(p.first, PoolMapping(p.second.get_size(),
				       p.second.get_pg_num(),
				       p.second.is_erasure()));
  }
  pools.erase(q, pools.end());
  ceph_assert(pools.size() == osdmap.get_pools().size());
}

// never used
void OSDMapMapping::update(const OSDMap& osdmap)
{
  _start(osdmap);
  for (auto& p : osdmap.get_pools()) {
    _update_range(osdmap, p.first, 0, p.second.get_pg_num());
  }
  _finish(osdmap);
  //_dump();  // for debugging
}

// never used
void OSDMapMapping::update(const OSDMap& osdmap, pg_t pgid)
{
  _update_range(osdmap, pgid.pool(), pgid.ps(), pgid.ps() + 1);
}

// called by
// OSDMapMapping::_finish, which called by MappingJob::complete
void OSDMapMapping::_build_rmap(const OSDMap& osdmap)
{
  // vector<vector<pg_t>>
  acting_rmap.resize(osdmap.get_max_osd());

  //up_rmap.resize(osdmap.get_max_osd());
  for (auto& v : acting_rmap) {
    v.resize(0);
  }
  //for (auto& v : up_rmap) {
  //  v.resize(0);
  //}
  for (auto& p : pools) {
    pg_t pgid(0, p.first);
    for (unsigned ps = 0; ps < p.second.pg_num; ++ps) {
      pgid.set_ps(ps);
      int32_t *row = &p.second.table[p.second.row_size() * ps];
      for (int i = 0; i < row[2]; ++i) {
	if (row[4 + i] != CRUSH_ITEM_NONE) {
	  acting_rmap[row[4 + i]].push_back(pgid);
	}
      }
      //for (int i = 0; i < row[3]; ++i) {
      //up_rmap[row[4 + p.second.size + i]].push_back(pgid);
      //}
    }
  }
}

// called by
// OSDMapMapping::update, which never used
// MappingJob::complete, which called by ParallelPGMapper::Job::finish_one when all shards finished
void OSDMapMapping::_finish(const OSDMap& osdmap)
{
  _build_rmap(osdmap);
  epoch = osdmap.get_epoch();
}

void OSDMapMapping::_dump()
{
  for (auto& p : pools) {
    cout << "pool " << p.first << std::endl;
    for (unsigned i = 0; i < p.second.table.size(); ++i) {
      cout << " " << p.second.table[i];
      if (i % p.second.row_size() == p.second.row_size() - 1)
	cout << std::endl;
    }
  }
}

// called by
// MappingJob::process, which called by ParallelPGMapper::WQ::_process
void OSDMapMapping::_update_range(
  const OSDMap& osdmap,
  int64_t pool,
  unsigned pg_begin,
  unsigned pg_end)
{
  auto i = pools.find(pool);
  ceph_assert(i != pools.end());
  ceph_assert(pg_begin <= pg_end);
  ceph_assert(pg_end <= i->second.pg_num);
  for (unsigned ps = pg_begin; ps < pg_end; ++ps) {
    vector<int> up, acting;
    int up_primary, acting_primary;

    osdmap.pg_to_up_acting_osds(
      pg_t(ps, pool),
      &up, &up_primary, &acting, &acting_primary);

    // PoolMapping::set
    i->second.set(ps, std::move(up), up_primary,
		  std::move(acting), acting_primary);
  }
}

// ---------------------------

void ParallelPGMapper::Job::finish_one()
{
  Context *fin = nullptr;
  {
    Mutex::Locker l(lock);
    if (--shards == 0) {
      if (!aborted) {
	finish = ceph_clock_now();
	complete();
      }
      cond.Signal();
      fin = onfinish;
      onfinish = nullptr;
    }
  }
  if (fin) {
    fin->complete(0);
  }
}

void ParallelPGMapper::WQ::_process(Item *i, ThreadPool::TPHandle &h)
{
  ldout(m->cct, 20) << __func__ << " " << i->job << " " << i->pool
		    << " [" << i->begin << "," << i->end << ")" << dendl;

  // process one shard
  i->job->process(i->pool, i->begin, i->end);

  // finish one shard
  i->job->finish_one();
  delete i;
}

// called by
// OSDMapMapping::start_update, for MappingJob, which called by OSDMonitor::start_mapping
// OSDMonitor::maybe_prime_pg_temp, for PrimeTempJob
void ParallelPGMapper::queue(
  Job *job,
  unsigned pgs_per_item)
{
  bool any = false;
  // for MappingJob, Job::osdmap is current osdmap
  // for PrimeTempJob, Job::osdmap is next osdmap
  for (auto& p : job->osdmap->get_pools()) {
    for (unsigned ps = 0; ps < p.second.get_pg_num(); ps += pgs_per_item) {
      unsigned ps_end = std::min(ps + pgs_per_item, p.second.get_pg_num());
      job->start_one();

      wq.queue(new Item(job, p.first, ps, ps_end));

      ldout(cct, 20) << __func__ << " " << job << " " << p.first << " [" << ps
		     << "," << ps_end << ")" << dendl;
      any = true;
    }
  }
  ceph_assert(any);
}
