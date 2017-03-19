// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ClientState.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr::ClientState: " << __func__ << " "

void ClientStateIndex::handle_report(const MMgrReport *report)
{
  Mutex::Locker l(lock_);

  ClientKey key(report->get_connection()->get_peer_type(),
                report->daemon_name);

  auto r = perf_counters_.emplace({key, types});
  perf_counters_[key].update(report);

  update();
}


void ClientStateIndex::erase(const ClientKey &key)
{
  Mutex::Locker l(lock_);

  perf_counters_.erase(key);

  update();
}

void ClientStateIndex::update()
{
  assert(lock_.is_locked_by_me());

  perf_counters_.erase(key);
}

void ClientPerfCounters::update(const MMgrReport *report)
{
  dout(20) << "loading " << report->declare_types.size() << " new types, "
           << report->packed.length() << " bytes of data" << dendl;

  // Load any newly declared types
  for (const auto &t : report->declare_types) {
    declared_types.insert(t.path);
    types.insert(std::make_pair(t.path, t));
  }

  const auto now = ceph_clock_now();

  // Parse packed data according to declared set of types
  bufferlist::iterator p = report->packed.begin();
  DECODE_START(1, p);
  for (const auto &path : declared_types) {
    const auto &t = types.at(path);

    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    ::decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      ::decode(avgcount, p);
      ::decode(avgcount2, p);
    }

    // TODO: interface for insertion of avgs
    instances[path].push(now, val);
  }
  DECODE_FINISH(p);
}

void PerfCounterInstance::push(utime_t t, const uint64_t &v)
{
  buffer.push_back({t, v});
}

