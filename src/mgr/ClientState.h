// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_MGR_CLIENT_STATE_H_
#define CEPH_MGR_CLIENT_STATE_H_

#include <map>
#include <string>
#include <memory>
#include <set>
#include <map>
#include <boost/circular_buffer.hpp>

#include "common/Mutex.h"

#include "msg/msg_types.h"

// For PerfCounterType
#include "messages/MMgrReport.h"


typedef std::pair<entity_type_t, std::string> ClientKey;
typedef std::pair<std::string, str::string> ImageKey;

struct ImageState
{

};

// An instance of a performance counter type, within
// a particular daemon.
class PerfCounterInstance
{
  class DataPoint
  {
    public:
    utime_t t;
    uint64_t v;
    DataPoint(utime_t t_, uint64_t v_)
      : t(t_), v(v_)
    {}
  };

  boost::circular_buffer<DataPoint> buffer;
  uint64_t get_current() const;

  public:
  const boost::circular_buffer<DataPoint> & get_data() const
  {
    return buffer;
  }
  void push(utime_t t, uint64_t const &v);
  PerfCounterInstance()
    : buffer(20) {}
};


typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

// Performance counters for one daemon
class ClientPerfCounters
{
public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  ClientPerfCounters(PerfCounterTypes &types_)
    : types(types_)
  {}

  std::map<std::string, PerfCounterInstance> instances;

  // FIXME: this state is really local to DaemonServer, it's part
  // of the protocol rather than being part of what other classes
  // mgiht want to read.  Maybe have a separate session object
  // inside DaemonServer instead of stashing session-ish state here?
  std::set<std::string> declared_types;

  void update(MMgrReport *report);

  void clear()
  {
    instances.clear();
    declared_types.clear();
  }
};

// The state that we store about one daemon
class ClientState
{
public:
  ClientKey key;

  // The hostname where daemon was last seen running (extracted
  // from the metadata)
  std::string hostname;

  // The metadata (hostname, version, etc) sent from the daemon
  std::map<std::string, std::string> metadata;

  // The perf counters received in MMgrReport messages
  ClientPerfCounters perf_counters;
  std::map<ImageKey, ImageState> image_states_;

  ClientState(PerfCounterTypes &types_)
    : perf_counters(types_)
  {
  }
};

typedef std::shared_ptr<ClientState> ClientStatePtr;
typedef std::map<ClientKey, ClientStatePtr> ClientStateCollection;




/**
 * Fuse the collection of per-daemon metadata from Ceph into
 * a view that can be queried by service type, ID or also
 * by server (aka fqdn).
 */
class ClientStateIndex
{
  private:
  std::map<std::string, ClientStateCollection> by_server;
  ClientStateCollection all;

  std::set<ClientKey> updating;

  mutable Mutex lock;

  public:

  ClientStateIndex() : lock("ClientState") {}

  // FIXME: shouldn't really be public, maybe construct ClientState
  // objects internally to avoid this.
  PerfCounterTypes types;

  void insert(ClientStatePtr dm);
  void _erase(ClientKey dmk);

  bool exists(const ClientKey &key) const;
  ClientStatePtr get(const ClientKey &key);
  ClientStateCollection get_by_server(const std::string &hostname) const;
  ClientStateCollection get_by_type(uint8_t type) const;

  const ClientStateCollection &get_all() const {return all;}
  const std::map<std::string, ClientStateCollection> &get_all_servers() const
  {
    return by_server;
  }

  void notify_updating(const ClientKey &k) { updating.insert(k); }
  void clear_updating(const ClientKey &k) { updating.erase(k); }
  bool is_updating(const ClientKey &k) { return updating.count(k) > 0; }

  /**
   * Remove state for all daemons of this type whose names are
   * not present in `names_exist`.  Use this function when you have
   * a cluster map and want to ensure that anything absent in the map
   * is also absent in this class.
   */
  void cull(entity_type_t daemon_type, std::set<std::string> names_exist);
};

#endif // CEPH_MGR_CLIENT_STATE_H_

