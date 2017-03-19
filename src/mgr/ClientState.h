// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_CLIENT_STATE_H_
#define CEPH_MGR_CLIENT_STATE_H_

#include <boost/circular_buffer.hpp>
#include <map>
#include <memory>
#include <set>
#include <string>
#include "common/Mutex.h"
#include "messages/MMgrReport.h"
#include "msg/msg_types.h"

namespace mgr_client {

struct PerfCounter {
  uint32_t type_;
  uint64_t u64_;
  uint64_t avgcount_;
};

struct PerfCounters {
  std::map<std::string, PerfCounterDataPoint> m_counters;
};

class PerfCounterDataPoint {
public:
  utime_t time_;
  PerfCounter counter_;
  PerfCounterDataPoint();
};

class PerfCounterInstance {
public:
  PerfCounterInstance() : buffer(20) {}
  const boost::circular_buffer<PerfCounterDataPoint>& get_data() const {
    return data_points_;
  }
  void push(utime_t t, const uint64_t &v);
private:
  boost::circular_buffer<PerfCounterDataPoint> data_points_;
};

class ClientPerfCounters
{
public:
  ClientPerfCounters(const PerfCounterTypes &types) : types_(types) {}
  void update(const MMgrReport *report);
  void clear() {
    instances_.clear();
  }
private:
  std::map<std::string, PerfCounterDataPoint> instances_;
};

typedef std::pair<entity_type_t, std::string> ClientKey;
typedef std::pair<std::string, std::string> ImageKey;

class ImageState
{
public:
  ClientKey client_key_;
  ImageKey image_key_;
  std::string snap_name_;
  std::map<std::string, PerfCounterInstance> perf_instances_;
};

typedef std::shared_ptr<ImageState> ImageStatePtr;

class ClientStateIndex
{
private:
  Mutex lock_;

  // RBD/RGW/FS specific fields
  std::multimap<ClientKey, ImageStatePtr> image_states_by_client_;
  std::multimap<ImageKey, ImageStatePtr> image_states_;
private:
  void update();
public:
  ClientStateIndex() : lock_("ClientStateIndex") {}
  void handle_report(const MMgrReport *report);
  void erase(const ClientKey &key);
};

} // namespace mgr_client

#endif // CEPH_MGR_CLIENT_STATE_H_

