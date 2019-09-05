// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 */


#pragma once


#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#include <limits>
#include <cmath>
#include <chrono>


namespace crimson {
  namespace dmclock {
    // we're using double to represent time, but we could change it by
    // changing the following declarations (and by making sure a min
    // function existed)
    using Time = double;
    static const Time TimeZero = 0.0;
    static const Time TimeMax = std::numeric_limits<Time>::max();
    static const double NaN = nan("");


    inline Time get_time() {
#if defined(__linux__)
      struct timespec now;
      auto result = clock_gettime(CLOCK_REALTIME, &now);
      (void) result; // reference result in case assert is compiled out
      assert(0 == result);
      return now.tv_sec + (now.tv_nsec / 1.0e9);
#else
      struct timeval now;
      auto result = gettimeofday(&now, NULL);
      (void) result; // reference result in case assert is compiled out
      assert(0 == result);
      return now.tv_sec + (now.tv_usec / 1.0e6);
#endif
    }

    inline int gen_server_id(int osd, int shard) {
      const int mask = 0xffff0000;
      return ((osd << 16) & mask) | (shard & (mask >> 16));
    }
    inline std::pair<int, int> get_osd_shard(int serverid) {
      const int mask = 0x0000ffff;
      int osd, shard;
      osd = (serverid >> 16) & mask;
      shard = serverid & mask;
      return std::make_pair(osd, shard);
    }

    std::string format_time(const Time& time, unsigned int modulo = 1000);

    void debugger();

  } // namespace dmclock
} // namespace crimson
