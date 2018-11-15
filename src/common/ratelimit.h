#ifndef CEPH_COMMON_RATELIMIT_H
#define CEPH_COMMON_RATELIMIT_H

#include <atomic>
#include <chrono>

constexpr unsigned char kRatelimitInterval = 5;
constexpr unsigned char kRatelimitBurst = 10;

struct ratelimit_state
{
  std::atomic_flag lock = ATOMIC_FLAG_INIT;

  unsigned char interval = kRatelimitInterval;
  unsigned char burst = kRatelimitBurst;
  unsigned int printed = 0;
  std::chrono::time_point<std::chrono::steady_clock> begin;

  ratelimit_state() { }
  ratelimit_state(unsigned char i, unsigned char b) : interval(i), burst(b) { }
};

#define DEFINE_RATELIMIT_STATE(name) \
  struct ratelimit_state name
#define DEFINE_RATELIMIT_STATE_EX(name, interval, burst) \
  struct ratelimit_state name(interval, burst)

/**
 * Check if rate limit needs to be enforced.
 *
 * No more than s->burst calls in every s->interval.
 *
 * Input:
 * @param s the rate limit state
 *
 * Output:
 * @returns true if rate limit needs to be enforced, false otherwise
 */
// since __attribute__((always_inline)) is not defined, it won't be inlined
inline bool ratelimit_check(struct ratelimit_state *s)
{
  if (s->lock.test_and_set(std::memory_order_acquire)) {
    return true;
  }

  if (!s->begin.time_since_epoch().count()) {
    s->begin = std::chrono::steady_clock::now();
  }

  auto now = std::chrono::steady_clock::now();
  if (now > s->begin + std::chrono::seconds(s->interval)) {
    // restart a new interval
    s->begin = std::chrono::time_point<std::chrono::steady_clock>();
    s->printed = 0;
  }

  bool r = true;
  if (s->printed < s->burst) {
    s->printed++;
    r = false;
  }

  s->lock.clear(std::memory_order_release);
  return r;
}

#endif
