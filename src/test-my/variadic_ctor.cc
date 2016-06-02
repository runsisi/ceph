/*
 * variadic_ctor.cc
 *
 *  Created on: Mar 31, 2017
 *      Author: runsisi
 */


#include <iostream>
using namespace std;

#define debug do {cout << __func__ << ", " << __LINE__ << endl;} while (0)

struct pg_log_t {
  pg_log_t() = default;

  pg_log_t(int dummy) {
    debug;
  }
};

struct IndexedLog : public pg_log_t {
IndexedLog() {
  debug;
}

template <typename... Args>
IndexedLog(Args&&... args) :
  pg_log_t(std::forward<Args>(args)...) {
  debug;
}

IndexedLog(const IndexedLog &rhs) :
  pg_log_t(rhs) {
  debug;
}


IndexedLog &operator=(const IndexedLog &rhs) {
  debug;
  this->~IndexedLog();
  new (this) IndexedLog(rhs);
  return *this;
}

void dummy(const pg_log_t& o) {
  *this = IndexedLog(o);
}
};

int main() {
  debug;
  IndexedLog l;
  pg_log_t o;
  l.dummy(o);
  return 0;
}
