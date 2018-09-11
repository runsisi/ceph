#include <chrono>
#include <iostream>
#include <time.h>
#include <sys/time.h>

using namespace std;

int main() {
  using std::chrono::duration_cast;
  using std::chrono::seconds;
  using std::chrono::hours;
  using std::chrono::milliseconds;

  auto now = std::chrono::system_clock::now();
  auto now_in_hours = duration_cast<hours>(now.time_since_epoch());
  auto h = now_in_hours.count();
  cout << h << endl;

  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  cout << tp.tv_sec << endl;

  struct timeval tv;
  gettimeofday(&tv, NULL);
  cout << tv.tv_sec << endl;

  //struct timespec ts;
  //ts.tv_sec = duration_cast<seconds>(when.time_since_epoch()).count();
  //auto tv_nsec = (when.time_since_epoch() % seconds(1)).count();
  //cout << tv_nsec << endl;

  //auto now = chrono::system_clock::now();
  //cout << chrono::system_clock::period::num << endl;
  //cout << chrono::system_clock::period::den << endl;

  return 0;
}
