/*
 * slow_cond_wait.cc
 *
 *  Created on: Aug 25, 2018
 *      Author: runsisi
 */

#include <list>
#include <string>
#include <vector>
#include <set>
#include <chrono>
#include <sstream>
#include <iostream>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <assert.h>

using namespace std;

//#define assert(x)

class WorkThread;
class ThreadPool;

class Mutex {
public:
  bool recursive;

  pthread_mutex_t _m;
  int nlock;
  pthread_t locked_by;

  void _post_lock() {
    if (!recursive) {
      assert(nlock == 0);
      locked_by = pthread_self();
    };
    nlock++;
  }

  void _pre_unlock() {
    assert(nlock > 0);
    --nlock;
    if (!recursive) {
      assert(locked_by == pthread_self());
      locked_by = 0;
      assert(nlock == 0);
    }
  }

public:
  void operator=(const Mutex &M) = delete;
  Mutex(const Mutex &M) = delete;

  Mutex(bool r = false)
    : nlock(0), locked_by(0), recursive(r) {
    if (recursive) {
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&_m,&attr);
      pthread_mutexattr_destroy(&attr);
    } else {
      // If the mutex type is PTHREAD_MUTEX_DEFAULT, attempting to recursively
      // lock the mutex results in undefined behavior. Attempting to unlock the
      // mutex if it was not locked by the calling thread results in undefined
      // behavior. Attempting to unlock the mutex if it is not locked results in
      // undefined behavior.
      pthread_mutex_init(&_m, NULL);
    }
  }

  ~Mutex() {
    assert(nlock == 0);

    pthread_mutex_destroy(&_m);
  }

  bool is_locked() const {
    return (nlock > 0);
  }

  bool is_locked_by_me() const {
    return nlock > 0 && locked_by == pthread_self();
  }

  bool TryLock() {
    int r = pthread_mutex_trylock(&_m);
    if (r == 0) {
      _post_lock();
    }
    return r == 0;
  }

  void Lock() {
    int r;
    r = pthread_mutex_lock(&_m);
    assert(r == 0);
    _post_lock();
  }

  void Unlock() {
    _pre_unlock();
    int r = pthread_mutex_unlock(&_m);
    assert(r == 0);
  }

  friend class Cond;

public:
  class Locker {
    Mutex &mutex;

  public:
    explicit Locker(Mutex& m) : mutex(m) {
      mutex.Lock();
    }
    ~Locker() {
      mutex.Unlock();
    }
  };
};

class Cond {
public:
  pthread_cond_t _c;

  Mutex *waiter_mutex;

public:
  void operator=(Cond &C) = delete;
  Cond(const Cond &C) = delete;

  Cond() : waiter_mutex(NULL) {
    int r = pthread_cond_init(&_c,NULL);
    assert(r == 0);
  }
  ~Cond() {
    pthread_cond_destroy(&_c);
  }

  int Wait(Mutex &mutex)  {
    // make sure this cond is used with one mutex only
    assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    assert(mutex.is_locked());

    mutex._pre_unlock();
    int r = pthread_cond_wait(&_c, &mutex._m);
    mutex._post_lock();
    return r;
  }

  int WaitInterval(Mutex &mutex, int interval) {
    using std::chrono::duration_cast;
    using std::chrono::seconds;

    // system_clock has precision in nanoseconds
    std::chrono::system_clock::time_point when = std::chrono::system_clock::now();
    when += seconds(interval);

    struct timespec ts;
    ts.tv_sec = duration_cast<seconds>(when.time_since_epoch()).count();
    ts.tv_nsec = (when.time_since_epoch() % seconds(1)).count();

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  int Signal() {
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
           waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int SignalOne() {
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
           waiter_mutex->is_locked());

    int r = pthread_cond_signal(&_c);
    return r;
  }
};

struct WorkQueue_ {
  WorkQueue_() { }
  virtual ~WorkQueue_() {}
  /// Remove all work items from the queue.
  virtual void _clear() = 0;
  /// Check whether there is anything to do.
  virtual bool _empty() = 0;
  /// Get the next work item to process.
  virtual void *_void_dequeue() = 0;
  /** @brief Process the work item.
   * This function will be called several times in parallel
   * and must therefore be thread-safe. */
  virtual void _void_process(void *item) = 0;
  /** @brief Synchronously finish processing a work item.
   * This function is called after _void_process with the global thread pool lock held,
   * so at most one copy will execute simultaneously for a given thread pool.
   * It can be used for non-thread-safe finalization. */
  virtual void _void_process_finish(void *) = 0;
};

class Thread {
public:
  pthread_t thread_id;

  void *entry_wrapper() {
    return entry();
  }

public:
  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;

  Thread() : thread_id(0) {

  }
  virtual ~Thread() {

  }

  virtual void *entry() = 0;

  static void *_entry_func(void *arg) {
    void *r = ((Thread*)arg)->entry_wrapper();
      return r;
  }

  const pthread_t &get_thread_id() const {
    return thread_id;
  }
  bool is_started() const {
    return thread_id != 0;
  }
  bool am_self() const {
    return (pthread_self() == thread_id);
  }
  int try_create(size_t stacksize) {
    pthread_attr_t *thread_attr = NULL;
    pthread_attr_t thread_attr_loc;

    stacksize &= 4095;  // must be multiple of page
    if (stacksize) {
      thread_attr = &thread_attr_loc;
      pthread_attr_init(thread_attr);
      pthread_attr_setstacksize(thread_attr, stacksize);
    }

    int r;
    r = pthread_create(&thread_id, thread_attr, _entry_func, (void*)this);

    if (thread_attr) {
      pthread_attr_destroy(thread_attr);
    }

    return r;
  }
  void create(size_t stacksize = 0) {
    int ret = try_create(stacksize);
    if (ret != 0) {
      assert(ret == 0);
    }
  }
  int join(void **prval = 0) {
    if (thread_id == 0) {
      assert("join on thread that was never started" == 0);
      return -EINVAL;
    }

    int status = pthread_join(thread_id, prval);
    if (status != 0) {
      assert(status == 0);
    }

    thread_id = 0;
    return status;
  }
  int detach() {
    return pthread_detach(thread_id);
  }
};

struct WorkThread : public Thread {
  ThreadPool *pool;

  WorkThread(ThreadPool *p) : pool(p) {}
  void *entry() override;
};

class ThreadPool {
public:
  Mutex _lock;
  Cond _cond;
  bool _stop;
  int _pause;
  int _draining;
  Cond _wait_cond;

  // track thread pool size changes
  unsigned _num_threads;
  vector<WorkQueue_*> work_queues;
  int next_work_queue = 0;

  set<WorkThread*> _threads;
  list<WorkThread*> _old_threads;  ///< need to be joined
  int processing;

  void start_threads() {
    assert(_lock.is_locked());
    while (_threads.size() < _num_threads) {
      WorkThread *wt = new WorkThread(this);
      _threads.insert(wt);

      wt->create();
    }
  }
  void join_old_threads() {
    assert(_lock.is_locked());
    while (!_old_threads.empty()) {
      _old_threads.front()->join();
      delete _old_threads.front();
      _old_threads.pop_front();
    }
  }

public:
  ThreadPool(int n)
    : _lock("threadpool::lock"),  // this should be safe due to declaration order
      _stop(false),
      _pause(0),
      _draining(0),
      _num_threads(n),
      processing(0) {
  }

  ~ThreadPool() {
    assert(_threads.empty());
  }

  /// return number of threads currently running
  int get_num_threads() {
    Mutex::Locker l(_lock);
    return _num_threads;
  }

  /// assign a work queue to this thread pool
  void add_work_queue(WorkQueue_* wq) {
    Mutex::Locker l(_lock);
    work_queues.push_back(wq);
  }
  /// remove a work queue from this thread pool
  void remove_work_queue(WorkQueue_* wq) {
    Mutex::Locker l(_lock);
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++)
      work_queues[i-1] = work_queues[i];
    assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  /// start thread pool thread
  void start() {
    _lock.Lock();
    start_threads();
    _lock.Unlock();
  }

  /// stop thread pool thread
  void stop(bool clear_after=true) {
    _lock.Lock();
    _stop = true;
    _cond.Signal();
    join_old_threads();
    _lock.Unlock();
    for (set<WorkThread*>::iterator p = _threads.begin();
         p != _threads.end();
         ++p) {
      (*p)->join();
      delete *p;
    }
    _threads.clear();
    _lock.Lock();
    for (unsigned i=0; i<work_queues.size(); i++)
      work_queues[i]->_clear();
    _stop = false;
    _lock.Unlock();
  }

  /// pause thread pool (if it not already paused)
  void pause() {
    _lock.Lock();
    _pause++;
    while (processing)
      _wait_cond.Wait(_lock);
    _lock.Unlock();
  }

  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause() {
    _lock.Lock();
    assert(_pause > 0);
    _pause--;
    _cond.Signal();
    _lock.Unlock();
  }

  /** @brief Wait until work completes.
   * If the parameter is NULL, blocks until all threads are idle.
   * If it is not NULL, blocks until the given work queue does not have
   * any items left to process. */
  void drain(WorkQueue_* wq = 0) {
    _lock.Lock();
    _draining++;
    while (processing || (wq != NULL && !wq->_empty()))
      _wait_cond.Wait(_lock);
    _draining--;
    _lock.Unlock();
  }

  void worker(WorkThread *wt) {
    _lock.Lock();
    cout << "worker start" << endl;

    while (!_stop) {

      // manage dynamic thread pool
      join_old_threads();
      if (_threads.size() > _num_threads) {
        _threads.erase(wt);
        _old_threads.push_back(wt);
        break;
      }

      if (!_pause && !work_queues.empty()) {
        WorkQueue_* wq;
        int tries = work_queues.size();
        bool did = false;
        cout << "tries: " << tries << endl;

        while (tries--) {
          cout << "next_work_queue: " << next_work_queue << endl;
          next_work_queue %= work_queues.size();
          wq = work_queues[next_work_queue++];

          void *item = wq->_void_dequeue();
          if (item) {
            processing++;
            _lock.Unlock();

            wq->_void_process(item);

            _lock.Lock();
            wq->_void_process_finish(item);
            processing--;
            if (_pause || _draining)
              _wait_cond.Signal();
            did = true;
            break;
          }
        }
        if (did)
          continue;
      }

      cout << "wait next_work_queue: " << next_work_queue << endl;

      auto begin = std::chrono::system_clock::now();
      _cond.WaitInterval(_lock, 1);
      auto end = std::chrono::system_clock::now();
      auto diff = end - begin;
      auto max = std::chrono::milliseconds(1500);
      if (diff > max) {
        cout << "cond var wait: " << diff.count() << " > "
            << std::chrono::duration_cast<std::chrono::nanoseconds>(max).count() << endl;
        assert(false);
      }
    }
    cout << "worker finish" << endl;

    _lock.Unlock();
  }
};

void *WorkThread::entry() {
  pool->worker(this);
  return 0;
}

template<typename T>
class WorkQueue : public WorkQueue_ {
private:
  ThreadPool *m_pool;
  std::list<T *> m_items;
  uint32_t m_processing;

public:
  WorkQueue(ThreadPool* p)
    : m_pool(p), m_processing(0) {
    m_pool->add_work_queue(this);
  }
  virtual ~WorkQueue() {
    m_pool->remove_work_queue(this);
    assert(m_processing == 0);
  }

  void _clear() override {
    assert(m_pool->_lock.is_locked());
    m_items.clear();
  }
  bool _empty() override {
    assert(m_pool->_lock.is_locked());
    return m_items.empty();
  }
  void *_void_dequeue() override {
    assert(m_pool->_lock.is_locked());
    if (m_items.empty()) {
      return NULL;
    }
    ++m_processing;
    T *item = m_items.front();
    m_items.pop_front();
    return item;
  }
  void _void_process(void *item) override {
    process(reinterpret_cast<T *>(item));
  }
  void _void_process_finish(void *item) override {
    assert(m_pool->_lock.is_locked());
    assert(m_processing > 0);
    --m_processing;
  }

  T *front() {
    assert(m_pool->_lock.is_locked());
    if (m_items.empty()) {
      return NULL;
    }
    return m_items.front();
  }

  void queue(T *item) {
    Mutex::Locker l(m_pool->_lock);
    m_items.push_back(item);
    m_pool->_cond.SignalOne();
  }
  void drain() {
    {
      // if this queue is empty and not processing, don't wait for other
      // queues to finish processing
      Mutex::Locker l(m_pool->_lock);
      if (m_processing == 0 && m_items.empty()) {
        return;
      }
    }
    m_pool->drain(this);
  }

  virtual void process(T *item) = 0;
};


/////////////////////////////////////////////////////////////////////////

struct Op {
public:
  Op(uint64_t id) {
    this->id = id;
  }

  uint64_t id;
  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point end;
};

class OpQueue : public WorkQueue<Op> {
public:
  OpQueue(ThreadPool *pool)
    : WorkQueue<Op>(pool) {

  }
  void process(Op *op) override {
    if (op->start.time_since_epoch().count() != 0) {
      op->end = std::chrono::system_clock::now();
      auto elapsed = op->end - op->start;

      // in nano seconds
      cout << op->id << ": " << elapsed.count() << endl;
    } else {
      cout << op->id << endl;
    }

    delete op;
  }
};

class IoQueue : public WorkQueue<Op> {
public:
  OpQueue *opq;
  bool first = true;

  IoQueue(ThreadPool *pool, OpQueue *opq)
    : WorkQueue<Op>(pool) {
    this->opq = opq;
  }
  void *_void_dequeue() override {
    Op *op = reinterpret_cast<Op *>(WorkQueue<Op>::_void_dequeue());
    if (op == nullptr) {
      return nullptr;
    }

    if (first) {
      return WorkQueue<Op>::_void_dequeue();
    } else {
      op->start = std::chrono::system_clock::now();

      opq->queue(op);

      _void_process_finish(op);

      return nullptr;
    }

    return WorkQueue<Op>::_void_dequeue();
  }
  void process(Op *op) override {
    if (first) {
      first = false;
    } else {
      op->start = std::chrono::system_clock::now();

      opq->queue(op);
    }
  }
};

bool g_stop = false;

static void sigint_handler(int) {
  cout << "\nstopping..\n";
  g_stop = true;
}

int main() {
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = sigint_handler;
  sigfillset(&act.sa_mask);  // mask all signals in the handler
  act.sa_flags = SA_SIGINFO;
  sigaction(SIGINT, &act, &oldact);

  ThreadPool *p = new ThreadPool(1);
  p->start();

  OpQueue *opq = new OpQueue{p};
  IoQueue *ioq = new IoQueue{p, opq};

  uint64_t id = 0;

  bool enqop = true;

  do {
    if (enqop) {
      int i = 1;
      while (i--) {
        Op *op = new Op{id++};
        opq->queue(op);

        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 10000000;
        nanosleep(&ts, NULL);
      }

      enqop = false;
    } else {
      Op *op = new Op{id++};
      ioq->queue(op);

      enqop = true;
    }

    struct timespec ts;
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    nanosleep(&ts, NULL);
  } while (!g_stop);

  ioq->drain();
  delete ioq;
  opq->drain();
  delete opq;

  p->stop();
  delete p;

  cout << "total " << --id << " Op(s) processed\n";

  return 0;
}
