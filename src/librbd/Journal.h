// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_H
#define CEPH_LIBRBD_JOURNAL_H

#include "include/int_types.h"
#include "include/atomic.h"
#include "include/Context.h"
#include "include/interval_set.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "journal/Future.h"
#include "journal/JournalMetadataListener.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include <algorithm>
#include <list>
#include <string>
#include <unordered_map>

class ContextWQ;
class SafeTimer;
namespace journal {
class Journaler;
}
namespace librados {
  class IoCtx;
}

namespace librbd {

struct AioObjectRequestHandle;
class ImageCtx;

namespace journal { template <typename> class Replay; }

template <typename ImageCtxT = ImageCtx>
class Journal {
public:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UNINITIALIZED ---> INITIALIZING ---> REPLAYING ------> FLUSHING ---> READY
   *    |                 *  .  ^             *  .              *           |
   *    |                 *  .  |             *  .              *           |
   *    |                 *  .  |    (error)  *  . . . . . . .  *           |
   *    |                 *  .  |             *              .  *           |
   *    |                 *  .  |             v              .  *           |
   *    |                 *  .  |         FLUSHING_RESTART   .  *           |
   *    |                 *  .  |             |              .  *           |
   *    |                 *  .  |             |              .  *           |
   *    |                 *  .  |             v              .  *           v
   *    |                 *  .  |         RESTARTING  < * * * * *       STOPPING
   *    |                 *  .  |             |              .              |
   *    |                 *  .  |             |              .              |
   *    |       * * * * * *  .  \-------------/              .              |
   *    |       * (error)    .                               .              |
   *    |       *            .   . . . . . . . . . . . . . . .              |
   *    |       *            .   .                                          |
   *    |       v            v   v                                          |
   *    |     CLOSED <----- CLOSING <---------------------------------------/
   *    |       |
   *    |       v
   *    \---> <finish>
   *
   * @endverbatim
   */
  enum State {
    STATE_UNINITIALIZED,
    STATE_INITIALIZING,
    STATE_REPLAYING,
    STATE_FLUSHING_RESTART,
    STATE_RESTARTING_REPLAY,
    STATE_FLUSHING_REPLAY,
    STATE_READY,
    STATE_STOPPING,
    STATE_CLOSING,
    STATE_CLOSED
  };

  static const std::string IMAGE_CLIENT_ID;
  static const std::string LOCAL_MIRROR_UUID;
  static const std::string ORPHAN_MIRROR_UUID;

  typedef std::list<AioObjectRequestHandle *> AioObjectRequests;

  Journal(ImageCtxT &image_ctx);
  ~Journal();

  static bool is_journal_supported(ImageCtxT &image_ctx);
  static int create(librados::IoCtx &io_ctx, const std::string &image_id,
                    uint8_t order, uint8_t splay_width,
                    const std::string &object_pool);
  static int remove(librados::IoCtx &io_ctx, const std::string &image_id);
  static int reset(librados::IoCtx &io_ctx, const std::string &image_id);

  static int is_tag_owner(ImageCtxT *image_ctx, bool *is_tag_owner);
  static int is_tag_owner(librados::IoCtx& io_ctx, std::string& image_id,
                          bool *is_tag_owner);
  static void is_tag_owner(ImageCtxT *image_ctx, bool *is_tag_owner,
                           Context *on_finish);
  static int get_tag_owner(ImageCtxT *image_ctx, std::string *mirror_uuid);
  static int get_tag_owner(librados::IoCtx& io_ctx, std::string& image_id,
                           std::string *mirror_uuid);
  static int request_resync(ImageCtxT *image_ctx);
  static int promote(ImageCtxT *image_ctx);

  bool is_journal_ready() const;
  bool is_journal_replaying() const;
  bool is_journal_appending() const;

  void wait_for_journal_ready(Context *on_ready);

  void open(Context *on_finish);
  void close(Context *on_finish);

  bool is_tag_owner() const;
  uint64_t get_tag_tid() const;
  journal::TagData get_tag_data() const;
  int demote();

  void allocate_local_tag(Context *on_finish);
  void allocate_tag(const std::string &mirror_uuid,
                    const journal::TagPredecessor &predecessor,
                    Context *on_finish);

  void flush_commit_position(Context *on_finish);

  uint64_t append_write_event(uint64_t offset, size_t length,
                              const bufferlist &bl,
                              const AioObjectRequests &requests,
                              bool flush_entry);
  uint64_t append_io_event(journal::EventEntry &&event_entry,
                           const AioObjectRequests &requests,
                           uint64_t offset, size_t length,
                           bool flush_entry);
  void commit_io_event(uint64_t tid, int r);
  void commit_io_event_extent(uint64_t tid, uint64_t offset, uint64_t length,
                              int r);

  void append_op_event(uint64_t op_tid, journal::EventEntry &&event_entry,
                       Context *on_safe);
  void commit_op_event(uint64_t tid, int r, Context *on_safe);
  void replay_op_ready(uint64_t op_tid, Context *on_resume);

  void flush_event(uint64_t tid, Context *on_safe);
  void wait_event(uint64_t tid, Context *on_safe);

  uint64_t allocate_op_tid() {
    uint64_t op_tid = m_op_tid.inc();
    assert(op_tid != 0);
    return op_tid;
  }

  void start_external_replay(journal::Replay<ImageCtxT> **journal_replay,
                             Context *on_start);
  void stop_external_replay();

  void add_listener(journal::Listener *listener);
  void remove_listener(journal::Listener *listener);

  int is_resync_requested(bool *do_resync);

  inline ContextWQ *get_work_queue() {
    return m_work_queue;
  }

private:
  ImageCtxT &m_image_ctx;

  // mock unit testing support
  typedef journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef typename TypeTraits::Future Future;
  typedef typename TypeTraits::ReplayEntry ReplayEntry;

  typedef std::list<bufferlist> Bufferlists;
  typedef std::list<Context *> Contexts;
  typedef std::list<Future> Futures;
  typedef interval_set<uint64_t> ExtentInterval;

  struct Event {
    Futures futures;
    AioObjectRequests aio_object_requests;
    Contexts on_safe_contexts;
    ExtentInterval pending_extents;
    bool committed_io = false;
    bool safe = false;
    int ret_val = 0;

    Event() {
    }
    Event(const Futures &_futures, const AioObjectRequests &_requests,
          uint64_t offset, size_t length)
      : futures(_futures), aio_object_requests(_requests) {
      if (length > 0) {
        pending_extents.insert(offset, length);
      }
    }
  };

  typedef std::unordered_map<uint64_t, Event> Events;
  typedef std::unordered_map<uint64_t, Future> TidToFutures;

  // used by Journal<I>::append_io_events
  struct C_IOEventSafe : public Context {
    Journal *journal;
    uint64_t tid;

    C_IOEventSafe(Journal *_journal, uint64_t _tid)
      : journal(_journal), tid(_tid) {
    }

    virtual void finish(int r) {
      journal->handle_io_event_safe(r, tid);
    }
  };

  // used by Journal<I>::commit_op_event
  struct C_OpEventSafe : public Context {
    Journal *journal;
    uint64_t tid;
    Future op_start_future;
    Future op_finish_future;
    Context *on_safe;

    C_OpEventSafe(Journal *journal, uint64_t tid, const Future &op_start_future,
                  const Future &op_finish_future, Context *on_safe)
      : journal(journal), tid(tid), op_start_future(op_start_future),
        op_finish_future(op_finish_future), on_safe(on_safe) {
    }

    virtual void finish(int r) {
      journal->handle_op_event_safe(r, tid, op_start_future, op_finish_future,
                                    on_safe);
    }
  };

  // used by Journal<I>::handle_replay_ready
  struct C_ReplayProcessSafe : public Context {
    Journal *journal;
    ReplayEntry replay_entry;

    C_ReplayProcessSafe(Journal *journal, ReplayEntry &&replay_entry) :
      journal(journal), replay_entry(std::move(replay_entry)) {
    }
    virtual void finish(int r) {
      journal->handle_replay_process_safe(replay_entry, r);
    }
  };

  struct ReplayHandler : public ::journal::ReplayHandler {
    Journal *journal;
    ReplayHandler(Journal *_journal) : journal(_journal) {
    }

    virtual void get() {
      // TODO
    }
    virtual void put() {
      // TODO
    }

    virtual void handle_entries_available() {
      journal->handle_replay_ready();
    }
    virtual void handle_complete(int r) {
      journal->handle_replay_complete(r);
    }
  };

  ContextWQ *m_work_queue = nullptr;
  SafeTimer *m_timer = nullptr;
  Mutex *m_timer_lock = nullptr;

  Journaler *m_journaler;
  mutable Mutex m_lock;
  State m_state;
  uint64_t m_max_append_size = 0;
  uint64_t m_tag_class = 0;
  uint64_t m_tag_tid = 0;
  journal::ImageClientMeta m_client_meta;
  journal::TagData m_tag_data;

  int m_error_result;
  Contexts m_wait_for_state_contexts;

  ReplayHandler m_replay_handler;
  bool m_close_pending;

  Mutex m_event_lock;
  uint64_t m_event_tid;
  Events m_events;

  atomic_t m_op_tid;
  TidToFutures m_op_futures;

  bool m_processing_entry = false;
  bool m_blocking_writes;

  journal::Replay<ImageCtxT> *m_journal_replay;

  util::AsyncOpTracker m_async_journal_op_tracker;

  struct MetadataListener : public ::journal::JournalMetadataListener {
    Journal<ImageCtxT> *journal;

    MetadataListener(Journal<ImageCtxT> *journal) : journal(journal) { }

    void handle_update(::journal::JournalMetadata *) {
      FunctionContext *ctx = new FunctionContext([this](int r) {
        journal->handle_metadata_updated();
      });
      journal->m_work_queue->queue(ctx, 0);
    }
  } m_metadata_listener;

  typedef std::set<journal::Listener *> Listeners;
  Listeners m_listeners;
  Cond m_listener_cond;
  bool m_listener_notify = false;

  uint64_t m_refresh_sequence = 0;

  bool is_journal_replaying(const Mutex &) const;
  bool is_tag_owner(const Mutex &) const;

  uint64_t append_io_events(journal::EventType event_type,
                            const Bufferlists &bufferlists,
                            const AioObjectRequests &requests,
                            uint64_t offset, size_t length, bool flush_entry);
  Future wait_event(Mutex &lock, uint64_t tid, Context *on_safe);

  void create_journaler();
  void destroy_journaler(int r);
  void recreate_journaler(int r);

  void complete_event(typename Events::iterator it, int r);

  void start_append();

  void handle_open(int r);

  void handle_replay_ready();
  void handle_replay_complete(int r);
  void handle_replay_process_ready(int r);
  void handle_replay_process_safe(ReplayEntry replay_entry, int r);

  void handle_start_external_replay(int r,
                                    journal::Replay<ImageCtxT> **journal_replay,
                                    Context *on_finish);

  void handle_flushing_restart(int r);
  void handle_flushing_replay();

  void handle_recording_stopped(int r);

  void handle_journal_destroyed(int r);

  void handle_io_event_safe(int r, uint64_t tid);
  void handle_op_event_safe(int r, uint64_t tid, const Future &op_start_future,
                            const Future &op_finish_future, Context *on_safe);

  void stop_recording();

  void transition_state(State state, int r);

  bool is_steady_state() const;
  void wait_for_steady_state(Context *on_state);

  int check_resync_requested(bool *do_resync);

  void handle_metadata_updated();
  void handle_refresh_metadata(uint64_t refresh_sequence, uint64_t tag_tid,
                               journal::TagData tag_data, int r);

};

} // namespace librbd

extern template class librbd::Journal<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_H
