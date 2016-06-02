// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Replay.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::Replay: " << this << " "

namespace librbd {
namespace journal {

namespace {

static const uint64_t IN_FLIGHT_IO_LOW_WATER_MARK(32);
static const uint64_t IN_FLIGHT_IO_HIGH_WATER_MARK(64);

static NoOpProgressContext no_op_progress_callback;

// created by
// librbd::journal::Replay<I>::handle_event
// C_ExecuteOp should be a better name
template <typename I, typename E>
struct ExecuteOp : public Context {
  I &image_ctx;
  E event;
  Context *on_op_complete;

  ExecuteOp(I &image_ctx, const E &event, Context *on_op_complete)
    : image_ctx(image_ctx), event(event), on_op_complete(on_op_complete) {
  }

  // only SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent need to
  // handle specially during replaying, see
  // librbd::operation::Request<I>::append_op_event(T *request)
  // then the control will return to Replay<I>::replay_op_ready, then
  // to SnapshotCreateRequest<I>::handle_append_op_event, and finally to
  // on_op_complete
  void execute(const journal::SnapCreateEvent &_) {
    image_ctx.operations->execute_snap_create(event.snap_namespace,
					      event.snap_name,
                                              on_op_complete,
                                              event.op_tid, false);
  }

  void execute(const journal::SnapRemoveEvent &_) {
    image_ctx.operations->execute_snap_remove(event.snap_namespace,
					      event.snap_name,
                                              on_op_complete);
  }

  void execute(const journal::SnapRenameEvent &_) {
    image_ctx.operations->execute_snap_rename(event.snap_id,
                                              event.dst_snap_name,
                                              on_op_complete);
  }

  void execute(const journal::SnapProtectEvent &_) {
    image_ctx.operations->execute_snap_protect(event.snap_namespace,
					       event.snap_name,
                                               on_op_complete);
  }

  void execute(const journal::SnapUnprotectEvent &_) {
    image_ctx.operations->execute_snap_unprotect(event.snap_namespace,
						 event.snap_name,
                                                 on_op_complete);
  }

  void execute(const journal::SnapRollbackEvent &_) {
    image_ctx.operations->execute_snap_rollback(event.snap_namespace,
						event.snap_name,
                                                no_op_progress_callback,
                                                on_op_complete);
  }

  void execute(const journal::RenameEvent &_) {
    image_ctx.operations->execute_rename(event.image_name,
                                         on_op_complete);
  }

  // only SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent need to
  // handle specially during replaying, see
  // librbd::operation::Request<I>::append_op_event(T *request)
  // then the control will return to Replay<I>::replay_op_ready, then
  // to ResizeRequest<I>::handle_append_op_event, and finally to
  // on_op_complete
  void execute(const journal::ResizeEvent &_) {
    image_ctx.operations->execute_resize(event.size, true, no_op_progress_callback,
                                         on_op_complete, event.op_tid);
  }

  void execute(const journal::FlattenEvent &_) {
    image_ctx.operations->execute_flatten(no_op_progress_callback,
                                          on_op_complete);
  }

  void execute(const journal::SnapLimitEvent &_) {
    image_ctx.operations->execute_snap_set_limit(event.limit, on_op_complete);
  }

  // only SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent need to
  // handle specially during replaying, see
  // librbd::operation::Request<I>::append_op_event(T *request)
  // then the control will return to Replay<I>::replay_op_ready, then
  // to DisableFeaturesRequest/EnableFeaturesRequest<I>::handle_append_op_event, and finally to
  // on_op_complete
  void execute(const journal::UpdateFeaturesEvent &_) {
    image_ctx.operations->execute_update_features(event.features, event.enabled,
						  on_op_complete, event.op_tid);
  }

  void execute(const journal::MetadataSetEvent &_) {
    image_ctx.operations->execute_metadata_set(event.key, event.value,
                                               on_op_complete);
  }

  void execute(const journal::MetadataRemoveEvent &_) {
    image_ctx.operations->execute_metadata_remove(event.key, on_op_complete);
  }

  void finish(int r) override {
    CephContext *cct = image_ctx.cct;

    if (r < 0) {
      lderr(cct) << ": ExecuteOp::" << __func__ << ": r=" << r << dendl;
      on_op_complete->complete(r);
      return;
    }

    ldout(cct, 20) << ": ExecuteOp::" << __func__ << dendl;

    RWLock::RLocker owner_locker(image_ctx.owner_lock);

    if (image_ctx.exclusive_lock == nullptr ||
        !image_ctx.exclusive_lock->accept_ops()) {
      ldout(cct, 5) << ": lost exclusive lock -- skipping op" << dendl;
      on_op_complete->complete(-ECANCELED);
      return;
    }

    execute(event);
  }
};

template <typename I>
struct C_RefreshIfRequired : public Context {
  I &image_ctx;
  Context *on_finish;

  C_RefreshIfRequired(I &image_ctx, Context *on_finish)
    : image_ctx(image_ctx), on_finish(on_finish) {
  }
  ~C_RefreshIfRequired() override {
    delete on_finish;
  }

  void finish(int r) override {
    CephContext *cct = image_ctx.cct;
    Context *ctx = on_finish;
    on_finish = nullptr;

    if (r < 0) {
      lderr(cct) << ": C_RefreshIfRequired::" << __func__ << ": r=" << r << dendl;
      image_ctx.op_work_queue->queue(ctx, r);
      return;
    }

    if (image_ctx.state->is_refresh_required()) {
      ldout(cct, 20) << ": C_RefreshIfRequired::" << __func__ << ": "
                     << "refresh required" << dendl;
      image_ctx.state->refresh(ctx);
      return;
    }

    image_ctx.op_work_queue->queue(ctx, 0);
  }
};

} // anonymous namespace

#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::Replay: " << this << " " \
                           << __func__

// created by
// Journal<I>::handle_start_external_replay, which started by ImageReplayer<I>::start_replay
// Journal<I>::handle_open
template <typename I>
Replay<I>::Replay(I &image_ctx)
  : m_image_ctx(image_ctx), m_lock("Replay<I>::m_lock") {
}

template <typename I>
Replay<I>::~Replay() {
  ceph_assert(m_in_flight_aio_flush == 0);
  ceph_assert(m_in_flight_aio_modify == 0);
  ceph_assert(m_aio_modify_unsafe_contexts.empty());
  ceph_assert(m_aio_modify_safe_contexts.empty());
  ceph_assert(m_op_events.empty());
  ceph_assert(m_in_flight_op_events == 0);
}

template <typename I>
int Replay<I>::decode(bufferlist::const_iterator *it, EventEntry *event_entry) {
  try {
    using ceph::decode;
    decode(*event_entry, *it);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

// called by
// Journal<I>::handle_replay_ready, with:
//      on_ready -> Journal<I>::handle_replay_process_ready
//      on_safe -> Journal<I>::handle_replay_process_safe, i.e., m_journaler->committed
// ImageReplayer<I>::process_entry, with:
//      on_ready -> ImageReplayer<I>::handle_process_entry_ready
//      on_safe -> ImageReplayer<I>::handle_process_entry_safe, i.e., m_remote_journaler->committed
template <typename I>
void Replay<I>::process(const EventEntry &event_entry,
                        Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": on_ready=" << on_ready << ", on_safe=" << on_safe
                 << dendl;

  // on_ready == ImageReplayer<I>::handle_process_entry_ready
  // on_safe == C_ReplayCommitted, i.e., ImageReplayer<I>::handle_process_entry_safe

  on_ready = util::create_async_context_callback(m_image_ctx, on_ready);

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock == nullptr ||
      !m_image_ctx.exclusive_lock->accept_ops()) {
    ldout(cct, 5) << ": lost exclusive lock -- skipping event" << dendl;
    m_image_ctx.op_work_queue->queue(on_safe, -ECANCELED);
    on_ready->complete(0);
    return;
  }

  boost::apply_visitor(EventVisitor(this, on_ready, on_safe),
                       event_entry.event);
}

// called by
// Journal<I>::handle_replay_complete
// Journal<I>::handle_replay_process_safe, upon failure
// ImageReplayer<I>::shut_down
// ImageReplayer<I>::replay_flush
template <typename I>
void Replay<I>::shut_down(bool cancel_ops, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  io::AioCompletion *flush_comp = nullptr;
  on_finish = util::create_async_context_callback(
    m_image_ctx, on_finish);

  {
    Mutex::Locker locker(m_lock);

    // safely commit any remaining AIO modify operations
    if ((m_in_flight_aio_flush + m_in_flight_aio_modify) != 0) {

      // has inflight aio events

      flush_comp = create_aio_flush_completion(nullptr);
      ceph_assert(flush_comp != nullptr);
    }

    // std::unordered_map<uint64_t, OpEvent>
    for (auto &op_event_pair : m_op_events) {

      // iterate unordered_map<uint64_t, OpEvent>

      OpEvent &op_event = op_event_pair.second;

      if (cancel_ops) {
        // cancel ops that are waiting to start (waiting for
        // OpFinishEvent or waiting for ready)
        if (op_event.on_start_ready == nullptr &&
            op_event.on_op_finish_event != nullptr) {

          // for SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent, we are waiting for
          // the OpFinishEvent, see Replay<I>::replay_op_ready
          // for other events, we are also waiting for the OpFinishEvent, see
          // Replay<I>::handle_event

          Context *on_op_finish_event = nullptr;

          std::swap(on_op_finish_event, op_event.on_op_finish_event);

          m_image_ctx.op_work_queue->queue(on_op_finish_event, -ERESTART);
        }
      } else if (op_event.on_op_finish_event != nullptr) {

        // was set by Replay<I>::handle_event for other events except
        // SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent
        // or by Replay<I>::replay_op_ready for
        // SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent

        // start ops waiting for OpFinishEvent
        Context *on_op_finish_event = nullptr;

        std::swap(on_op_finish_event, op_event.on_op_finish_event);

        m_image_ctx.op_work_queue->queue(on_op_finish_event, 0);
      } else if (op_event.on_start_ready != nullptr) {

        // was set by Replay<I>::handle_event, for
        // SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent only

        // waiting for op ready, see Replay<I>::replay_op_ready
        op_event_pair.second.finish_on_ready = true;
      }
    }

    ceph_assert(!m_shut_down);
    m_shut_down = true;

    ceph_assert(m_flush_ctx == nullptr);
    if (m_in_flight_op_events > 0 || flush_comp != nullptr) {

      // has inflight op events or inflight aio events

      // the callback will be called by:
      // Replay<I>::handle_aio_flush_complete
      // Replay<I>::handle_op_complete
      std::swap(m_flush_ctx, on_finish);
    }
  }

  // execute the following outside of lock scope
  if (flush_comp != nullptr) {

    // has inflight aio events

    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    io::ImageRequest<I>::aio_flush(&m_image_ctx, flush_comp,
                                   io::FLUSH_SOURCE_INTERNAL, {});
  }

  if (on_finish != nullptr) {

    // no inflight op events or inflight aio events, finish the callback

    on_finish->complete(0);
  }
}

template <typename I>
void Replay<I>::flush(Context *on_finish) {
  io::AioCompletion *aio_comp;
  {
    Mutex::Locker locker(m_lock);

    aio_comp = create_aio_flush_completion(
      util::create_async_context_callback(m_image_ctx, on_finish));
    if (aio_comp == nullptr) {
      return;
    }
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  io::ImageRequest<I>::aio_flush(&m_image_ctx, aio_comp,
                                 io::FLUSH_SOURCE_INTERNAL, {});
}

// called by
// Journal<I>::replay_op_ready, which is only used for
// SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent, when this method
// is called, the aio write has been blocked, so we can try to pop
// the next replay entry
// on_resume -> XxxRequest<I>::handle_append_op_event
template <typename I>
void Replay<I>::replay_op_ready(uint64_t op_tid, Context *on_resume) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": op_tid=" << op_tid << dendl;

  Mutex::Locker locker(m_lock);

  // m_op_events was inserted by Replay<I>::create_op_context_callback

  auto op_it = m_op_events.find(op_tid);
  ceph_assert(op_it != m_op_events.end());

  OpEvent &op_event = op_it->second;
  ceph_assert(op_event.op_in_progress &&
         op_event.on_op_finish_event == nullptr &&
         op_event.on_finish_ready == nullptr &&
         op_event.on_finish_safe == nullptr);

  // op_event.on_start_ready and op_event.op_in_progress were only set for:
  // SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent

  // op_event.on_start_ready is either ImageReplayer<I>::handle_process_entry_ready or
  // Journal<I>::handle_replay_process_ready to fetch the next Op event

  // resume processing replay events
  Context *on_start_ready = nullptr;
  std::swap(on_start_ready, op_event.on_start_ready);

  // ok, io blocked, can try to pop the next replay entry, for events
  // except SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent we have
  // called this callback directly in their own Replay<I>::handle_event
  on_start_ready->complete(0);

  // cancel has been requested -- send error to paused state machine
  if (!op_event.finish_on_ready && m_flush_ctx != nullptr) {

    // m_flush_ctx was set by Replay<I>::shut_down

    m_image_ctx.op_work_queue->queue(on_resume, -ERESTART);

    return;
  }

  // resume the op state machine once the associated OpFinishEvent
  // is processed
  op_event.on_op_finish_event = new FunctionContext(
    [on_resume](int r) {
      on_resume->complete(r);
    });

  // ok, next we need the OpFinishEvent to drive us proceed, like the other events
  // just did in their Replay<I>::handle_event

  // shut down request -- don't expect OpFinishEvent
  if (op_event.finish_on_ready) {

    // was set by Replay<I>::shut_down for SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent

    m_image_ctx.op_work_queue->queue(on_resume, 0);
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioDiscardEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": AIO discard event" << dendl;

  bool flush_required;
  auto aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                               io::AIO_TYPE_DISCARD,
                                               &flush_required,
                                               {});
  if (aio_comp == nullptr) {
    return;
  }

  if (!clipped_io(event.offset, aio_comp)) {
    io::ImageRequest<I>::aio_discard(&m_image_ctx, aio_comp,
                                     {{event.offset, event.length}},
                                     event.skip_partial_discard, {});
  }

  if (flush_required) {
    m_lock.Lock();
    auto flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    if (flush_comp != nullptr) {
      io::ImageRequest<I>::aio_flush(&m_image_ctx, flush_comp,
                                     io::FLUSH_SOURCE_INTERNAL, {});
    }
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioWriteEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": AIO write event" << dendl;

  bufferlist data = event.data;

  bool flush_required;
  auto aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                               io::AIO_TYPE_WRITE,
                                               &flush_required,
                                               {});
  if (aio_comp == nullptr) {
    return;
  }

  if (!clipped_io(event.offset, aio_comp)) {
    io::ImageRequest<I>::aio_write(&m_image_ctx, aio_comp,
                                   {{event.offset, event.length}},
                                   std::move(data), 0, {});
  }

  if (flush_required) {

    // the inflight aio modification has hit the low water mark, we
    // need to flush it manually

    m_lock.Lock();
    auto flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    if (flush_comp != nullptr) {
      io::ImageRequest<I>::aio_flush(&m_image_ctx, flush_comp,
                                     io::FLUSH_SOURCE_INTERNAL, {});
    }
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::AioFlushEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": AIO flush event" << dendl;

  io::AioCompletion *aio_comp;
  {
    Mutex::Locker locker(m_lock);

    // the flush event from journal entry and the inflight aio modification
    // water mark both can initiate aio_flush, here we flush it bc of
    // journal flush event entry

    aio_comp = create_aio_flush_completion(on_safe);
  }

  if (aio_comp != nullptr) {
    io::ImageRequest<I>::aio_flush(&m_image_ctx, aio_comp,
                                   io::FLUSH_SOURCE_INTERNAL, {});
  }
  // ready for the next journal entry
  on_ready->complete(0);
}

// OpFinishEvent was appended by Journal<I>::commit_op_event
template <typename I>
void Replay<I>::handle_event(const journal::AioWriteSameEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": AIO writesame event" << dendl;

  bufferlist data = event.data;
  bool flush_required;
  auto aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                               io::AIO_TYPE_WRITESAME,
                                               &flush_required,
                                               {});
  if (aio_comp == nullptr) {
    return;
  }

  if (!clipped_io(event.offset, aio_comp)) {
    io::ImageRequest<I>::aio_writesame(&m_image_ctx, aio_comp,
                                       {{event.offset, event.length}},
                                       std::move(data), 0, {});
  }

  if (flush_required) {
    m_lock.Lock();
    auto flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    if (flush_comp != nullptr) {
      io::ImageRequest<I>::aio_flush(&m_image_ctx, flush_comp,
                                     io::FLUSH_SOURCE_INTERNAL, {});
    }
  }
}

 template <typename I>
 void Replay<I>::handle_event(const journal::AioCompareAndWriteEvent &event,
                              Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": AIO CompareAndWrite event" << dendl;

  bufferlist cmp_data = event.cmp_data;
  bufferlist write_data = event.write_data;
  bool flush_required;
  auto aio_comp = create_aio_modify_completion(on_ready, on_safe,
                                               io::AIO_TYPE_COMPARE_AND_WRITE,
                                               &flush_required,
                                               {-EILSEQ});

  if (!clipped_io(event.offset, aio_comp)) {
    io::ImageRequest<I>::aio_compare_and_write(&m_image_ctx, aio_comp,
                                               {{event.offset, event.length}},
                                               std::move(cmp_data),
                                               std::move(write_data),
                                               nullptr, 0, {});
  }

  if (flush_required) {
    m_lock.Lock();
    auto flush_comp = create_aio_flush_completion(nullptr);
    m_lock.Unlock();

    io::ImageRequest<I>::aio_flush(&m_image_ctx, flush_comp,
                                   io::FLUSH_SOURCE_INTERNAL, {});
  }
}

template <typename I>
void Replay<I>::handle_event(const journal::OpFinishEvent &event,
                             Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Op finish event: "
                 << "op_tid=" << event.op_tid << dendl;

  bool op_in_progress;
  bool filter_ret_val;
  Context *on_op_complete = nullptr;
  Context *on_op_finish_event = nullptr;

  {
    Mutex::Locker locker(m_lock);

    auto op_it = m_op_events.find(event.op_tid);
    if (op_it == m_op_events.end()) {
      ldout(cct, 10) << ": unable to locate associated op: assuming previously "
                     << "committed." << dendl;
      on_ready->complete(0);
      m_image_ctx.op_work_queue->queue(on_safe, 0);
      return;
    }

    OpEvent &op_event = op_it->second;
    ceph_assert(op_event.on_finish_safe == nullptr);
    op_event.on_finish_ready = on_ready;
    op_event.on_finish_safe = on_safe;

    // op_event.on_start_ready and op_event.op_in_progress were only set for:
    // SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent
    op_in_progress = op_event.op_in_progress;

    // op_event was set by Replay<I>::create_op_context_callback
    std::swap(on_op_complete, op_event.on_op_complete);

    // op_event.on_op_finish_event was set by Replay<I>::replay_op_ready to
    // resume the Op state machine for SnapCreateEvent, ResizeEvent, UpdateFeaturesEvent,
    // the Op state machine has blocked the IO, the journaled Op will only be
    // executed until the OpFinishEvent received

    // for other Op, the op_event.on_op_finish_event was set to ExecuteOp, i.e.,
    // the journaled Op will only be executed until the OpFinishEvent received
    std::swap(on_op_finish_event, op_event.on_op_finish_event);

    // special errors which indicate op never started but was recorded
    // as failed in the journal
    filter_ret_val = (op_event.op_finish_error_codes.count(event.r) != 0);
  }

  if (event.r < 0) {

    // this op has failed, do not try to resume or apply op, now the
    // op_event.on_op_finish_event should be on_resume or C_RefreshIfRequired,

    if (op_in_progress) {

      // Op of type SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent

      // op_event.on_op_finish_event should be on_resume, i.e., XxxRequest<I>::handle_append_op_event,
      // we need to pass the error to it

      // bubble the error up to the in-progress op to cancel it
      on_op_finish_event->complete(event.r);
    } else {

      // other Op except SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent

      // op_event.on_op_finish_event should be C_RefreshIfRequired, now we should
      // skip the mediate callbacks, i.e., C_RefreshIfRequired and ExecuteOp and
      // complete the final callback, i.e., on_op_complete, i.e., C_OpOnComplete, i.e.,
      // handle_op_complete, directly

      // op hasn't been started -- bubble the error up since
      // our image is now potentially in an inconsistent state
      // since simple errors should have been caught before
      // creating the op event
      delete on_op_complete;
      delete on_op_finish_event;

      handle_op_complete(event.op_tid, filter_ret_val ? 0 : event.r);
    }

    return;
  }

  // for SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent, the state machine
  // has been stopped to block IO, now resume it
  // for other events we start the state machine, i.e., do ExecuteOp

  // journal recorded success -- apply the op now
  on_op_finish_event->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapCreateEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap create event" << dendl;

  Mutex::Locker locker(m_lock);

  // 1. create op_event
  // 2. op_event->on_start_safe = on_safe
  // 3. op_event->on_op_complete = new C_OpOnComplete, i.e., Replay::handle_op_complete
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    // duplicated op event
    return;
  }

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  // call ExecuteOp directly, op_event->on_op_finish_event will be set
  // later by Replay<I>::replay_op_ready
  // avoid lock cycles
  m_image_ctx.op_work_queue->queue(new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapCreateEvent>(m_image_ctx, event,
                                                            on_op_complete)),
    0);

  // do not process more events until the state machine is ready
  // since it will affect IO
  op_event->op_in_progress = true;
  // other events will call on_ready->complete(0) directly, but for
  // SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent we will defer it
  // to Replay<I>::replay_op_ready, which will be initiated by
  // C_RefreshIfRequired::finish -> ExecuteOp::finish -> ExecuteOp::execute(SnapCreateEvent)
  op_event->on_start_ready = on_ready;
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRemoveEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap remove event" << dendl;

  Mutex::Locker locker(m_lock);

  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  // now, we are waiting for the corresponding OpFinishEvent to drive
  // us to proceed, i.e., call ExecuteOp
  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRemoveEvent>(m_image_ctx, event,
                                                            on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-ENOENT};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap rename event" << dendl;

  Mutex::Locker locker(m_lock);

  OpEvent *op_event;

  // op_event->on_start_safe = on_safe
  // op_event->on_op_complete = C_OpOnComplete, i.e., Replay<I>::handle_op_complete
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRenameEvent>(m_image_ctx, event,
                                                            on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  // ImageReplayer<I>::handle_process_entry_ready, try to pop the next relay entry
  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapProtectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap protect event" << dendl;

  Mutex::Locker locker(m_lock);

  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapProtectEvent>(m_image_ctx, event,
                                                             on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EBUSY};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapUnprotectEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap unprotect event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapUnprotectEvent>(m_image_ctx,
                                                               event,
                                                               on_op_complete));

  // ignore errors recorded in the journal
  op_event->op_finish_error_codes = {-EBUSY};

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EINVAL};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapRollbackEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap rollback start event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapRollbackEvent>(m_image_ctx,
                                                              event,
                                                              on_op_complete));

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::RenameEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Rename event" << dendl;

  Mutex::Locker locker(m_lock);

  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::RenameEvent>(m_image_ctx, event,
                                                        on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EEXIST};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::ResizeEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Resize start event" << dendl;

  Mutex::Locker locker(m_lock);

  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  // avoid lock cycles
  m_image_ctx.op_work_queue->queue(new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::ResizeEvent>(m_image_ctx, event,
                                                        on_op_complete)), 0);

  // do not process more events until the state machine is ready
  // since it will affect IO
  op_event->op_in_progress = true;
  op_event->on_start_ready = on_ready;
}

template <typename I>
void Replay<I>::handle_event(const journal::FlattenEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Flatten start event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::FlattenEvent>(m_image_ctx, event,
                                                         on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-EINVAL};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::DemotePromoteEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Demote/Promote event" << dendl;
  on_ready->complete(0);
  on_safe->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::SnapLimitEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Snap limit event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  op_event->on_op_finish_event = new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::SnapLimitEvent>(m_image_ctx,
							   event,
							   on_op_complete));

  op_event->ignore_error_codes = {-ERANGE};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::UpdateFeaturesEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Update features event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  // avoid lock cycles
  m_image_ctx.op_work_queue->queue(new C_RefreshIfRequired<I>(
    m_image_ctx, new ExecuteOp<I, journal::UpdateFeaturesEvent>(
      m_image_ctx, event, on_op_complete)), 0);

  // do not process more events until the state machine is ready
  // since it will affect IO
  op_event->op_in_progress = true;
  op_event->on_start_ready = on_ready;
}

template <typename I>
void Replay<I>::handle_event(const journal::MetadataSetEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Metadata set event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  on_op_complete = new C_RefreshIfRequired<I>(m_image_ctx, on_op_complete);
  op_event->on_op_finish_event = util::create_async_context_callback(
    m_image_ctx, new ExecuteOp<I, journal::MetadataSetEvent>(
      m_image_ctx, event, on_op_complete));

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::MetadataRemoveEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": Metadata remove event" << dendl;

  Mutex::Locker locker(m_lock);
  OpEvent *op_event;
  Context *on_op_complete = create_op_context_callback(event.op_tid, on_ready,
                                                       on_safe, &op_event);
  if (on_op_complete == nullptr) {
    return;
  }

  on_op_complete = new C_RefreshIfRequired<I>(m_image_ctx, on_op_complete);
  op_event->on_op_finish_event = util::create_async_context_callback(
    m_image_ctx, new ExecuteOp<I, journal::MetadataRemoveEvent>(
      m_image_ctx, event, on_op_complete));

  // ignore errors caused due to replay
  op_event->ignore_error_codes = {-ENOENT};

  on_ready->complete(0);
}

template <typename I>
void Replay<I>::handle_event(const journal::UnknownEvent &event,
			     Context *on_ready, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": unknown event" << dendl;
  on_ready->complete(0);
  on_safe->complete(0);
}

// called by librbd::journal::Replay<I>::C_AioModifyComplete::finish
template <typename I>
void Replay<I>::handle_aio_modify_complete(Context *on_ready, Context *on_safe,
                                           int r, std::set<int> &filters,
                                           bool writeback_cache_enabled) {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": on_ready=" << on_ready << ", "
                 << "on_safe=" << on_safe << ", r=" << r << dendl;

  if (on_ready != nullptr) {

    // if we are not blocked by the inflight aio high water mark, we
    // can proceed to process the next journal entry

    on_ready->complete(0);
  }

  if (filters.find(r) != filters.end())
    r = 0;
  // we only complete the on_safe callback in rados callback if it fails,
  // else we complete a batch of on_safe callbacks in callback of the
  // folloing flush op, see Replay<I>::create_aio_flush_completion

  if (r < 0) {
    lderr(cct) << ": AIO modify op failed: " << cpp_strerror(r) << dendl;
    m_image_ctx.op_work_queue->queue(on_safe, r);
    return;
  }

  if (writeback_cache_enabled) {
    // will be completed after next flush operation completes
    m_aio_modify_safe_contexts.insert(on_safe);
  } else {
    // IO is safely stored on disk
    ceph_assert(m_in_flight_aio_modify > 0);
    --m_in_flight_aio_modify;

    if (m_on_aio_ready != nullptr) {
      ldout(cct, 10) << ": resuming paused AIO" << dendl;
      m_on_aio_ready->complete(0);
      m_on_aio_ready = nullptr;
    }

    ldout(cct, 20) << ": completing safe context: " << on_safe << dendl;
    m_image_ctx.op_work_queue->queue(on_safe, 0);
  }
}

// called by librbd::journal::Replay<I>::C_AioFlushComplete::finish
template <typename I>
void Replay<I>::handle_aio_flush_complete(Context *on_flush_safe,
                                          Contexts &on_safe_ctxs, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": AIO flush failed: " << cpp_strerror(r) << dendl;
  }

  Context *on_aio_ready = nullptr;
  Context *on_flush = nullptr;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_in_flight_aio_flush > 0);
    ceph_assert(m_in_flight_aio_modify >= on_safe_ctxs.size());
    --m_in_flight_aio_flush;

    // was increased by Replay<I>::create_aio_modify_completion
    m_in_flight_aio_modify -= on_safe_ctxs.size();

    // m_on_aio_ready is set in Replay<I>::create_aio_modify_completion
    // when the inflight aio modification has hit the high water mark
    std::swap(on_aio_ready, m_on_aio_ready);

    if (m_in_flight_op_events == 0 &&
        (m_in_flight_aio_flush + m_in_flight_aio_modify) == 0) {
      on_flush = m_flush_ctx;
    }

    // strip out previously failed on_safe contexts
    for (auto it = on_safe_ctxs.begin(); it != on_safe_ctxs.end(); ) {

      // iterate each on_safe callbacks between previous flush op
      // and this one

      if (m_aio_modify_safe_contexts.erase(*it)) {

        // the aio modification associated with this on_safe callback
        // committed to disk succeeded

        ++it;
      } else {

        // committed to disk failed, remove it
        it = on_safe_ctxs.erase(it);
      }
    }
  }

  if (on_aio_ready != nullptr) {

    // inflight aio modification high water mark relieved, fetch the
    // next journal entry to process

    ldout(cct, 10) << ": resuming paused AIO" << dendl;

    on_aio_ready->complete(0);
  }

  if (on_flush_safe != nullptr) {

    // for inflight aio water mark resulted flush op this callback
    // will be nullptr, for flush op from journal entry this is non-nullptr

    on_safe_ctxs.push_back(on_flush_safe);
  }

  for (auto ctx : on_safe_ctxs) {

    // iterate those on_safe callbacks committed to disk succeeded and
    // may plus the on_safe callback of the flush op from the journal
    // entry

    ldout(cct, 20) << ": completing safe context: " << ctx << dendl;

    ctx->complete(r);
  }

  if (on_flush != nullptr) {
    ldout(cct, 20) << ": completing flush context: " << on_flush << dendl;

    on_flush->complete(r);
  }
}

// called by
// Replay<I>::handle_event
template <typename I>
Context *Replay<I>::create_op_context_callback(uint64_t op_tid,
                                               Context *on_ready,
                                               Context *on_safe,
                                               OpEvent **op_event) {
  CephContext *cct = m_image_ctx.cct;
  if (m_shut_down) {
    ldout(cct, 5) << ": ignoring event after shut down" << dendl;
    on_ready->complete(0);
    m_image_ctx.op_work_queue->queue(on_safe, -ESHUTDOWN);
    return nullptr;
  }

  ceph_assert(m_lock.is_locked());
  if (m_op_events.count(op_tid) != 0) {
    lderr(cct) << ": duplicate op tid detected: " << op_tid << dendl;

    // on_ready is already async but on failure invoke on_safe async
    // as well
    on_ready->complete(0);
    m_image_ctx.op_work_queue->queue(on_safe, -EINVAL);
    return nullptr;
  }

  // will be decreased by Replay<I>::handle_op_complete
  ++m_in_flight_op_events;

  *op_event = &m_op_events[op_tid];

  (*op_event)->on_start_safe = on_safe;

  // replay->handle_op_complete
  Context *on_op_complete = new C_OpOnComplete(this, op_tid);

  (*op_event)->on_op_complete = on_op_complete;

  return on_op_complete;
}

// called by
// librbd::journal::Replay<I>::C_OpOnComplete::finish
// Replay<I>::handle_event(OpFinishEvent), upon Op event failure for events
// other than SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent
template <typename I>
void Replay<I>::handle_op_complete(uint64_t op_tid, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": op_tid=" << op_tid << ", "
                 << "r=" << r << dendl;

  OpEvent op_event;
  bool shutting_down = false;

  {
    Mutex::Locker locker(m_lock);

    // std::unordered_map<uint64_t, OpEvent>
    auto op_it = m_op_events.find(op_tid);
    ceph_assert(op_it != m_op_events.end());

    op_event = std::move(op_it->second);
    m_op_events.erase(op_it);

    if (m_shut_down) {
      ceph_assert(m_flush_ctx != nullptr);
      shutting_down = true;
    }
  }

  ceph_assert(op_event.on_start_ready == nullptr || (r < 0 && r != -ERESTART));
  if (op_event.on_start_ready != nullptr) {
    // failure for snapcreate/resize/updatefeatures

    // Replay<I>::handle_op_complete was called by C_RefreshIfRequired::finish -> ExecuteOp::finish
    // with r < 0

    // blocking op event failed before it became ready
    ceph_assert(op_event.on_finish_ready == nullptr &&
           op_event.on_finish_safe == nullptr);

    // try to pop the next replay entry
    op_event.on_start_ready->complete(0);
  } else {

    // op_event.on_finish_ready and op_event.on_finish_safe were set by
    // Replay<I>::handle_event(OpFinishEvent)

    // event kicked off by OpFinishEvent
    ceph_assert((op_event.on_finish_ready != nullptr &&
            op_event.on_finish_safe != nullptr) || shutting_down);
  }

  if (op_event.on_op_finish_event != nullptr) {

    // not called directly, i.e., the op event has not failed, see
    // Replay<I>::handle_event(OpFinishEvent)

    // for SnapCreateEvent/ResizeEvent/UpdateFeaturesEvent, on_resume->complete,
    // i.e., XxxRequest<I>::handle_append_op_event
    // for other events, C_RefreshIfRequired

    op_event.on_op_finish_event->complete(r);
  }

  // try to pop the next replay entry
  if (op_event.on_finish_ready != nullptr) {
    op_event.on_finish_ready->complete(0);
  }

  // filter out errors caused by replay of the same op
  if (r < 0 && op_event.ignore_error_codes.count(r) != 0) {
    r = 0;
  }

  // commit the op event
  op_event.on_start_safe->complete(r);

  // commit  OpFinishEvent
  if (op_event.on_finish_safe != nullptr) {
    op_event.on_finish_safe->complete(r);
  }

  // shut down request might have occurred while lock was
  // dropped -- handle if pending
  Context *on_flush = nullptr;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_in_flight_op_events > 0);
    --m_in_flight_op_events;

    if (m_in_flight_op_events == 0 &&
        (m_in_flight_aio_flush + m_in_flight_aio_modify) == 0) {
      // no in flight aio/op/flush, check if we can finish the shutdown now,
      // see Replay<I>::shut_down
      on_flush = m_flush_ctx;
    }
  }

  if (on_flush != nullptr) {
    m_image_ctx.op_work_queue->queue(on_flush, 0);
  }
}

template <typename I>
io::AioCompletion *
Replay<I>::create_aio_modify_completion(Context *on_ready,
                                        Context *on_safe,
                                        io::aio_type_t aio_type,
                                        bool *flush_required,
                                        std::set<int> &&filters) {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ceph_assert(m_on_aio_ready == nullptr);

  if (m_shut_down) {
    ldout(cct, 5) << ": ignoring event after shut down" << dendl;
    on_ready->complete(0);
    m_image_ctx.op_work_queue->queue(on_safe, -ESHUTDOWN);
    return nullptr;
  }

  // will be decreased by Replay<I>::handle_aio_flush_complete
  ++m_in_flight_aio_modify;

  bool writeback_cache_enabled = m_image_ctx.is_writeback_cache_enabled();
  if (writeback_cache_enabled) {
    m_aio_modify_unsafe_contexts.push_back(on_safe);
  }

  // FLUSH if we hit the low-water mark -- on_safe contexts are
  // completed by flushes-only so that we don't move the journal
  // commit position until safely on-disk

  *flush_required = (writeback_cache_enabled &&
                     m_aio_modify_unsafe_contexts.size() ==
                       IN_FLIGHT_IO_LOW_WATER_MARK);

  if (*flush_required) {

    // we will start a flush op manually right after the modification
    // of this journal entry

    ldout(cct, 10) << ": hit AIO replay low-water mark: scheduling flush"
                   << dendl;
  }

  // READY for more events if:
  // * not at high-water mark for IO
  // * in-flight ops are at a consistent point (snap create has IO flushed,
  //   shrink has adjusted clip boundary, etc) -- should have already been
  //   flagged not-ready
  if (m_in_flight_aio_modify == IN_FLIGHT_IO_HIGH_WATER_MARK) {

    // default 64

    ldout(cct, 10) << ": hit AIO replay high-water mark: pausing replay"
                   << dendl;
    ceph_assert(m_on_aio_ready == nullptr);
    std::swap(m_on_aio_ready, on_ready);
  }

  // when the modification is ACKed by librbd, we can process the next
  // event. when flushed, the completion of the next flush will fire the
  // on_safe callback
  // Replay<I>::handle_aio_modify_complete
  auto aio_comp = io::AioCompletion::create_and_start<Context>(
    new C_AioModifyComplete(this, on_ready, on_safe, std::move(filters),
                            writeback_cache_enabled),
    util::get_image_ctx(&m_image_ctx), aio_type);
  return aio_comp;
}

template <typename I>
io::AioCompletion *Replay<I>::create_aio_flush_completion(Context *on_safe) {
  ceph_assert(m_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  if (m_shut_down) {
    ldout(cct, 5) << ": ignoring event after shut down" << dendl;
    if (on_safe != nullptr) {
      m_image_ctx.op_work_queue->queue(on_safe, -ESHUTDOWN);
    }
    return nullptr;
  }

  // will be decreased by Replay<I>::handle_aio_flush_complete
  ++m_in_flight_aio_flush;

  // associate all prior write/discard ops to this flush request
  // all previous on_safe callbacks between the last flush op and this
  // one will be completed by this rados callback
  auto aio_comp = io::AioCompletion::create_and_start<Context>(
      new C_AioFlushComplete(this, on_safe,
                             std::move(m_aio_modify_unsafe_contexts)),
      util::get_image_ctx(&m_image_ctx), io::AIO_TYPE_FLUSH);
  m_aio_modify_unsafe_contexts.clear();

  return aio_comp;
}

template <typename I>
bool Replay<I>::clipped_io(uint64_t image_offset, io::AioCompletion *aio_comp) {
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.snap_lock.get_read();
  size_t image_size = m_image_ctx.size;
  m_image_ctx.snap_lock.put_read();

  if (image_offset >= image_size) {
    // rbd-mirror image sync might race an IO event w/ associated resize between
    // the point the peer is registered and the sync point is created, so no-op
    // IO events beyond the current image extents since under normal conditions
    // it wouldn't have been recorded in the journal
    ldout(cct, 5) << ": no-op IO event beyond image size" << dendl;
    aio_comp->get();
    aio_comp->unblock();
    aio_comp->put();
    return true;
  }

  return false;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::Replay<librbd::ImageCtx>;
