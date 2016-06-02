// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "cls/rbd/cls_rbd_types.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "librbd/object_watcher/Notifier.h"
#include "librbd/WatchNotifyTypes.h"
#include <set>
#include <string>
#include <utility>
#include <boost/variant.hpp>

class entity_name_t;

namespace librbd {

class ImageCtx;
template <typename T> class TaskFinisher;

template <typename ImageCtxT = ImageCtx>
class ImageWatcher {
public:
  ImageWatcher(ImageCtxT& image_ctx);
  ~ImageWatcher();

  void register_watch(Context *on_finish);
  void unregister_watch(Context *on_finish);
  void flush(Context *on_finish);

  void notify_flatten(uint64_t request_id, ProgressContext &prog_ctx,
                      Context *on_finish);
  void notify_resize(uint64_t request_id, uint64_t size, bool allow_shrink,
                     ProgressContext &prog_ctx, Context *on_finish);
  void notify_snap_create(const std::string &snap_name,
			  const cls::rbd::SnapshotNamespace &snap_namespace,
			  Context *on_finish);
  void notify_snap_rename(const snapid_t &src_snap_id,
                          const std::string &dst_snap_name,
                          Context *on_finish);
  void notify_snap_remove(const std::string &snap_name, Context *on_finish);
  void notify_snap_protect(const std::string &snap_name, Context *on_finish);
  void notify_snap_unprotect(const std::string &snap_name, Context *on_finish);
  void notify_rebuild_object_map(uint64_t request_id,
                                 ProgressContext &prog_ctx, Context *on_finish);
  void notify_rename(const std::string &image_name, Context *on_finish);

  void notify_update_features(uint64_t features, bool enabled,
                              Context *on_finish);

  void notify_acquired_lock();
  void notify_released_lock();
  void notify_request_lock();

  void notify_header_update(Context *on_finish);
  static void notify_header_update(librados::IoCtx &io_ctx,
                                   const std::string &oid);

  uint64_t get_watch_handle() const {
    RWLock::RLocker watch_locker(m_watch_lock);
    return m_watch_handle;
  }

private:
  enum WatchState {
    WATCH_STATE_UNREGISTERED,
    WATCH_STATE_REGISTERED,
    WATCH_STATE_ERROR,
    WATCH_STATE_REWATCHING
  };

  enum TaskCode {
    TASK_CODE_REQUEST_LOCK,
    TASK_CODE_CANCEL_ASYNC_REQUESTS,
    TASK_CODE_REREGISTER_WATCH,
    TASK_CODE_ASYNC_REQUEST,
    TASK_CODE_ASYNC_PROGRESS
  };

  typedef std::pair<Context *, ProgressContext *> AsyncRequest;

  class Task {
  public:
    Task(TaskCode task_code) : m_task_code(task_code) {}
    Task(TaskCode task_code, const watch_notify::AsyncRequestId &id)
      : m_task_code(task_code), m_async_request_id(id) {}

    inline bool operator<(const Task& rhs) const {
      if (m_task_code != rhs.m_task_code) {
        return m_task_code < rhs.m_task_code;
      } else if ((m_task_code == TASK_CODE_ASYNC_REQUEST ||
                  m_task_code == TASK_CODE_ASYNC_PROGRESS) &&
                 m_async_request_id != rhs.m_async_request_id) {
        return m_async_request_id < rhs.m_async_request_id;
      }
      return false;
    }
  private:
    TaskCode m_task_code;
    watch_notify::AsyncRequestId m_async_request_id;
  };

  // used by
  // ImageWatcher<I>::register_watch
  struct WatchCtx : public librados::WatchCtx2 {
    ImageWatcher &image_watcher;

    WatchCtx(ImageWatcher &parent) : image_watcher(parent) {}

    virtual void handle_notify(uint64_t notify_id,
                               uint64_t handle,
      			       uint64_t notifier_id,
                               bufferlist& bl);
    virtual void handle_error(uint64_t handle, int err);
  };

  class RemoteProgressContext : public ProgressContext {
  public:
    RemoteProgressContext(ImageWatcher &image_watcher,
                          const watch_notify::AsyncRequestId &id)
      : m_image_watcher(image_watcher), m_async_request_id(id)
    {
    }

    virtual int update_progress(uint64_t offset, uint64_t total) {
      m_image_watcher.schedule_async_progress(m_async_request_id, offset,
                                              total);
      return 0;
    }

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
  };

  // used by
  // ImageWatcher<I>::prepare_async_request
  class RemoteContext : public Context {
  public:
    RemoteContext(ImageWatcher &image_watcher,
      	          const watch_notify::AsyncRequestId &id,
      	          ProgressContext *prog_ctx)
      : m_image_watcher(image_watcher), m_async_request_id(id),
        m_prog_ctx(prog_ctx)
    {
    }

    virtual ~RemoteContext() {
      delete m_prog_ctx;
    }

    virtual void finish(int r);

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
    ProgressContext *m_prog_ctx;
  };

  // used by
  // ImageWatcher<I>::register_watch
  struct C_RegisterWatch : public Context {
    ImageWatcher *image_watcher;
    Context *on_finish;

    C_RegisterWatch(ImageWatcher *image_watcher, Context *on_finish)
       : image_watcher(image_watcher), on_finish(on_finish) {
    }
    virtual void finish(int r) override {
      image_watcher->handle_register_watch(r);
      on_finish->complete(r);
    }
  };

  struct C_NotifyAck : public Context {
    ImageWatcher *image_watcher;
    uint64_t notify_id;
    uint64_t handle;
    bufferlist out;

    C_NotifyAck(ImageWatcher *image_watcher, uint64_t notify_id,
                uint64_t handle);

    // image_watcher->acknowledge_notify
    virtual void finish(int r);
  };

  // used by
  // ImageWatcher<I>::handle_payload, not all, but for some payload type
  struct C_ResponseMessage : public Context {
    C_NotifyAck *notify_ack;

    C_ResponseMessage(C_NotifyAck *notify_ack) : notify_ack(notify_ack) {
    }

    // notify_ack->complete, i.e., image_watcher->acknowledge_notify
    virtual void finish(int r);
  };

  // used by
  // ImageWatcher<I>::handle_notify, to delay handling the payload of
  // the notify, i.e., the image has to be refreshed first
  struct C_ProcessPayload : public Context {
    ImageWatcher *image_watcher;
    uint64_t notify_id;
    uint64_t handle;
    watch_notify::Payload payload;

    C_ProcessPayload(ImageWatcher *image_watcher_, uint64_t notify_id_,
                     uint64_t handle_, const watch_notify::Payload &payload)
      : image_watcher(image_watcher_), notify_id(notify_id_), handle(handle_),
        payload(payload) {
    }

    virtual void finish(int r) override {
      image_watcher->process_payload(notify_id, handle, payload, r);
    }
  };

  // used by
  // ImageWatcher<I>::process_payload, to handle each type of payload
  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    ImageWatcher *image_watcher;
    uint64_t notify_id;
    uint64_t handle;

    HandlePayloadVisitor(ImageWatcher *image_watcher_, uint64_t notify_id_,
      		   uint64_t handle_)
      : image_watcher(image_watcher_), notify_id(notify_id_), handle(handle_)
    {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      // C_NotifyAck::finish will call image_watcher->acknowledge_notify
      C_NotifyAck *ctx = new C_NotifyAck(image_watcher, notify_id,
                                                handle);

      // if handle_payload returns false, then the notify will be
      // acked by ImageWatcher::handle_payload

      if (image_watcher->handle_payload(payload, ctx)) {

        // handle the payload finished, ack directly

        ctx->complete(0);
      }
    }
  };

  ImageCtxT &m_image_ctx;

  mutable RWLock m_watch_lock;
  WatchCtx m_watch_ctx;
  uint64_t m_watch_handle;
  WatchState m_watch_state;
  Context *m_unregister_watch_ctx = nullptr;

  TaskFinisher<Task> *m_task_finisher;

  RWLock m_async_request_lock;
  std::map<watch_notify::AsyncRequestId, AsyncRequest> m_async_requests;
  std::set<watch_notify::AsyncRequestId> m_async_pending;

  Mutex m_owner_client_id_lock;
  watch_notify::ClientId m_owner_client_id;

  // initialized by ImageWatcher<I>::ImageWatcher, will be used to notify
  // the image_ctx.header_oid object
  object_watcher::Notifier m_notifier;

  void handle_register_watch(int r);

  void schedule_cancel_async_requests();
  void cancel_async_requests();

  void set_owner_client_id(const watch_notify::ClientId &client_id);
  watch_notify::ClientId get_client_id();

  void handle_request_lock(int r);
  void schedule_request_lock(bool use_timer, int timer_delay = -1);

  void notify_lock_owner(bufferlist &&bl, Context *on_finish);

  Context *remove_async_request(const watch_notify::AsyncRequestId &id);
  void schedule_async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void notify_async_request(const watch_notify::AsyncRequestId &id,
                            bufferlist &&in, ProgressContext& prog_ctx,
                            Context *on_finish);

  void schedule_async_progress(const watch_notify::AsyncRequestId &id,
                               uint64_t offset, uint64_t total);
  int notify_async_progress(const watch_notify::AsyncRequestId &id,
                            uint64_t offset, uint64_t total);
  void schedule_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void notify_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void handle_async_complete(const watch_notify::AsyncRequestId &request, int r,
                             int ret_val);

  int prepare_async_request(const watch_notify::AsyncRequestId& id,
                            bool* new_request, Context** ctx,
                            ProgressContext** prog_ctx);

  bool handle_payload(const watch_notify::HeaderUpdatePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AcquiredLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ReleasedLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RequestLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncProgressPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncCompletePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::FlattenPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ResizePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapCreatePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRenamePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRemovePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapProtectPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapUnprotectPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RebuildObjectMapPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RenamePayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UpdateFeaturesPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UnknownPayload& payload,
                      C_NotifyAck *ctx);
  void process_payload(uint64_t notify_id, uint64_t handle,
                       const watch_notify::Payload &payload, int r);

  void handle_notify(uint64_t notify_id, uint64_t handle, bufferlist &bl);
  void handle_error(uint64_t cookie, int err);
  void acknowledge_notify(uint64_t notify_id, uint64_t handle, bufferlist &out);

  void rewatch();
  void handle_rewatch(int r);
};

} // namespace librbd

extern template class librbd::ImageWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
