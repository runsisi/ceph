// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/JournalMetadata.h"
#include "journal/Utils.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "cls/journal/cls_journal_client.h"
#include <functional>
#include <set>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "JournalMetadata: " << this << " "

namespace journal {

using namespace cls::journal;

namespace {

struct C_GetClient : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  AsyncOpTracker &async_op_tracker;
  std::string client_id;
  cls::journal::Client *client;
  Context *on_finish;

  bufferlist out_bl;

  C_GetClient(CephContext *cct, librados::IoCtx &ioctx, const std::string &oid,
              AsyncOpTracker &async_op_tracker, const std::string &client_id,
              cls::journal::Client *client, Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), async_op_tracker(async_op_tracker),
      client_id(client_id), client(client), on_finish(on_finish) {
    async_op_tracker.start_op();
  }
  virtual ~C_GetClient() {
    async_op_tracker.finish_op();
  }

  virtual void send() {
    send_get_client();
  }

  void send_get_client() {
    ldout(cct, 20) << "C_GetClient: " << __func__ << dendl;

    librados::ObjectReadOperation op;

    // get from omap "client_" + client id
    client::get_client_start(&op, client_id);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_GetClient, &C_GetClient::handle_get_client>);

    // access journal metadata object
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_client(int r) {
    ldout(cct, 20) << "C_GetClient: " << __func__ << ": r=" << r << dendl;

    if (r == 0) {
      bufferlist::iterator it = out_bl.begin();

      // decode and get the cls::journal::Client instance
      r = client::get_client_finish(&it, client);
    }

    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

struct C_AllocateTag : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;

  // journal metadata object id associated with each image
  const std::string &oid;

  AsyncOpTracker &async_op_tracker;
  uint64_t tag_class;
  Tag *tag;
  Context *on_finish;

  bufferlist out_bl;

  C_AllocateTag(CephContext *cct, librados::IoCtx &ioctx,
                const std::string &oid, AsyncOpTracker &async_op_tracker,
                uint64_t tag_class, const bufferlist &data, Tag *tag,
                Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), async_op_tracker(async_op_tracker),
      tag_class(tag_class), tag(tag), on_finish(on_finish) {
    async_op_tracker.start_op();
    tag->data = data;
  }

  virtual ~C_AllocateTag() {
    async_op_tracker.finish_op();
  }

  void send() {
    send_get_next_tag_tid();
  }

  void send_get_next_tag_tid() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectReadOperation op;
    client::get_next_tag_tid_start(&op);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_get_next_tag_tid>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_next_tag_tid(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == 0) {
      bufferlist::iterator iter = out_bl.begin();
      r = client::get_next_tag_tid_finish(&iter, &tag->tid);
    }

    if (r < 0) {
      complete(r);
      return;
    }

    send_tag_create();
  }

  void send_tag_create() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectWriteOperation op;

    client::tag_create(&op, tag->tid, tag_class, tag->data);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_tag_create>);

    int r = ioctx.aio_operate(oid, comp, &op);
    assert(r == 0);
    comp->release();
  }

  void handle_tag_create(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == -ESTALE) {
      send_get_next_tag_tid();
      return;
    } else if (r < 0) {
      complete(r);
      return;
    }

    send_get_tag();
  }

  void send_get_tag() {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << dendl;

    librados::ObjectReadOperation op;
    client::get_tag_start(&op, tag->tid);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AllocateTag, &C_AllocateTag::handle_get_tag>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_tag(int r) {
    ldout(cct, 20) << "C_AllocateTag: " << __func__ << ": r=" << r << dendl;

    if (r == 0) {
      bufferlist::iterator iter = out_bl.begin();

      cls::journal::Tag journal_tag;
      r = client::get_tag_finish(&iter, &journal_tag);
      if (r == 0) {
        *tag = journal_tag;
      }
    }
    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

struct C_GetTag : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  AsyncOpTracker &async_op_tracker;
  uint64_t tag_tid;
  JournalMetadata::Tag *tag;
  Context *on_finish;

  bufferlist out_bl;

  C_GetTag(CephContext *cct, librados::IoCtx &ioctx, const std::string &oid,
           AsyncOpTracker &async_op_tracker, uint64_t tag_tid,
           JournalMetadata::Tag *tag, Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), async_op_tracker(async_op_tracker),
      tag_tid(tag_tid), tag(tag), on_finish(on_finish) {
    async_op_tracker.start_op();
  }
  virtual ~C_GetTag() {
    async_op_tracker.finish_op();
  }

  void send() {
    send_get_tag();
  }

  void send_get_tag() {
    librados::ObjectReadOperation op;

    // get tag from omap of key "tag_" + tag_tid
    client::get_tag_start(&op, tag_tid);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_GetTag, &C_GetTag::handle_get_tag>);

    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_get_tag(int r) {
    if (r == 0) {
      bufferlist::iterator iter = out_bl.begin();
      r = client::get_tag_finish(&iter, tag);
    }

    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

struct C_GetTags : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  const std::string &client_id;
  AsyncOpTracker &async_op_tracker;
  uint64_t start_after_tag_tid;
  boost::optional<uint64_t> tag_class;
  JournalMetadata::Tags *tags;
  Context *on_finish;

  const uint64_t MAX_RETURN = 64;
  bufferlist out_bl;

  C_GetTags(CephContext *cct, librados::IoCtx &ioctx, const std::string &oid,
            const std::string &client_id, AsyncOpTracker &async_op_tracker,
            uint64_t start_after_tag_tid,
            const boost::optional<uint64_t> &tag_class,
            JournalMetadata::Tags *tags, Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), client_id(client_id),
      async_op_tracker(async_op_tracker),
      start_after_tag_tid(start_after_tag_tid), tag_class(tag_class),
      tags(tags), on_finish(on_finish) {
    async_op_tracker.start_op();
  }
  virtual ~C_GetTags() {
    async_op_tracker.finish_op();
  }

  void send() {
    send_tag_list();
  }

  void send_tag_list() {
    librados::ObjectReadOperation op;

    // the client id is used to exclude the tags that their tids less
    // than the minimum committed tags, see journal_tag_list
    client::tag_list_start(&op, start_after_tag_tid, MAX_RETURN, client_id,
                           tag_class);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_GetTags, &C_GetTags::handle_tag_list>);

    out_bl.clear();
    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_tag_list(int r) {
    if (r == 0) {
      std::set<cls::journal::Tag> journal_tags;
      bufferlist::iterator iter = out_bl.begin();

      r = client::tag_list_finish(&iter, &journal_tags);
      if (r == 0) {
        for (auto &journal_tag : journal_tags) {
          // list<Tag>
          tags->push_back(journal_tag);

          // update the next tag tid to start
          start_after_tag_tid = journal_tag.tid;
        }

        if (journal_tags.size() == MAX_RETURN) {

          // not finished yet, continue to read the next batch

          send_tag_list();
          return;
        }
      }
    }

    complete(r);
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

struct C_FlushCommitPosition : public Context {
  Context *commit_position_ctx;
  Context *on_finish;

  C_FlushCommitPosition(Context *commit_position_ctx, Context *on_finish)
    : commit_position_ctx(commit_position_ctx), on_finish(on_finish) {
  }
  virtual void finish(int r) override {
    if (commit_position_ctx != nullptr) {
      commit_position_ctx->complete(r);
    }
    on_finish->complete(r);
  }
};

struct C_AssertActiveTag : public Context {
  CephContext *cct;
  librados::IoCtx &ioctx;
  const std::string &oid;
  AsyncOpTracker &async_op_tracker;
  std::string client_id;
  uint64_t tag_tid;
  Context *on_finish;

  bufferlist out_bl;

  C_AssertActiveTag(CephContext *cct, librados::IoCtx &ioctx,
                    const std::string &oid, AsyncOpTracker &async_op_tracker,
                    const std::string &client_id, uint64_t tag_tid,
                    Context *on_finish)
    : cct(cct), ioctx(ioctx), oid(oid), async_op_tracker(async_op_tracker),
      client_id(client_id), tag_tid(tag_tid), on_finish(on_finish) {
    async_op_tracker.start_op();
  }
  virtual ~C_AssertActiveTag() {
    async_op_tracker.finish_op();
  }

  void send() {
    ldout(cct, 20) << "C_AssertActiveTag: " << __func__ << dendl;

    librados::ObjectReadOperation op;

    // cls_journal::tag_list, we need to try to get 2 tags in case the tag_tid is 0, see 
    // handle_send below
    client::tag_list_start(&op, tag_tid, 2, client_id, boost::none);

    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      this, nullptr, &utils::rados_state_callback<
        C_AssertActiveTag, &C_AssertActiveTag::handle_send>);

    int r = ioctx.aio_operate(oid, comp, &op, &out_bl);
    assert(r == 0);
    comp->release();
  }

  void handle_send(int r) {
    ldout(cct, 20) << "C_AssertActiveTag: " << __func__ << ": r=" << r << dendl;

    std::set<cls::journal::Tag> tags;
    if (r == 0) {
      bufferlist::iterator it = out_bl.begin();

      // decode to set<cls::journal::Tag>
      r = client::tag_list_finish(&it, &tags);
    }

    // NOTE: since 0 is treated as an uninitialized list filter, we need to
    // load to entries and look at the last tid
    // if tag_tid is 0, and no other additional filters we will always get at least one tag whose
    // tag_tid is 0, so here we need to try to get at list 2 tags, see journal_tag_list
    if (r == 0 && !tags.empty() && tags.rbegin()->tid > tag_tid) {
      r = -ESTALE;
    }
    complete(r);
  }

  virtual void finish(int r) {
    on_finish->complete(r);
  }
};

} // anonymous namespace

// C_GetClient, C_AllocateTag, C_GetTag, C_GetTags, C_AssertActiveTag
// C_FlushCommitPosition

JournalMetadata::JournalMetadata(ContextWQ *work_queue, SafeTimer *timer,
                                 Mutex *timer_lock, librados::IoCtx &ioctx,
                                 const std::string &oid, // "journal." + journal_id(i.e., image local id)
                                 const std::string &client_id,
                                 const Settings &settings)
    : RefCountedObject(NULL, 0), m_cct(NULL), m_oid(oid),
      m_client_id(client_id), m_settings(settings), m_order(0),
      m_splay_width(0), m_pool_id(-1), m_initialized(false),
      m_work_queue(work_queue), m_timer(timer), m_timer_lock(timer_lock),
      m_lock("JournalMetadata::m_lock"), m_commit_tid(0), m_watch_ctx(this),
      m_watch_handle(0), m_minimum_set(0), m_active_set(0),
      m_update_notifications(0), m_commit_position_ctx(NULL),
      m_commit_position_task_ctx(NULL) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
}

JournalMetadata::~JournalMetadata() {
  Mutex::Locker locker(m_lock);
  assert(!m_initialized);
}

void JournalMetadata::init(Context *on_finish) {
  {
    Mutex::Locker locker(m_lock);
    assert(!m_initialized);
    m_initialized = true;
  }

  // chain the init sequence (reverse order)
  on_finish = utils::create_async_context_callback(
    this, on_finish);

  // journal_metadata->handle_immutable_metadata will refresh
  // mutable metadata
  on_finish = new C_ImmutableMetadata(this, on_finish);

  on_finish = new FunctionContext([this, on_finish](int r) {
      if (r < 0) {
        lderr(m_cct) << __func__ << ": failed to watch journal"
                     << cpp_strerror(r) << dendl;

        Mutex::Locker locker(m_lock);

        m_watch_handle = 0;
        on_finish->complete(r);
        return;
      }

      get_immutable_metadata(&m_order, &m_splay_width, &m_pool_id, on_finish);
    });

  librados::AioCompletion *comp = librados::Rados::aio_create_completion(
    on_finish, nullptr, utils::rados_ctx_callback);

  // watch the journal metadata object, i.e., journal header object,
  // m_watch_ctx will be used to call JournalMetadata::handle_watch_notify and
  // JournalMetadata::handle_watch_error accordingly
  int r = m_ioctx.aio_watch(m_oid, comp, &m_watch_handle, &m_watch_ctx);
  assert(r == 0);
  comp->release();
}

void JournalMetadata::shut_down(Context *on_finish) {

  ldout(m_cct, 20) << __func__ << dendl;

  uint64_t watch_handle = 0;

  {
    Mutex::Locker locker(m_lock);
    m_initialized = false;
    std::swap(watch_handle, m_watch_handle);
  }

  // chain the shut down sequence (reverse order)
  on_finish = utils::create_async_context_callback(
    this, on_finish);
  on_finish = new FunctionContext([this, on_finish](int r) {
      ldout(m_cct, 20) << "shut_down: waiting for ops" << dendl;
      m_async_op_tracker.wait_for_ops(on_finish);
    });
  on_finish = new FunctionContext([this, on_finish](int r) {
      ldout(m_cct, 20) << "shut_down: flushing watch" << dendl;
      librados::Rados rados(m_ioctx);
      librados::AioCompletion *comp = librados::Rados::aio_create_completion(
        on_finish, nullptr, utils::rados_ctx_callback);
      r = rados.aio_watch_flush(comp);
      assert(r == 0);
      comp->release();
    });
  on_finish = new FunctionContext([this, on_finish](int r) {
      flush_commit_position(on_finish);
    });

  if (watch_handle != 0) {
    librados::AioCompletion *comp = librados::Rados::aio_create_completion(
      on_finish, nullptr, utils::rados_ctx_callback);

    // a watch handle is a pointer to Objecter::LingerOp
    int r = m_ioctx.aio_unwatch(watch_handle, comp);
    assert(r == 0);
    comp->release();
  } else {
    on_finish->complete(0);
  }
}

// called by
// JournalMetadata::init
// Journaler::get_immutable_metadata
void JournalMetadata::get_immutable_metadata(uint8_t *order,
					     uint8_t *splay_width,
					     int64_t *pool_id,
					     Context *on_finish) {

  // new client::C_ImmutableMetadata to get <order, splay width, pool id>
  client::get_immutable_metadata(m_ioctx, m_oid, order, splay_width, pool_id,
				 on_finish);
}

// called by
// JournalMetadata::refresh
// Journaler::get_mutable_metadata
void JournalMetadata::get_mutable_metadata(uint64_t *minimum_set,
					   uint64_t *active_set,
					   RegisteredClients *clients,
					   Context *on_finish) {

  // new client::C_MutableMetadata to get <minimum set, active set, set<Client>>
  client::get_mutable_metadata(m_ioctx, m_oid, minimum_set, active_set, clients,
			       on_finish);
}

// called by Journaler::register_client
void JournalMetadata::register_client(const bufferlist &data,
				      Context *on_finish) {
  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;

  librados::ObjectWriteOperation op;
  client::client_register(&op, m_client_id, data);

  // journal_metadata->async_notify_update(on_safe)
  C_NotifyUpdate *ctx = new C_NotifyUpdate(this, on_finish);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

// called by Journaler::update_client
void JournalMetadata::update_client(const bufferlist &data,
				    Context *on_finish) {
  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;

  librados::ObjectWriteOperation op;

  // data is ClientMeta variant, either ImageClientMeta, or MirrorPeerClientMeta
  client::client_update_data(&op, m_client_id, data);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this, on_finish);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

void JournalMetadata::unregister_client(Context *on_finish) {
  assert(!m_client_id.empty());

  ldout(m_cct, 10) << __func__ << ": " << m_client_id << dendl;

  librados::ObjectWriteOperation op;
  client::client_unregister(&op, m_client_id);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this, on_finish);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

void JournalMetadata::allocate_tag(uint64_t tag_class, const bufferlist &data,
                                   Tag *tag, Context *on_finish) {
  on_finish = new C_NotifyUpdate(this, on_finish);

  C_AllocateTag *ctx = new C_AllocateTag(m_cct, m_ioctx, m_oid,
                                         m_async_op_tracker, tag_class,
                                         data, tag, on_finish);

  // get the next tag_tid then encode tag_class, tag_tid and data to allocate
  // a tag on the journal metadata object
  ctx->send();
}

// we do not want this interface can only be used to get the client of
// JournalMetadata::m_client_id, but can used for any client
void JournalMetadata::get_client(const std::string &client_id,
                                 cls::journal::Client *client,
                                 Context *on_finish) {
  // access journal metadata object, i.e., "journal." + local image id
  // to get omap entry "client_" + client id
  C_GetClient *ctx = new C_GetClient(m_cct, m_ioctx, m_oid, m_async_op_tracker,
                                     client_id, client, on_finish);

  ctx->send();
}

void JournalMetadata::get_tag(uint64_t tag_tid, Tag *tag, Context *on_finish) {
  C_GetTag *ctx = new C_GetTag(m_cct, m_ioctx, m_oid, m_async_op_tracker,
                               tag_tid, tag, on_finish);

  // get the tag_tid specified tag
  ctx->send();
}

void JournalMetadata::get_tags(uint64_t start_after_tag_tid,
                               const boost::optional<uint64_t> &tag_class,
                               Tags *tags, Context *on_finish) {
  // m_client_id is used to exclude thos committed tags of current client,
  // i.e., the client identified by m_client_id
  C_GetTags *ctx = new C_GetTags(m_cct, m_ioctx, m_oid, m_client_id,
                                 m_async_op_tracker, start_after_tag_tid,
                                 tag_class, tags, on_finish);
  ctx->send();
}

// called by:
// Journaler::add_listener
// JournalRecorder::JournalRecorder
// JournalTrimmer::JournalTrimmer
void JournalMetadata::add_listener(JournalMetadataListener *listener) {
  Mutex::Locker locker(m_lock);

  while (m_update_notifications > 0) {
    m_update_cond.Wait(m_lock);
  }

  m_listeners.push_back(listener);
}

void JournalMetadata::remove_listener(JournalMetadataListener *listener) {
  Mutex::Locker locker(m_lock);

  while (m_update_notifications > 0) {
    m_update_cond.Wait(m_lock);
  }

  m_listeners.remove(listener);
}

void JournalMetadata::set_minimum_set(uint64_t object_set) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << __func__ << ": current=" << m_minimum_set
                   << ", new=" << object_set << dendl;

  if (m_minimum_set >= object_set) {
    return;
  }

  librados::ObjectWriteOperation op;
  client::set_minimum_set(&op, object_set);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this);
  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();

  m_minimum_set = object_set;
}

int JournalMetadata::set_active_set(uint64_t object_set) {
  C_SaferCond ctx;
  set_active_set(object_set, &ctx);
  return ctx.wait();
}

void JournalMetadata::set_active_set(uint64_t object_set, Context *on_finish) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << __func__ << ": current=" << m_active_set
                   << ", new=" << object_set << dendl;
  if (m_active_set >= object_set) {
    m_work_queue->queue(on_finish, 0);
    return;
  }

  librados::ObjectWriteOperation op;
  client::set_active_set(&op, object_set);

  C_NotifyUpdate *ctx = new C_NotifyUpdate(this, on_finish);
  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();

  m_active_set = object_set;
}

void JournalMetadata::assert_active_tag(uint64_t tag_tid, Context *on_finish) {
  Mutex::Locker locker(m_lock);

  C_AssertActiveTag *ctx = new C_AssertActiveTag(m_cct, m_ioctx, m_oid,
                                                 m_async_op_tracker,
                                                 m_client_id, tag_tid,
                                                 on_finish);
  ctx->send();
}

void JournalMetadata::flush_commit_position() {
  ldout(m_cct, 20) << __func__ << dendl;

  Mutex::Locker timer_locker(*m_timer_lock);
  Mutex::Locker locker(m_lock);
  if (m_commit_position_ctx == nullptr) {
    return;
  }

  cancel_commit_task();
  handle_commit_position_task();
}

void JournalMetadata::flush_commit_position(Context *on_safe) {
  ldout(m_cct, 20) << __func__ << dendl;

  Mutex::Locker timer_locker(*m_timer_lock);
  Mutex::Locker locker(m_lock);
  if (m_commit_position_ctx == nullptr) {
    // nothing to flush
    if (on_safe != nullptr) {
      m_work_queue->queue(on_safe, 0);
    }
    return;
  }

  if (on_safe != nullptr) {
    m_commit_position_ctx = new C_FlushCommitPosition(
      m_commit_position_ctx, on_safe);
  }
  cancel_commit_task();
  handle_commit_position_task();
}

void JournalMetadata::reserve_entry_tid(uint64_t tag_tid, uint64_t entry_tid) {
  Mutex::Locker locker(m_lock);
  uint64_t &allocated_entry_tid = m_allocated_entry_tids[tag_tid];
  if (allocated_entry_tid <= entry_tid) {
    allocated_entry_tid = entry_tid + 1;
  }
}

bool JournalMetadata::get_last_allocated_entry_tid(uint64_t tag_tid,
                                                   uint64_t *entry_tid) const {
  Mutex::Locker locker(m_lock);

  AllocatedEntryTids::const_iterator it = m_allocated_entry_tids.find(tag_tid);
  if (it == m_allocated_entry_tids.end()) {
    return false;
  }

  assert(it->second > 0);
  *entry_tid = it->second - 1;
  return true;
}

void JournalMetadata::handle_immutable_metadata(int r, Context *on_init) {
  if (r < 0) {
    lderr(m_cct) << "failed to initialize immutable metadata: "
                 << cpp_strerror(r) << dendl;

    on_init->complete(r);
    return;
  }

  // immutable metadata are set directly to member variables of JournalMetadata,
  // while mutable metadata are stashed in C_Refresh instance

  ldout(m_cct, 10) << "initialized immutable metadata" << dendl;

  refresh(on_init);
}

// called by
// JournalMetadata::handle_immutable_metadata
// JournalMetadata::handle_watch_reset
// JournalMetadata::handle_watch_notify
void JournalMetadata::refresh(Context *on_complete) {
  ldout(m_cct, 10) << "refreshing mutable metadata" << dendl;

  // journal_metadata->handle_refresh_complete
  C_Refresh *refresh = new C_Refresh(this, on_complete);

  // mutable metadata are stashed in C_Refresh instance
  get_mutable_metadata(&refresh->minimum_set, &refresh->active_set,
		       &refresh->registered_clients, refresh);
}

// called by JournalMetadata::C_Refresh::finish
void JournalMetadata::handle_refresh_complete(C_Refresh *refresh, int r) {
  ldout(m_cct, 10) << "refreshed mutable metadata: r=" << r << dendl;

  if (r == 0) {

    // now set mutable metadata from stashed mutable metadata

    Mutex::Locker locker(m_lock);

    // we only set the mutable metadata of the specified client, the
    // client is passed in originally from Journaler -> JournalMetadata
    Client client(m_client_id, bufferlist());

    RegisteredClients::iterator it = refresh->registered_clients.find(client);

    if (it != refresh->registered_clients.end()) {

      // set only when the client exists

      if (it->state == cls::journal::CLIENT_STATE_DISCONNECTED) {
	ldout(m_cct, 0) << "client flagged disconnected: " << m_client_id
			<< dendl;
      }
      m_minimum_set = MAX(m_minimum_set, refresh->minimum_set);
      m_active_set = MAX(m_active_set, refresh->active_set);

      m_registered_clients = refresh->registered_clients;
      m_client = *it;

      ++m_update_notifications;

      m_lock.Unlock();
      for (Listeners::iterator it = m_listeners.begin();
           it != m_listeners.end(); ++it) {

        // notify JournalRecorder and JournalTrimmer that the journal
        // mutable metadata has been updated

        (*it)->handle_update(this);
      }
      m_lock.Lock();

      if (--m_update_notifications == 0) {
        m_update_cond.Signal();
      }
    } else {
      lderr(m_cct) << "failed to locate client: " << m_client_id << dendl;
      r = -ENOENT;
    }
  }

  if (refresh->on_finish != NULL) {
    refresh->on_finish->complete(r);
  }
}

void JournalMetadata::cancel_commit_task() {
  ldout(m_cct, 20) << __func__ << dendl;

  assert(m_timer_lock->is_locked());
  assert(m_lock.is_locked());
  assert(m_commit_position_ctx != nullptr);
  assert(m_commit_position_task_ctx != nullptr);

  m_timer->cancel_event(m_commit_position_task_ctx);

  m_commit_position_task_ctx = NULL;
}

void JournalMetadata::schedule_commit_task() {
  ldout(m_cct, 20) << __func__ << dendl;

  assert(m_timer_lock->is_locked());
  assert(m_lock.is_locked());

  assert(m_commit_position_ctx != nullptr);

  if (m_commit_position_task_ctx == NULL) {
    // journal_metadata->handle_commit_position_task
    m_commit_position_task_ctx = new C_CommitPositionTask(this);

    m_timer->add_event_after(m_settings.commit_interval,
                             m_commit_position_task_ctx);
  }
}

// called by
// JournalMetadata::flush_commit_position()
// JournalMetadata::flush_commit_position(Context *on_safe)
// JournalMetadata::C_CommitPositionTask::finish
void JournalMetadata::handle_commit_position_task() {
  assert(m_timer_lock->is_locked());
  assert(m_lock.is_locked());

  ldout(m_cct, 20) << __func__ << ": "
                   << "client_id=" << m_client_id << ", "
                   << "commit_position=" << m_commit_position << dendl;

  librados::ObjectWriteOperation op;

  client::client_commit(&op, m_client_id, m_commit_position);

  // journal_metadata->async_notify_update
  Context *ctx = new C_NotifyUpdate(this, m_commit_position_ctx);

  m_commit_position_ctx = NULL;

  ctx = schedule_laggy_clients_disconnect(ctx);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);
  int r = m_ioctx.aio_operate(m_oid, comp, &op);
  assert(r == 0);
  comp->release();

  m_commit_position_task_ctx = NULL;
}

// called by
// JournalMetadata::handle_watch_error
// JournalMetadata::handle_watch_reset
// previous watch reset, we need to schedule a new watch, delegate this
// to the timer
void JournalMetadata::schedule_watch_reset() {
  assert(m_timer_lock->is_locked());

  // journal_metadata->handle_watch_reset
  m_timer->add_event_after(1, new C_WatchReset(this));
}

// called by JournalMetadata::C_WatchReset::finish, to re-watch and refresh
void JournalMetadata::handle_watch_reset() {
  assert(m_timer_lock->is_locked());

  if (!m_initialized) {
    return;
  }

  int r = m_ioctx.watch2(m_oid, &m_watch_handle, &m_watch_ctx);
  if (r < 0) {
    if (r == -ENOENT) {
      ldout(m_cct, 5) << __func__ << ": journal header not found" << dendl;
    } else {
      lderr(m_cct) << __func__ << ": failed to watch journal: "
                   << cpp_strerror(r) << dendl;
    }

    // schedule a timer to call us again
    schedule_watch_reset();
  } else {
    ldout(m_cct, 10) << __func__ << ": reset journal watch" << dendl;

    refresh(NULL);
  }
}

// called by JournalMetadata::C_WatchCtx::handle_notify, the watch was initiated by
// JournalMetadata::init or JournalMetadata::handle_watch_reset
void JournalMetadata::handle_watch_notify(uint64_t notify_id, uint64_t cookie) {
  ldout(m_cct, 10) << "journal header updated" << dendl;

  bufferlist bl;
  m_ioctx.notify_ack(m_oid, notify_id, cookie, bl);

  refresh(NULL);
}

// called by JournalMetadata::C_WatchCtx::handle_error
void JournalMetadata::handle_watch_error(int err) {
  if (err == -ENOTCONN) {
    ldout(m_cct, 5) << "journal watch error: header removed" << dendl;
  } else {
    lderr(m_cct) << "journal watch error: " << cpp_strerror(err) << dendl;
  }

  Mutex::Locker timer_locker(*m_timer_lock);
  Mutex::Locker locker(m_lock);

  // release old watch on error
  if (m_watch_handle != 0) {
    m_ioctx.unwatch2(m_watch_handle);
    m_watch_handle = 0;
  }

  if (m_initialized && err != -ENOENT) {
    // schedule to call journal_metadata->handle_watch_reset, i.e.,
    // re-watch and refresh
    schedule_watch_reset();
  }
}

uint64_t JournalMetadata::allocate_commit_tid(uint64_t object_num,
                                              uint64_t tag_tid,
                                              uint64_t entry_tid) {
  Mutex::Locker locker(m_lock);

  uint64_t commit_tid = ++m_commit_tid;

  m_pending_commit_tids[commit_tid] = CommitEntry(object_num, tag_tid,
                                                  entry_tid);

  ldout(m_cct, 20) << "allocated commit tid: commit_tid=" << commit_tid << " ["
                   << "object_num=" << object_num << ", "
                   << "tag_tid=" << tag_tid << ", "
                   << "entry_tid=" << entry_tid << "]"
                   << dendl;

  return commit_tid;
}

void JournalMetadata::overflow_commit_tid(uint64_t commit_tid,
                                          uint64_t object_num) {
  Mutex::Locker locker(m_lock);

  auto it = m_pending_commit_tids.find(commit_tid);
  assert(it != m_pending_commit_tids.end());
  assert(it->second.object_num < object_num);

  ldout(m_cct, 20) << __func__ << ": "
                   << "commit_tid=" << commit_tid << ", "
                   << "old_object_num=" << it->second.object_num << ", "
                   << "new_object_num=" << object_num << dendl;

  it->second.object_num = object_num;
}

void JournalMetadata::get_commit_entry(uint64_t commit_tid,
                                       uint64_t *object_num,
                                       uint64_t *tag_tid, uint64_t *entry_tid) {
  Mutex::Locker locker(m_lock);

  auto it = m_pending_commit_tids.find(commit_tid);
  assert(it != m_pending_commit_tids.end());

  *object_num = it->second.object_num;
  *tag_tid = it->second.tag_tid;
  *entry_tid = it->second.entry_tid;
}

void JournalMetadata::committed(uint64_t commit_tid,
                                const CreateContext &create_context) {
  ldout(m_cct, 20) << "committed tid=" << commit_tid << dendl;

  ObjectSetPosition commit_position;

  Context *stale_ctx = nullptr;

  {
    Mutex::Locker timer_locker(*m_timer_lock);
    Mutex::Locker locker(m_lock);

    assert(commit_tid > m_commit_position_tid);

    if (!m_commit_position.object_positions.empty()) {
      // in-flight commit position update

      commit_position = m_commit_position;
    } else {
      // safe commit position

      commit_position = m_client.commit_position;
    }

    // CommitEntry was inserted by JournalRecorder::append, FutureImpl::m_commit_tid
    // global uniquely identifies this journal::EventEntry
    CommitTids::iterator it = m_pending_commit_tids.find(commit_tid);
    assert(it != m_pending_commit_tids.end());

    CommitEntry &commit_entry = it->second;

    commit_entry.committed = true;

    bool update_commit_position = false;

    // m_pending_commit_tids inserted by JournalMetadata::allocate_commit_tid
    while (!m_pending_commit_tids.empty()) {
      CommitTids::iterator it = m_pending_commit_tids.begin();

      CommitEntry &commit_entry = it->second;

      // commit may not occur in order, only remove the CommitEntry
      // when all its previous CommitEntry(s) have been committed

      if (!commit_entry.committed) {
        break;
      }

      // the newer commit position always at the front of the
      // commit position list, we will prune to ensure only one
      // commit position at the same splay offset
      commit_position.object_positions.emplace_front(
        commit_entry.object_num, commit_entry.tag_tid,
        commit_entry.entry_tid);

      m_pending_commit_tids.erase(it);

      update_commit_position = true;
    }

    if (!update_commit_position) {

      // the previous CommitEntry(s) have not committed yet

      return;
    }

    // used to prune the position to have one position per splay offset
    std::set<uint8_t> in_use_splay_offsets;

    // iterate from the newest commit position and try to prune old
    // commit position(s) at the same splay offset
    ObjectPositions::iterator ob_it = commit_position.object_positions.begin();

    while (ob_it != commit_position.object_positions.end()) {
      uint8_t splay_offset = ob_it->object_number % m_splay_width;

      // we do not want duplicate commit position at the same splay offset,
      // so remove the duplicate old commit positions
      if (!in_use_splay_offsets.insert(splay_offset).second) {

        // insert failed which means there is already an commit position
        // at the splay offset, i.e., there is already newer commit position
        // exists

        ob_it = commit_position.object_positions.erase(ob_it);
      } else {
        ++ob_it;
      }
    }

    stale_ctx = m_commit_position_ctx;

    // JournalTrimmer::C_CommitPositionSafe, actually do nothing
    m_commit_position_ctx = create_context();

    m_commit_position = commit_position;
    m_commit_position_tid = commit_tid;

    ldout(m_cct, 20) << "updated commit position: " << commit_position << ", "
                     << "on_safe=" << m_commit_position_ctx << dendl;

    // schedule a timer to update the commit position
    schedule_commit_task();
  }


  if (stale_ctx != nullptr) {
    ldout(m_cct, 20) << "canceling stale commit: on_safe=" << stale_ctx
                     << dendl;

    stale_ctx->complete(-ESTALE);
  }
}

// never be called
void JournalMetadata::notify_update() {
  ldout(m_cct, 10) << "notifying journal header update" << dendl;

  bufferlist bl;
  m_ioctx.notify2(m_oid, bl, 5000, NULL);
}

// called by JournalMetadata::C_NotifyUpdate::finish, which used
// by client and tag related updates
void JournalMetadata::async_notify_update(Context *on_safe) {
  ldout(m_cct, 10) << "async notifying journal header update" << dendl;

  // used to finish the on_safe callback, always call it with 0 as
  // result, so even the notify failed, we ignore the notify error
  C_AioNotify *ctx = new C_AioNotify(this, on_safe);

  librados::AioCompletion *comp =
    librados::Rados::aio_create_completion(ctx, NULL,
                                           utils::rados_ctx_callback);

  bufferlist bl;
  int r = m_ioctx.aio_notify(m_oid, comp, bl, 5000, NULL);
  assert(r == 0);

  comp->release();
}

// called by Journaler::~Journaler
void JournalMetadata::wait_for_ops() {
  C_SaferCond ctx;
  m_async_op_tracker.wait_for_ops(&ctx);
  ctx.wait();
}

// called by JournalMetadata::C_AioNotify::finish, which means the
// JournalMetadata::async_notify_update has completed
void JournalMetadata::handle_notified(int r) {
  ldout(m_cct, 10) << "notified journal header update: r=" << r << dendl;
}

// called by JournalMetadata::handle_commit_position_task
Context *JournalMetadata::schedule_laggy_clients_disconnect(Context *on_finish) {
  assert(m_lock.is_locked());

  ldout(m_cct, 20) << __func__ << dendl;

  if (m_settings.max_concurrent_object_sets <= 0) {
    return on_finish;
  }

  Context *ctx = on_finish;

  for (auto &c : m_registered_clients) {
    if (c.state == cls::journal::CLIENT_STATE_DISCONNECTED ||
	c.id == m_client_id ||
	m_settings.whitelisted_laggy_clients.count(c.id) > 0) {
      continue;
    }

    const std::string &client_id = c.id;
    uint64_t object_set = 0;
    if (!c.commit_position.object_positions.empty()) {
      auto &position = *(c.commit_position.object_positions.begin());
      object_set = position.object_number / m_splay_width;
    }

    if (m_active_set > object_set + m_settings.max_concurrent_object_sets) {
      ldout(m_cct, 1) << __func__ << ": " << client_id
		      << ": scheduling disconnect" << dendl;

      ctx = new FunctionContext([this, client_id, ctx](int r1) {
          ldout(m_cct, 10) << __func__ << ": " << client_id
                           << ": flagging disconnected" << dendl;

          librados::ObjectWriteOperation op;

          // two states for journal client: CONNECTED or DISCONNECTED
          client::client_update_state(&op, client_id,
                                      cls::journal::CLIENT_STATE_DISCONNECTED);

          librados::AioCompletion *comp =
              librados::Rados::aio_create_completion(ctx, nullptr,
                                                     utils::rados_ctx_callback);
          int r = m_ioctx.aio_operate(m_oid, comp, &op);
          assert(r == 0);
          comp->release();
	});
    }
  }

  if (ctx == on_finish) {
    ldout(m_cct, 20) << __func__ << ": no laggy clients to disconnect" << dendl;
  }

  return ctx;
}

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata::RegisteredClients &clients) {
  os << "[";
  for (JournalMetadata::RegisteredClients::const_iterator c = clients.begin();
       c != clients.end(); ++c) {
    os << (c == clients.begin() ? "" : ", " ) << *c;
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os,
			 const JournalMetadata &jm) {
  Mutex::Locker locker(jm.m_lock);
  os << "[oid=" << jm.m_oid << ", "
     << "initialized=" << jm.m_initialized << ", "
     << "order=" << (int)jm.m_order << ", "
     << "splay_width=" << (int)jm.m_splay_width << ", "
     << "pool_id=" << jm.m_pool_id << ", "
     << "minimum_set=" << jm.m_minimum_set << ", "
     << "active_set=" << jm.m_active_set << ", "
     << "client_id=" << jm.m_client_id << ", "
     << "commit_tid=" << jm.m_commit_tid << ", "
     << "commit_interval=" << jm.m_settings.commit_interval << ", "
     << "commit_position=" << jm.m_commit_position << ", "
     << "registered_clients=" << jm.m_registered_clients << "]";
  return os;
}

} // namespace journal
