// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Journal.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/journal/Replay.h"
#include "librbd/Utils.h"
#include "cls/journal/cls_journal_types.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/Settings.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"

#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal: "

namespace librbd {

namespace {

struct C_DecodeTag : public Context {
  CephContext *cct;
  Mutex *lock;
  uint64_t *tag_tid;
  journal::TagData *tag_data;
  Context *on_finish;

  cls::journal::Tag tag;

  C_DecodeTag(CephContext *cct, Mutex *lock, uint64_t *tag_tid,
              journal::TagData *tag_data, Context *on_finish)
    : cct(cct), lock(lock), tag_tid(tag_tid), tag_data(tag_data),
      on_finish(on_finish) {
  }

  virtual void complete(int r) override {
    on_finish->complete(process(r));
    Context::complete(0);
  }
  virtual void finish(int r) override {
  }

  int process(int r) {
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to allocate tag: " << cpp_strerror(r) << dendl;
      return r;
    }

    Mutex::Locker locker(*lock);
    *tag_tid = tag.tid;

    bufferlist::iterator data_it = tag.data.begin();
    r = decode(&data_it, tag_data);
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to decode allocated tag" << dendl;
      return r;
    }

    ldout(cct, 20) << this << " " << __func__ << ": "
                   << "allocated journal tag: "
                   << "tid=" << tag.tid << ", "
                   << "data=" << *tag_data << dendl;
    return 0;
  }

  static int decode(bufferlist::iterator *it,
                    journal::TagData *tag_data) {
    try {
      ::decode(*tag_data, *it);
    } catch (const buffer::error &err) {
      return -EBADMSG;
    }
    return 0;
  }

};

struct C_DecodeTags : public Context {
  CephContext *cct;
  Mutex *lock;
  uint64_t *tag_tid;
  journal::TagData *tag_data;
  Context *on_finish;

  ::journal::Journaler::Tags tags;

  C_DecodeTags(CephContext *cct, Mutex *lock, uint64_t *tag_tid,
               journal::TagData *tag_data, Context *on_finish)
    : cct(cct), lock(lock), tag_tid(tag_tid), tag_data(tag_data),
      on_finish(on_finish) {
  }

  virtual void complete(int r) {
    on_finish->complete(process(r));
    Context::complete(0);
  }
  virtual void finish(int r) override {
  }

  int process(int r) {
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to retrieve journal tags: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    if (tags.empty()) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "no journal tags retrieved" << dendl;
      return -ENOENT;
    }

    Mutex::Locker locker(*lock);
    *tag_tid = tags.back().tid;

    bufferlist::iterator data_it = tags.back().data.begin();
    r = C_DecodeTag::decode(&data_it, tag_data);
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to decode journal tag" << dendl;
      return r;
    }

    ldout(cct, 20) << this << " " << __func__ << ": "
                   << "most recent journal tag: "
                   << "tid=" << *tag_tid << ", "
                   << "data=" << *tag_data << dendl;
    return 0;
  }
};

// TODO: once journaler is 100% async, remove separate threads and
// reuse ImageCtx's thread pool
class ThreadPoolSingleton : public ThreadPool {
public:
  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "librbd::Journal", "tp_librbd_journ", 1) {
    start();
  }
  virtual ~ThreadPoolSingleton() {
    stop();
  }
};

class SafeTimerSingleton : public SafeTimer {
public:
  Mutex lock;

  explicit SafeTimerSingleton(CephContext *cct)
      : SafeTimer(cct, lock, true),
        lock("librbd::Journal::SafeTimerSingleton::lock") {
    init();
  }
  virtual ~SafeTimerSingleton() {
    Mutex::Locker locker(lock);
    shutdown();
  }
};

// called by Journal<I>::get_tag_owner, Journal<I>::request_resyn, Journal<I>::promote
template <typename J>
int open_journaler(CephContext *cct, J *journaler,
                   cls::journal::Client *client,
                   journal::ImageClientMeta *client_meta,
                   journal::TagData *tag_data) {
  C_SaferCond init_ctx;

  // watch journal.xxx metadata object, get immutable (order, splay width, pool id)
  // and mutable metadata (minimum set, active set, set<Client>)
  journaler->init(&init_ctx);

  int r = init_ctx.wait();
  if (r < 0) {
    return r;
  }

  // get all registered clients and filter by the specified client id
  // note: every newly created primary image registered a master client with client
  // id set to IMAGE_CLIENT_ID and allocated an initial tag with tag.mirror uuid
  // set to LOCAL_MIRROR_UUID, every newly created secondary image also registered
  // a master client with client id set to IMAGE_CLIENT_ID while allocated an initial
  // tag with tag.mirror uuid set to remote mirror uuid, see Journal<I>::create
  r = journaler->get_cached_client(Journal<ImageCtx>::IMAGE_CLIENT_ID, client);
  if (r < 0) {
    return r;
  }

  librbd::journal::ClientData client_data;
  bufferlist::iterator bl_it = client->data.begin();
  try {
    ::decode(client_data, bl_it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  journal::ImageClientMeta *image_client_meta =
    boost::get<journal::ImageClientMeta>(&client_data.client_meta);
  if (image_client_meta == nullptr) {
    return -EINVAL;
  }

  *client_meta = *image_client_meta;

  C_SaferCond get_tags_ctx;
  Mutex lock("lock");
  uint64_t tag_tid;
  C_DecodeTags *tags_ctx = new C_DecodeTags(
      cct, &lock, &tag_tid, tag_data, &get_tags_ctx);

  // get tags of the journal and filter out those have the tag class and
  // past the commit position of the local client
  journaler->get_tags(client_meta->tag_class, &tags_ctx->tags, tags_ctx);

  r = get_tags_ctx.wait();
  if (r < 0) {
    return r;
  }
  return 0;
}

// called by Journal::create, Journal::promote, Journal::demote
template <typename J>
int allocate_journaler_tag(CephContext *cct, J *journaler,
                           const cls::journal::Client &client,
                           uint64_t tag_class,
                           const journal::TagData &prev_tag_data,
                           const std::string &mirror_uuid,
                           cls::journal::Tag *new_tag) {
  journal::TagData tag_data;

  if (!client.commit_position.object_positions.empty()) {

    // the latest committed entry always at the front of the commit
    // position list, see JournalMetadata::committed

    auto position = client.commit_position.object_positions.front();

    tag_data.predecessor_commit_valid = true;
    tag_data.predecessor_tag_tid = position.tag_tid;
    tag_data.predecessor_entry_tid = position.entry_tid;
  }

  tag_data.predecessor_mirror_uuid = prev_tag_data.mirror_uuid;
  tag_data.mirror_uuid = mirror_uuid; // owner of the tag (exclusive lock epoch)

  bufferlist tag_bl;
  ::encode(tag_data, tag_bl);

  C_SaferCond allocate_tag_ctx;

  // allocate a new tag with specified tag class and tag data and
  // then get the allocated tag
  journaler->allocate_tag(tag_class, tag_bl, new_tag, &allocate_tag_ctx);

  int r = allocate_tag_ctx.wait();
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to allocate tag: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

} // anonymous namespace

using util::create_async_context_callback;
using util::create_context_callback;

// client id for local image
template <typename I>
const std::string Journal<I>::IMAGE_CLIENT_ID("");

// mirror uuid to use for local images
template <typename I>
const std::string Journal<I>::LOCAL_MIRROR_UUID("");

// mirror uuid to use for orphaned (demoted) images
template <typename I>
const std::string Journal<I>::ORPHAN_MIRROR_UUID("<orphan>");

template <typename I>
std::ostream &operator<<(std::ostream &os,
                         const typename Journal<I>::State &state) {
  switch (state) {
  case Journal<I>::STATE_UNINITIALIZED:
    os << "Uninitialized";
    break;
  case Journal<I>::STATE_INITIALIZING:
    os << "Initializing";
    break;
  case Journal<I>::STATE_REPLAYING:
    os << "Replaying";
    break;
  case Journal<I>::STATE_FLUSHING_RESTART:
    os << "FlushingRestart";
    break;
  case Journal<I>::STATE_RESTARTING_REPLAY:
    os << "RestartingReplay";
    break;
  case Journal<I>::STATE_FLUSHING_REPLAY:
    os << "FlushingReplay";
    break;
  case Journal<I>::STATE_READY:
    os << "Ready";
    break;
  case Journal<I>::STATE_STOPPING:
    os << "Stopping";
    break;
  case Journal<I>::STATE_CLOSING:
    os << "Closing";
    break;
  case Journal<I>::STATE_CLOSED:
    os << "Closed";
    break;
  default:
    os << "Unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

template <typename I>
Journal<I>::Journal(I &image_ctx)
  : m_image_ctx(image_ctx), m_journaler(NULL),
    m_lock("Journal<I>::m_lock"), m_state(STATE_UNINITIALIZED),
    m_error_result(0), m_replay_handler(this), m_close_pending(false),
    m_event_lock("Journal<I>::m_event_lock"), m_event_tid(0),
    m_blocking_writes(false), m_journal_replay(NULL),
    m_metadata_listener(this) {

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << ": ictx=" << &m_image_ctx << dendl;

  ThreadPoolSingleton *thread_pool_singleton;
  cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
    thread_pool_singleton, "librbd::journal::thread_pool");

  m_work_queue = new ContextWQ("librbd::journal::work_queue",
                               cct->_conf->rbd_op_thread_timeout,
                               thread_pool_singleton);

  SafeTimerSingleton *safe_timer_singleton;
  cct->lookup_or_create_singleton_object<SafeTimerSingleton>(
    safe_timer_singleton, "librbd::journal::safe_timer");

  m_timer = safe_timer_singleton;
  m_timer_lock = &safe_timer_singleton->lock;
}

template <typename I>
Journal<I>::~Journal() {
  if (m_work_queue != nullptr) {
    m_work_queue->drain();
    delete m_work_queue;
  }

  assert(m_state == STATE_UNINITIALIZED || m_state == STATE_CLOSED);
  assert(m_journaler == NULL);
  assert(m_journal_replay == NULL);
  assert(m_on_replay_close_request == nullptr);
  assert(m_wait_for_state_contexts.empty());
}

// static
template <typename I>
bool Journal<I>::is_journal_supported(I &image_ctx) {
  assert(image_ctx.snap_lock.is_locked());
  return ((image_ctx.features & RBD_FEATURE_JOURNALING) &&
          !image_ctx.read_only && image_ctx.snap_id == CEPH_NOSNAP);
}

// static
// called by Journal<I>::reset and librbd::create_v2
template <typename I>
int Journal<I>::create(librados::IoCtx &io_ctx, const std::string &image_id,
		       uint8_t order, uint8_t splay_width,
		       const std::string &object_pool, bool non_primary,
                       const std::string &primary_mirror_uuid) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  librados::Rados rados(io_ctx);
  int64_t pool_id = -1;
  if (!object_pool.empty()) {
    IoCtx data_io_ctx;

    int r = rados.ioctx_create(object_pool.c_str(), data_io_ctx);
    if (r != 0) {
      lderr(cct) << __func__ << ": "
                 << "failed to create journal: "
		 << "error opening journal objects pool '" << object_pool
		 << "': " << cpp_strerror(r) << dendl;
      return r;
    }

    // pool for journal_data.xxx objects
    pool_id = data_io_ctx.get_id();
  }

  Journaler journaler(io_ctx, image_id, IMAGE_CLIENT_ID, {});

  // create a journal.xxx metadata object to manage the journaling
  // of this image
  int r = journaler.create(order, splay_width, pool_id);
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to create journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  cls::journal::Client client;
  cls::journal::Tag tag;
  journal::TagData tag_data;

  // if we are primary then the provided mirror uuid must be ""
  // if we are mirror image then the provided mirror uuid must be the current primary image
  assert(non_primary ^ primary_mirror_uuid.empty());

  // for local created mirror image, primary_mirror_uuid is set to remote mirror uuid,
  // see BootstrapRequest<I>::create_local_image and
  // CreateImageRequest<I>::create_image

  // tag owner
  std::string mirror_uuid = (non_primary ? primary_mirror_uuid :
                                           LOCAL_MIRROR_UUID);

  // allocate a tag with owner set to 'mirror_uuid', because we have
  // created a new journal metadata object, so tag id and tag class
  // both start from 0
  // allocate_journaler_tag(cct, journaler, client, tag_class, prev_tag_data, mirror_uuid, *new_tag)
  // client is used to get the latest commit position to set tag_data.prev tag tid and tag_data.prev entry id
  // tag_class is part of Tag
  // pre_tag_data is used to set tag_data.prev mirror uuid
  // mirror_uuid is used to set tag_data.mirror uuid
  r = allocate_journaler_tag(cct, &journaler, client,
                             cls::journal::Tag::TAG_CLASS_NEW,
                             tag_data, mirror_uuid, &tag);

  // journaler.m_client_id is IMAGE_CLIENT_ID, so we are registering a
  // master client

  bufferlist client_data;
  ::encode(journal::ClientData{journal::ImageClientMeta{tag.tag_class}},
           client_data);

  // register the local, i.e., master, client, i.e., journaler.m_client_id
  // is IMAGE_CLIENT_ID,
  // note: rbd-mirror daemon will register a mirror peer client on remote
  // journal, see BootstrapRequest<I>::register_client
  r = journaler.register_client(client_data);
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to register client: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// static
// called by librbd::update_features and librbd::remove
template <typename I>
int Journal<I>::remove(librados::IoCtx &io_ctx, const std::string &image_id) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  Journaler journaler(io_ctx, image_id, IMAGE_CLIENT_ID, {});

  bool journal_exists;

  // stat journal header to check if the journal metadata object exists
  int r = journaler.exists(&journal_exists);
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to stat journal header: " << cpp_strerror(r) << dendl;
    return r;
  } else if (!journal_exists) {
    return 0;
  }

  C_SaferCond cond;

  // open journal
  journaler.init(&cond);
  BOOST_SCOPE_EXIT_ALL(&journaler) {
    journaler.shut_down();
  };

  r = cond.wait();
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to initialize journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = journaler.remove(true);
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to remove journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// static
template <typename I>
int Journal<I>::reset(librados::IoCtx &io_ctx, const std::string &image_id) {
  CephContext *cct = reinterpret_cast<CephContext *>(io_ctx.cct());
  ldout(cct, 5) << __func__ << ": image=" << image_id << dendl;

  Journaler journaler(io_ctx, image_id, IMAGE_CLIENT_ID, {});

  C_SaferCond cond;

  journaler.init(&cond);
  BOOST_SCOPE_EXIT_ALL(&journaler) {
    journaler.shut_down();
  };

  int r = cond.wait();
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to initialize journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  uint8_t order, splay_width;
  int64_t pool_id;
  journaler.get_metadata(&order, &splay_width, &pool_id);

  std::string pool_name;
  if (pool_id != -1) {
    librados::Rados rados(io_ctx);
    r = rados.pool_reverse_lookup(pool_id, &pool_name);
    if (r < 0) {
      lderr(cct) << __func__ << ": "
                 << "failed to lookup data pool: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  r = journaler.remove(true);
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to reset journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = create(io_ctx, image_id, order, splay_width, pool_name, false, "");
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to create journal: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// static
template <typename I>
int Journal<I>::is_tag_owner(I *image_ctx, bool *is_tag_owner) {

  // call another static function

  return Journal<>::is_tag_owner(image_ctx->md_ctx, image_ctx->id, is_tag_owner);
}

// static
template <typename I>
int Journal<I>::is_tag_owner(IoCtx& io_ctx, std::string& image_id,
                             bool *is_tag_owner) {
  std::string mirror_uuid;

  // get the last tag and check the tag owner
  int r = get_tag_owner(io_ctx, image_id, &mirror_uuid);
  if (r < 0) {
    return r;
  }

  *is_tag_owner = (mirror_uuid == LOCAL_MIRROR_UUID);
  return 0;
}

// static
template <typename I>
int Journal<I>::get_tag_owner(I *image_ctx, std::string *mirror_uuid) {

  // call another static function

  return get_tag_owner(image_ctx->md_ctx, image_ctx->id, mirror_uuid);
}

// static
template <typename I>
int Journal<I>::get_tag_owner(IoCtx& io_ctx, std::string& image_id,
                              std::string *mirror_uuid) {
  CephContext *cct = (CephContext *)io_ctx.cct();

  ldout(cct, 20) << __func__ << dendl;

  Journaler journaler(io_ctx, image_id, IMAGE_CLIENT_ID, {});

  // <client id, bufferlist data>
  cls::journal::Client client;
  // <static meta type, tag class, resync_requested>
  journal::ImageClientMeta client_meta;
  // <mirror uuid, pre mirror uuid, bool pre commit valid, pre tag id, pre entry id>
  journal::TagData tag_data;

  // init Journaler and get the last tag of the client, i.e., the last
  // exclusive lock owner
  int r = open_journaler(cct, &journaler, &client, &client_meta, &tag_data);
  if (r >= 0) {
    *mirror_uuid = tag_data.mirror_uuid;
  }

  journaler.shut_down();

  return r;
}

// static
template <typename I>
int Journal<I>::request_resync(I *image_ctx) {
  CephContext *cct = image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  Journaler journaler(image_ctx->md_ctx, image_ctx->id, IMAGE_CLIENT_ID, {});

  cls::journal::Client client;
  journal::ImageClientMeta client_meta;
  journal::TagData tag_data;
  int r = open_journaler(image_ctx->cct, &journaler, &client, &client_meta,
                         &tag_data);
  BOOST_SCOPE_EXIT_ALL(&journaler) {
    journaler.shut_down();
  };

  if (r < 0) {
    return r;
  }

  client_meta.resync_requested = true;

  journal::ClientData client_data(client_meta);
  bufferlist client_data_bl;
  ::encode(client_data, client_data_bl);

  C_SaferCond update_client_ctx;
  journaler.update_client(client_data_bl, &update_client_ctx);

  r = update_client_ctx.wait();
  if (r < 0) {
    lderr(cct) << __func__ << ": "
               << "failed to update client: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

// static
// tag owner: last tag.mirror uuid(maybe m_remote_mirror_uuid) -> LOCAL_MIRROR_UUID
template <typename I>
int Journal<I>::promote(I *image_ctx) {
  CephContext *cct = image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  // journaler.m_client_id is IMAGE_CLIENT_ID
  Journaler journaler(image_ctx->md_ctx, image_ctx->id, IMAGE_CLIENT_ID, {});

  cls::journal::Client client;
  journal::ImageClientMeta client_meta;
  journal::TagData tag_data;

  int r = open_journaler(image_ctx->cct, &journaler, &client, &client_meta,
                         &tag_data);
  BOOST_SCOPE_EXIT_ALL(&journaler) {
    journaler.shut_down();
  };

  if (r < 0) {
    return r;
  }

  // allocate a tag with owner, i.e., tag.mirror uuid, set to LOCAL_MIRROR_UUID,
  // so we can acquire the exclusive lock and accept aio request

  cls::journal::Tag new_tag;

  // allocate a new tag with owner set to local master client
  // allocate_journaler_tag(cct, journaler, client, tag_class, prev_tag_data, mirror_uuid, *new_tag)
  // client is used to get the latest commit position to set tag_data.prev tag tid and tag_data.prev entry id
  // tag_class is part of Tag
  // pre_tag_data is used to set tag_data.prev mirror uuid
  // mirror_uuid is used to set tag_data.mirror uuid
  r = allocate_journaler_tag(cct, &journaler, client, client_meta.tag_class,
                             tag_data, LOCAL_MIRROR_UUID, &new_tag);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
bool Journal<I>::is_journal_ready() const {
  Mutex::Locker locker(m_lock);

  // replay has finished

  return (m_state == STATE_READY);
}

template <typename I>
bool Journal<I>::is_journal_replaying() const {
  Mutex::Locker locker(m_lock);

  return (m_state == STATE_REPLAYING ||
          m_state == STATE_FLUSHING_REPLAY ||
          m_state == STATE_FLUSHING_RESTART ||
          m_state == STATE_RESTARTING_REPLAY);
}

template <typename I>
void Journal<I>::wait_for_journal_ready(Context *on_ready) {
  on_ready = create_async_context_callback(m_image_ctx, on_ready);

  Mutex::Locker locker(m_lock);
  if (m_state == STATE_READY) {
    on_ready->complete(m_error_result);
  } else {
    wait_for_steady_state(on_ready);
  }
}

// called in AcquireRequest<I>::send_open_journal
template <typename I>
void Journal<I>::open(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  on_finish = create_async_context_callback(m_image_ctx, on_finish);

  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_UNINITIALIZED);

  // called until STATE_READY or STATE_CLOSED
  wait_for_steady_state(on_finish);

  // new Journaler instance and init it, see also ImageReplayer<I>::init_remote_journaler
  // transit into STATE_INITIALIZING
  create_journaler();
}

template <typename I>
void Journal<I>::close(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  on_finish = create_async_context_callback(m_image_ctx, on_finish);

  Mutex::Locker locker(m_lock);

  assert(m_state != STATE_UNINITIALIZED);

  if (m_state == STATE_CLOSED) {
    on_finish->complete(m_error_result);
    return;
  }

  if (m_state == STATE_READY) {
    stop_recording();
  }

  // interrupt external replay if active
  if (m_on_replay_close_request != nullptr) {

    // set by Journal<I>::start_external_replay

    m_on_replay_close_request->complete(0);
    m_on_replay_close_request = nullptr;
  }

  // if we are in state STATE_REPLAYING or other non-steady states, we
  // will wait, and the flag m_close_pending will notify them to shutdown
  m_close_pending = true;

  wait_for_steady_state(on_finish);
}

template <typename I>
bool Journal<I>::is_tag_owner() const {
  return (m_tag_data.mirror_uuid == LOCAL_MIRROR_UUID);
}

template <typename I>
journal::TagData Journal<I>::get_tag_data() const {
  return m_tag_data;
}

// tag owner: LOCAL_MIRROR_UUID -> ORPHAN_MIRROR_UUID
template <typename I>
int Journal<I>::demote() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_journaler != nullptr && is_tag_owner());

  cls::journal::Client client;

  // get master client of the journal to get the prev tag tid and entry id
  int r = m_journaler->get_cached_client(IMAGE_CLIENT_ID, &client);
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to retrieve client: " << cpp_strerror(r) << dendl;
    return r;
  }

  cls::journal::Tag new_tag;

  // allocate a new tag with owner set to orphan, so neither the local (master)
  // client nor the remote (mirror peer) client owns the exclusive lock
  // allocate_journaler_tag(cct, journaler, client, tag_class, prev_tag_data, mirror_uuid, *new_tag)
  // client is used to get the latest commit position to set tag_data.prev tag tid and tag_data.prev entry id
  // tag_class is part of Tag
  // pre_tag_data is used to set tag_data.prev mirror uuid
  // mirror_uuid is used to set tag_data.mirror uuid

  // m_tag_class was got in Journal<I>::handle_initialized
  r = allocate_journaler_tag(cct, m_journaler, client, m_tag_class,
                             m_tag_data, ORPHAN_MIRROR_UUID, &new_tag);
  if (r < 0) {
    return r;
  }

  // update m_tag_data to the newly created tag.tag_data
  bufferlist::iterator tag_data_bl_it = new_tag.data.begin();
  r = C_DecodeTag::decode(&tag_data_bl_it, &m_tag_data);
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to decode newly allocated tag" << dendl;
    return r;
  }

  // append the demote event to the journal

  journal::EventEntry event_entry{journal::DemoteEvent{}};
  bufferlist event_entry_bl;
  ::encode(event_entry, event_entry_bl);

  m_tag_tid = new_tag.tid;
  Future future = m_journaler->append(m_tag_tid, event_entry_bl);

  C_SaferCond ctx;

  future.flush(&ctx);

  r = ctx.wait();
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to append demotion journal event: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  m_journaler->committed(future);

  C_SaferCond flush_ctx;

  m_journaler->flush_commit_position(&flush_ctx);

  r = flush_ctx.wait();
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to flush demotion commit position: "
               << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// ImageReplayer has an interface with the same name, i.e.,
// ImageReplayer<I>::allocate_local_tag to mirror the tag of remote journal tag, but
// it has to modify the tag.mirror_uuid and tag.prev mirror uuid before allocate the
// local tag
// in this interface we allocate a tag with tag.mirror uuid and tag.prev mirror uuid
// both set to LOCAL_MIRROR_UUID blindly

// called by librbd::journal::StandardPolicy::allocate_tag_on_lock
// ImageCtx has two policy instances: 1) librbd/exclusive_lock/StandardPolicy
// and 2) librbd/journal/StandardPolicy
template <typename I>
void Journal<I>::allocate_local_tag(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  bool predecessor_commit_valid = false;
  uint64_t predecessor_tag_tid = 0;
  uint64_t predecessor_entry_tid = 0;

  {
    Mutex::Locker locker(m_lock);

    assert(m_journaler != nullptr && is_tag_owner());

    cls::journal::Client client;

    int r = m_journaler->get_cached_client(IMAGE_CLIENT_ID, &client);
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to retrieve client: " << cpp_strerror(r) << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, r);
      return;
    }

    // since we are primary, populate the predecessor with our known commit
    // position
    assert(m_tag_data.mirror_uuid == LOCAL_MIRROR_UUID);

    // new -> order
    if (!client.commit_position.object_positions.empty()) {
      auto position = client.commit_position.object_positions.front();

      predecessor_commit_valid = true;
      predecessor_tag_tid = position.tag_tid;
      predecessor_entry_tid = position.entry_tid;
    }
  }

  // allocate a tag with current owner and previous owner set to local, i.e.,
  // master, client
  allocate_tag(LOCAL_MIRROR_UUID, LOCAL_MIRROR_UUID, predecessor_commit_valid,
               predecessor_tag_tid, predecessor_entry_tid, on_finish);
}

// called by Journal<I>::allocate_local_tag and ImageReplayer<I>::allocate_local_tag
template <typename I>
void Journal<I>::allocate_tag(const std::string &mirror_uuid,
                              const std::string &predecessor_mirror_uuid,
                              bool predecessor_commit_valid,
                              uint64_t predecessor_tag_tid,
                              uint64_t predecessor_entry_tid,
                              Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ":  mirror_uuid=" << mirror_uuid
                 << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_journaler != nullptr);

  journal::TagData tag_data;

  tag_data.mirror_uuid = mirror_uuid; // owner of the tag (exclusive lock epoch)

  tag_data.predecessor_mirror_uuid = predecessor_mirror_uuid;
  tag_data.predecessor_commit_valid = predecessor_commit_valid;
  tag_data.predecessor_tag_tid = predecessor_tag_tid;
  tag_data.predecessor_entry_tid = predecessor_entry_tid;

  bufferlist tag_bl;
  ::encode(tag_data, tag_bl);

  // once the new tag got created, then update the m_tag_data
  C_DecodeTag *decode_tag_ctx = new C_DecodeTag(cct, &m_lock, &m_tag_tid,
                                                &m_tag_data, on_finish);

  // m_tag_class was got in Journal<I>::handle_initialized
  m_journaler->allocate_tag(m_tag_class, tag_bl, &decode_tag_ctx->tag,
                            decode_tag_ctx);
}

template <typename I>
void Journal<I>::flush_commit_position(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_journaler != nullptr);

  m_journaler->flush_commit_position(on_finish);
}

template <typename I>
uint64_t Journal<I>::append_write_event(uint64_t offset, size_t length,
                                        const bufferlist &bl,
                                        const AioObjectRequests &requests,
                                        bool flush_entry) {
  assert(m_image_ctx.owner_lock.is_locked());

  assert(m_max_append_size > journal::AioWriteEvent::get_fixed_size());

  uint64_t max_write_data_size =
    m_max_append_size - journal::AioWriteEvent::get_fixed_size();

  // ensure that the write event fits within the journal entry
  Bufferlists bufferlists;
  uint64_t bytes_remaining = length;
  uint64_t event_offset = 0;

  do {

    // one user io may split into multiple EventEntry and then in
    // append_io_events each EventEntry is represented by a future,
    // and multiple futures make up an Event

    uint64_t event_length = MIN(bytes_remaining, max_write_data_size);

    bufferlist event_bl;

    event_bl.substr_of(bl, event_offset, event_length);
    journal::EventEntry event_entry(journal::AioWriteEvent(offset + event_offset,
                                                           event_length,
                                                           event_bl));

    bufferlists.emplace_back();
    ::encode(event_entry, bufferlists.back());

    event_offset += event_length;
    bytes_remaining -= event_length;
  } while (bytes_remaining > 0);

  return append_io_events(journal::EVENT_TYPE_AIO_WRITE, bufferlists, requests,
                          offset, length, flush_entry);
}

template <typename I>
uint64_t Journal<I>::append_io_event(journal::EventEntry &&event_entry,
                                     const AioObjectRequests &requests,
                                     uint64_t offset, size_t length,
                                     bool flush_entry) {
  assert(m_image_ctx.owner_lock.is_locked());

  bufferlist bl;
  ::encode(event_entry, bl);
  return append_io_events(event_entry.get_event_type(), {bl}, requests, offset,
                          length, flush_entry);
}

template <typename I>
uint64_t Journal<I>::append_io_events(journal::EventType event_type,
                                      const Bufferlists &bufferlists,
                                      const AioObjectRequests &requests,
                                      uint64_t offset, size_t length,
                                      bool flush_entry) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(!bufferlists.empty());

  Futures futures;
  uint64_t tid;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_READY);

    Mutex::Locker event_locker(m_event_lock);

    tid = ++m_event_tid;
    assert(tid != 0);

    for (auto &bl : bufferlists) {

      // each buffer is an encoded librbd::journal::EventEntry

      assert(bl.length() <= m_max_append_size);

      // each buffer encoded into a journal Entry and append to the
      // journal object
      futures.push_back(m_journaler->append(m_tag_tid, bl));
    }

    m_events[tid] = Event(futures, requests, offset, length);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "event=" << event_type << ", "
                 << "new_reqs=" << requests.size() << ", "
                 << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "flush=" << flush_entry << ", tid=" << tid << dendl;

  // Journal::handle_io_event_safe
  Context *on_safe = create_async_context_callback(
    m_image_ctx, new C_IOEventSafe(this, tid));

  if (flush_entry) {
    futures.back().flush(on_safe);
  } else {
    futures.back().wait(on_safe);
  }
  return tid;
}

// called by AioCompletion::complete, which means the user io requests
// have completed
template <typename I>
void Journal<I>::commit_io_event(uint64_t tid, int r) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 "r=" << r << dendl;

  Mutex::Locker event_locker(m_event_lock);

  typename Events::iterator it = m_events.find(tid);
  if (it == m_events.end()) {
    return;
  }

  // update client commit position
  complete_event(it, r);
}

template <typename I>
void Journal<I>::commit_io_event_extent(uint64_t tid, uint64_t offset,
                                        uint64_t length, int r) {
  assert(length > 0);

  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "r=" << r << dendl;

  Mutex::Locker event_locker(m_event_lock);

  typename Events::iterator it = m_events.find(tid);
  if (it == m_events.end()) {
    return;
  }

  Event &event = it->second;
  if (event.ret_val == 0 && r < 0) {
    event.ret_val = r;
  }

  ExtentInterval extent;
  extent.insert(offset, length);

  ExtentInterval intersect;
  intersect.intersection_of(extent, event.pending_extents);

  event.pending_extents.subtract(intersect);
  if (!event.pending_extents.empty()) {
    ldout(cct, 20) << this << " " << __func__ << ": "
                   << "pending extents: " << event.pending_extents << dendl;
    return;
  }

  complete_event(it, event.ret_val);
}

template <typename I>
void Journal<I>::append_op_event(uint64_t op_tid,
                                 journal::EventEntry &&event_entry,
                                 Context *on_safe) {
  assert(m_image_ctx.owner_lock.is_locked());

  bufferlist bl;
  ::encode(event_entry, bl);

  Future future;
  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_READY);

    future = m_journaler->append(m_tag_tid, bl);

    // delay committing op event to ensure consistent replay
    assert(m_op_futures.count(op_tid) == 0);
    m_op_futures[op_tid] = future;
  }

  on_safe = create_async_context_callback(m_image_ctx, on_safe);
  on_safe = new FunctionContext([this, on_safe](int r) {
      // ensure all committed IO before this op is committed
      m_journaler->flush_commit_position(on_safe);
    });

  future.flush(on_safe);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "op_tid=" << op_tid << ", "
                 << "event=" << event_entry.get_event_type() << dendl;
}

template <typename I>
void Journal<I>::commit_op_event(uint64_t op_tid, int r, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": op_tid=" << op_tid << ", "
                 << "r=" << r << dendl;

  journal::EventEntry event_entry((journal::OpFinishEvent(op_tid, r)));

  bufferlist bl;
  ::encode(event_entry, bl);

  Future op_start_future;
  Future op_finish_future;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_READY);

    // ready to commit op event
    auto it = m_op_futures.find(op_tid);
    assert(it != m_op_futures.end());

    op_start_future = it->second;
    m_op_futures.erase(it);

    op_finish_future = m_journaler->append(m_tag_tid, bl);
  }

  op_finish_future.flush(create_async_context_callback(
    m_image_ctx, new C_OpEventSafe(this, op_tid, op_start_future,
                                   op_finish_future, on_safe)));
}

// called by Request<I>::replay_op_ready
template <typename I>
void Journal<I>::replay_op_ready(uint64_t op_tid, Context *on_resume) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": op_tid=" << op_tid << dendl;

  {
    Mutex::Locker locker(m_lock);

    assert(m_journal_replay != nullptr);
    m_journal_replay->replay_op_ready(op_tid, on_resume);
  }
}

template <typename I>
void Journal<I>::flush_event(uint64_t tid, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "on_safe=" << on_safe << dendl;

  Future future;
  {
    Mutex::Locker event_locker(m_event_lock);
    future = wait_event(m_lock, tid, on_safe);
  }

  if (future.is_valid()) {
    future.flush(nullptr);
  }
}

template <typename I>
void Journal<I>::wait_event(uint64_t tid, Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": tid=" << tid << ", "
                 << "on_safe=" << on_safe << dendl;

  Mutex::Locker event_locker(m_event_lock);
  wait_event(m_lock, tid, on_safe);
}

template <typename I>
typename Journal<I>::Future Journal<I>::wait_event(Mutex &lock, uint64_t tid,
                                                   Context *on_safe) {
  assert(m_event_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;

  typename Events::iterator it = m_events.find(tid);
  assert(it != m_events.end());

  Event &event = it->second;

  if (event.safe) {
    // journal entry already safe
    ldout(cct, 20) << this << " " << __func__ << ": "
                   << "journal entry already safe" << dendl;

    m_image_ctx.op_work_queue->queue(on_safe, event.ret_val);

    return Future();
  }

  event.on_safe_contexts.push_back(create_async_context_callback(m_image_ctx,
                                                                 on_safe));
  return event.futures.back();
}

// start_external_replay: STATE_READY -> STATE_REPLAYING
// stop_external_replay: STATE_REPLAYING -> STATE_READY

// STATE_READY -> STATE_REPLAYING

// called by ImageReplayer<I>::start_replay or ImageReplayer<I>::replay_flush
// pay attention to the difference between this and Journaler::start_replay
template <typename I>
void Journal<I>::start_external_replay(journal::Replay<I> **journal_replay,
                                       Context *on_start,
                                       Context *on_close_request) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);

  // Journal<I>::start_append transit us into STATE_READY
  assert(m_state == STATE_READY);

  assert(m_journal_replay == nullptr);
  assert(m_on_replay_close_request == nullptr);

  m_on_replay_close_request = on_close_request;

  on_start = util::create_async_context_callback(m_image_ctx, on_start);
  on_start = new FunctionContext(
    [this, journal_replay, on_start](int r) {
      handle_start_external_replay(r, journal_replay, on_start);
    });

  // safely flush all in-flight events before starting external replay
  m_journaler->stop_append(util::create_async_context_callback(m_image_ctx,
                                                               on_start));
}

template <typename I>
void Journal<I>::handle_start_external_replay(int r,
                                              journal::Replay<I> **journal_replay,
                                              Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_state == STATE_READY);
  assert(m_journal_replay == nullptr);

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to stop recording: " << cpp_strerror(r) << dendl;

    *journal_replay = nullptr;

    if (m_on_replay_close_request != nullptr) {

      // set by Journal<I>::start_external_replay

      m_on_replay_close_request->complete(r);
      m_on_replay_close_request = nullptr;
    }

    // get back to a sane-state
    // m_journaler->m_recorder has been deleted by Journaler::stop_append,
    // so we need to recreate it
    start_append();

    on_finish->complete(r);
    return;
  }

  transition_state(STATE_REPLAYING, 0);

  // the internal replay started in Journal<I>::handle_get_tags has been
  // finished, so create a new replay handler
  m_journal_replay = journal::Replay<I>::create(m_image_ctx);

  // the image replayer will need this
  *journal_replay = m_journal_replay;

  on_finish->complete(0);
}

// STATE_REPLAYING -> STATE_READY

// called by ImageReplayer<I>::replay_flush, ImageReplayer<I>::shut_down
template <typename I>
void Journal<I>::stop_external_replay() {
  Mutex::Locker locker(m_lock);

  assert(m_journal_replay != nullptr);
  assert(m_state == STATE_REPLAYING);

  if (m_on_replay_close_request != nullptr) {

    // set by Journal<I>::start_external_replay

    m_on_replay_close_request->complete(-ECANCELED);
    m_on_replay_close_request = nullptr;
  }

  delete m_journal_replay;
  m_journal_replay = nullptr;

  if (m_close_pending) {

    // set by Journal<I>::close

    destroy_journaler(0);
    return;
  }

  // call m_journaler->start_append to alloc JournalRecorder instance
  // and transit us into STATE_READY
  start_append();
}

// called by Journal<I>::open and Journal<I>::handle_journal_destroyed
template <typename I>
void Journal<I>::create_journaler() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());
  assert(m_state == STATE_UNINITIALIZED || m_state == STATE_RESTARTING_REPLAY);
  assert(m_journaler == NULL);

  transition_state(STATE_INITIALIZING, 0);
  ::journal::Settings settings;
  settings.commit_interval = m_image_ctx.journal_commit_age;
  settings.max_payload_bytes = m_image_ctx.journal_max_payload_bytes;

  m_journaler = new Journaler(m_work_queue, m_timer, m_timer_lock,
			      m_image_ctx.md_ctx, m_image_ctx.id,
			      IMAGE_CLIENT_ID, settings);
  m_journaler->init(create_async_context_callback(
    m_image_ctx, create_context_callback<
      Journal<I>, &Journal<I>::handle_initialized>(this)));
}

template <typename I>
void Journal<I>::destroy_journaler(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  assert(m_lock.is_locked());

  delete m_journal_replay;
  m_journal_replay = NULL;

  m_journaler->remove_listener(&m_metadata_listener);

  transition_state(STATE_CLOSING, r);

  // shutdown JournalTrimmer and JournalMetadata
  m_journaler->shut_down(create_async_context_callback(
    m_image_ctx, create_context_callback<
      Journal<I>, &Journal<I>::handle_journal_destroyed>(this)));
}

// called by Journal<I>::handle_flushing_restart
template <typename I>
void Journal<I>::recreate_journaler(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  assert(m_lock.is_locked());
  assert(m_state == STATE_FLUSHING_RESTART ||
         m_state == STATE_FLUSHING_REPLAY);

  delete m_journal_replay;
  m_journal_replay = NULL;

  m_journaler->remove_listener(&m_metadata_listener);

  transition_state(STATE_RESTARTING_REPLAY, r);

  // shutdown and delete the current Journaler, then create a new Journaler,
  // the state will transit into STATE_INITIALIZING first
  m_journaler->shut_down(create_async_context_callback(
    m_image_ctx, create_context_callback<
      Journal<I>, &Journal<I>::handle_journal_destroyed>(this)));
}

template <typename I>
void Journal<I>::complete_event(typename Events::iterator it, int r) {
  assert(m_event_lock.is_locked());
  assert(m_state == STATE_READY);

  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << ": tid=" << it->first << " "
                 << "r=" << r << dendl;

  Event &event = it->second;

  if (r < 0) {
    // event recorded to journal but failed to update disk, we cannot
    // commit this IO event. this event must be replayed.

    assert(event.safe);
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to commit IO to disk, replay required: "
               << cpp_strerror(r) << dendl;
  }

  event.committed_io = true;

  if (event.safe) {

    // journal safe + io requests completed

    if (r >= 0) {

      // do not update commit position if our io requests failed

      for (auto &future : event.futures) {
        m_journaler->committed(future);
      }
    }

    m_events.erase(it);
  }
}

// called by Journal<I>::stop_external_replay, Journal<I>::handle_flushing_replay
template <typename I>
void Journal<I>::start_append() {
  assert(m_lock.is_locked());

  // new JournalRecorder instance
  m_journaler->start_append(m_image_ctx.journal_object_flush_interval,
			    m_image_ctx.journal_object_flush_bytes,
			    m_image_ctx.journal_object_flush_age);

  transition_state(STATE_READY, 0);
}

template <typename I>
void Journal<I>::handle_initialized(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_INITIALIZING);

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to initialize journal: " << cpp_strerror(r)
               << dendl;

    destroy_journaler(r);
    return;
  }

  m_max_append_size = m_journaler->get_max_append_size();

  ldout(cct, 20) << this << " max_append_size=" << m_max_append_size << dendl;

  // locate the master image client record
  cls::journal::Client client;
  r = m_journaler->get_cached_client(Journal<ImageCtx>::IMAGE_CLIENT_ID,
                                     &client);
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to locate master image client" << dendl;

    destroy_journaler(r);
    return;
  }

  librbd::journal::ClientData client_data;
  bufferlist::iterator bl = client.data.begin();
  try {
    ::decode(client_data, bl);
  } catch (const buffer::error &err) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to decode client meta data: " << err.what()
               << dendl;

    destroy_journaler(-EINVAL);
    return;
  }

  journal::ImageClientMeta *image_client_meta =
    boost::get<journal::ImageClientMeta>(&client_data.client_meta);
  if (image_client_meta == nullptr) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to extract client meta data" << dendl;

    destroy_journaler(-EINVAL);
    return;
  }

  // used to create new tag, see Journal<I>::demote and Journal<I>::allocate_tag
  m_tag_class = image_client_meta->tag_class;

  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "client: " << client << ", "
                 << "image meta: " << *image_client_meta << dendl;

  C_DecodeTags *tags_ctx = new C_DecodeTags(
    cct, &m_lock, &m_tag_tid, &m_tag_data, create_async_context_callback(
      m_image_ctx, create_context_callback<
        Journal<I>, &Journal<I>::handle_get_tags>(this)));

  m_journaler->get_tags(m_tag_class, &tags_ctx->tags, tags_ctx);

  // will call journal->handle_metadata_updated if notified
  m_journaler->add_listener(&m_metadata_listener);
}

template <typename I>
void Journal<I>::handle_get_tags(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_state == STATE_INITIALIZING);

  if (r < 0) {
    destroy_journaler(r);
    return;
  }

  transition_state(STATE_REPLAYING, 0);

  m_journal_replay = journal::Replay<I>::create(m_image_ctx);

  // create an JournalPlayer instance and register a rbd replay handler
  // into it, then prefetch
  m_journaler->start_replay(&m_replay_handler);
}

// called by:
// ReplayHandler::handle_entries_available
// Journal<I>::handle_replay_process_ready
template <typename I>
void Journal<I>::handle_replay_ready() {
  CephContext *cct = m_image_ctx.cct;

  // the JournalPlayer has notified us that it has fetched a set
  // of journal objects, and we can process the journal entries now

  ReplayEntry replay_entry;

  {
    Mutex::Locker locker(m_lock);

    if (m_state != STATE_REPLAYING) {
      return;
    }

    ldout(cct, 20) << this << " " << __func__ << dendl;

    if (!m_journaler->try_pop_front(&replay_entry)) {

      // no more entries, either finished the whole replay process or
      // fetch in progress, whenever we popped one entry, we try to
      // fetch the next object if the current object is empty, see
      // remove_empty_object_player called in JournalPlayer::try_pop_front

      return;
    }

    // only one entry should be in-flight at a time
    assert(!m_processing_entry);
    m_processing_entry = true;
  }

  // process this entry, i.e., to replay this journal entry

  bufferlist data = replay_entry.get_data();
  bufferlist::iterator it = data.begin();

  journal::EventEntry event_entry;
  int r = m_journal_replay->decode(&it, &event_entry);
  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to decode journal event entry" << dendl;
    handle_replay_process_safe(replay_entry, r);
    return;
  }

  Context *on_ready = create_context_callback<
    Journal<I>, &Journal<I>::handle_replay_process_ready>(this);

  Context *on_commit = new C_ReplayProcessSafe(this, std::move(replay_entry));

  m_journal_replay->process(event_entry, on_ready, on_commit);
}

// only called by ReplayHandler::handle_complete
template <typename I>
void Journal<I>::handle_replay_complete(int r) {

  // all replay entries have been fetched and replayed, or something
  // has been failed

  CephContext *cct = m_image_ctx.cct;

  bool cancel_ops = false;

  {
    Mutex::Locker locker(m_lock);

    if (m_state != STATE_REPLAYING) {
      return;
    }

    ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

    if (r < 0) {

      // fetch and replay failed, we need to recreate the Journaler and restart
      // the whole replay process, see handle_flushing_restart

      cancel_ops = true;

      transition_state(STATE_FLUSHING_RESTART, r);
    } else {

      // currently no error occurred, but the

      // state might change back to FLUSHING_RESTART on flush error
      transition_state(STATE_FLUSHING_REPLAY, 0);
    }
  }

  Context *ctx = new FunctionContext([this, cct](int r) {
      ldout(cct, 20) << this << " handle_replay_complete: "
                     << "handle shut down replay" << dendl;

      State state;
      {
        Mutex::Locker locker(m_lock);

        assert(m_state == STATE_FLUSHING_RESTART ||
               m_state == STATE_FLUSHING_REPLAY);

        state = m_state;
      }

      if (state == STATE_FLUSHING_RESTART) {

        // the whole replay process failed, restart the replay again

        handle_flushing_restart(0);
      } else {

        // STATE_FLUSHING_REPLAY

        // replay succeeded, delete m_journal_replay and new JournalRecorder
        // then transit state to STATE_READY

        handle_flushing_replay();
      }
    });

  ctx = new FunctionContext([this, cct, cancel_ops, ctx](int r) {
      ldout(cct, 20) << this << " handle_replay_complete: "
                     << "shut down replay" << dendl;

      m_journal_replay->shut_down(cancel_ops, ctx);
    });

  // all journal entries has been replayed or some entries failed,
  // stop replay and flush or restart replay

  // JournalPlayer::shutdown and delete the JournalPlayer instance
  m_journaler->stop_replay(ctx);
}

template <typename I>
void Journal<I>::handle_replay_process_ready(int r) {
  // journal::Replay is ready for more events -- attempt to pop another
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(r == 0);

  {
    Mutex::Locker locker(m_lock);

    assert(m_processing_entry);
    m_processing_entry = false;
  }

  // try to process the next entry
  handle_replay_ready();
}

template <typename I>
void Journal<I>::handle_replay_process_safe(ReplayEntry replay_entry, int r) {
  CephContext *cct = m_image_ctx.cct;

  m_lock.Lock();

  assert(m_state == STATE_REPLAYING ||
         m_state == STATE_FLUSHING_RESTART ||
         m_state == STATE_FLUSHING_REPLAY);

  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to commit journal event to disk: " << cpp_strerror(r)
               << dendl;

    if (m_state == STATE_REPLAYING) {
      // abort the replay if we have an error
      transition_state(STATE_FLUSHING_RESTART, r);

      m_lock.Unlock();

      // stop replay, shut down, and restart
      Context *ctx = new FunctionContext([this, cct](int r) {
          ldout(cct, 20) << this << " handle_replay_process_safe: "
                         << "shut down replay" << dendl;

          {
            Mutex::Locker locker(m_lock);
            assert(m_state == STATE_FLUSHING_RESTART);
          }

          m_journal_replay->shut_down(true, create_context_callback<
            Journal<I>, &Journal<I>::handle_flushing_restart>(this));
        });

      // call JournalPlayer::shut_down to wait in-progress fetch or watch
      // to finish, see JournalPlayer::C_Fetch and JournalPlayer::C_Watch
      m_journaler->stop_replay(ctx);

      return;
    } else if (m_state == STATE_FLUSHING_REPLAY) {

      // STATE_FLUSHING_REPLAY can only be set in
      // Journal<I>::handle_replay_complete

      // end-of-replay flush in-progress -- we need to restart replay
      transition_state(STATE_FLUSHING_RESTART, r);

      m_lock.Unlock();

      return;
    }
  } else {
    // only commit the entry if written successfully
    m_journaler->committed(replay_entry);
  }

  m_lock.Unlock();
}

// called by　Journal<I>::handle_replay_complete and Journal<I>::handle_replay_process_safe
template <typename I>
void Journal<I>::handle_flushing_restart(int r) {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(r == 0);
  assert(m_state == STATE_FLUSHING_RESTART);

  if (m_close_pending) {

    // we are to close, set by Journal<I>::close

    destroy_journaler(r);
    return;
  }

  recreate_journaler(r);
}

// called by　Journal<I>::handle_replay_complete
template <typename I>
void Journal<I>::handle_flushing_replay() {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_state == STATE_FLUSHING_REPLAY || m_state == STATE_FLUSHING_RESTART);

  if (m_close_pending) {

    // we are to close, set by Journal<I>::close

    // will shutdown Journaler and delete it, see Journal::handle_journal_destroyed
    destroy_journaler(0);

    return;
  } else if (m_state == STATE_FLUSHING_RESTART) {

    // failed to replay one-or-more events -- restart

    // transit into STATE_RESTARTING_REPLAY
    recreate_journaler(0);

    return;
  }


  delete m_journal_replay;
  m_journal_replay = NULL;

  m_error_result = 0;

  // transit into STATE_READY
  start_append();
}

template <typename I>
void Journal<I>::handle_recording_stopped(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  assert(m_state == STATE_STOPPING);

  // shutdown Journaler, i.e., m_journaler which created in Journal<I>::open
  destroy_journaler(r);
}

template <typename I>
void Journal<I>::handle_journal_destroyed(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << this << " " << __func__
               << "error detected while closing journal: " << cpp_strerror(r)
               << dendl;
  }

  Mutex::Locker locker(m_lock);

  delete m_journaler;
  m_journaler = nullptr;

  assert(m_state == STATE_CLOSING || m_state == STATE_RESTARTING_REPLAY);

  if (m_state == STATE_RESTARTING_REPLAY) {

    // create a new Journaler

    create_journaler();
    return;
  }

  transition_state(STATE_CLOSED, r);
}

// user io Event write to journal object returned
template <typename I>
void Journal<I>::handle_io_event_safe(int r, uint64_t tid) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << ", "
                 << "tid=" << tid << dendl;

  // journal will be flushed before closing
  assert(m_state == STATE_READY || m_state == STATE_STOPPING);

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to commit IO event: "  << cpp_strerror(r) << dendl;
  }

  AioObjectRequests aio_object_requests;
  Contexts on_safe_contexts;

  {
    Mutex::Locker event_locker(m_event_lock);

    typename Events::iterator it = m_events.find(tid);
    assert(it != m_events.end());

    Event &event = it->second;

    aio_object_requests.swap(event.aio_object_requests);
    on_safe_contexts.swap(event.on_safe_contexts);

    if (r < 0 || event.committed_io) {

      // journal safe + io requests completed

      // failed journal write so IO won't be sent -- or IO extent was
      // overwritten by future IO operations so this was a no-op IO event
      event.ret_val = r;

      for (auto &future : event.futures) {
        m_journaler->committed(future);
      }
    }

    if (event.committed_io) {
      m_events.erase(it);
    } else {
      event.safe = true;
    }
  }

  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "completing tid=" << tid << dendl;

  for (AioObjectRequests::iterator it = aio_object_requests.begin();
       it != aio_object_requests.end(); ++it) {
    if (r < 0) {
      // don't send aio requests if the journal fails -- bubble error up
      (*it)->complete(r);
    } else {
      // send any waiting aio requests now that journal entry is safe
      RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

      (*it)->send();
    }
  }

  // alert the cache about the journal event status
  for (Contexts::iterator it = on_safe_contexts.begin();
       it != on_safe_contexts.end(); ++it) {

    (*it)->complete(r);
  }
}

template <typename I>
void Journal<I>::handle_op_event_safe(int r, uint64_t tid,
                                      const Future &op_start_future,
                                      const Future &op_finish_future,
                                      Context *on_safe) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << ", "
                 << "tid=" << tid << dendl;

  // journal will be flushed before closing
  assert(m_state == STATE_READY || m_state == STATE_STOPPING);

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to commit op event: "  << cpp_strerror(r) << dendl;
  }

  m_journaler->committed(op_start_future);
  m_journaler->committed(op_finish_future);

  // reduce the replay window after committing an op event
  m_journaler->flush_commit_position(on_safe);
}

// called by Journal<I>::close
template <typename I>
void Journal<I>::stop_recording() {
  assert(m_lock.is_locked());
  assert(m_journaler != NULL);

  assert(m_state == STATE_READY);

  transition_state(STATE_STOPPING, 0);

  // flush pending appends and delete the JournalRecorder
  m_journaler->stop_append(util::create_async_context_callback(
    m_image_ctx, create_context_callback<
      Journal<I>, &Journal<I>::handle_recording_stopped>(this)));
}

template <typename I>
void Journal<I>::transition_state(State state, int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": new state=" << state << dendl;

  assert(m_lock.is_locked());

  m_state = state;

  if (m_error_result == 0 && r < 0) {
    m_error_result = r;
  }

  if (is_steady_state()) {
    Contexts wait_for_state_contexts(std::move(m_wait_for_state_contexts));

    for (auto ctx : wait_for_state_contexts) {
      ctx->complete(m_error_result);
    }
  }
}

template <typename I>
bool Journal<I>::is_steady_state() const {
  assert(m_lock.is_locked());
  switch (m_state) {
  case STATE_READY:
  case STATE_CLOSED:
    return true;
  case STATE_UNINITIALIZED:
  case STATE_INITIALIZING:
  case STATE_REPLAYING:
  case STATE_FLUSHING_RESTART:
  case STATE_RESTARTING_REPLAY:
  case STATE_FLUSHING_REPLAY:
  case STATE_STOPPING:
  case STATE_CLOSING:
    break;
  }
  return false;
}

template <typename I>
void Journal<I>::wait_for_steady_state(Context *on_state) {
  assert(m_lock.is_locked());
  assert(!is_steady_state());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": on_state=" << on_state
                 << dendl;

  m_wait_for_state_contexts.push_back(on_state);
}

template <typename I>
int Journal<I>::check_resync_requested(bool *do_resync) {
  Mutex::Locker l(m_lock);
  return check_resync_requested_internal(do_resync);
}

template <typename I>
int Journal<I>::check_resync_requested_internal(bool *do_resync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  assert(m_lock.is_locked());
  assert(do_resync != nullptr);

  cls::journal::Client client;
  int r = m_journaler->get_cached_client(IMAGE_CLIENT_ID, &client);
  if (r < 0) {
     lderr(cct) << this << " " << __func__ << ": "
                << "failed to retrieve client: " << cpp_strerror(r) << dendl;
     return r;
  }

  librbd::journal::ClientData client_data;
  bufferlist::iterator bl_it = client.data.begin();
  try {
    ::decode(client_data, bl_it);
  } catch (const buffer::error &err) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to decode client data: " << err << dendl;
    return -EINVAL;
  }

  journal::ImageClientMeta *image_client_meta =
    boost::get<journal::ImageClientMeta>(&client_data.client_meta);
  if (image_client_meta == nullptr) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to access image client meta struct" << dendl;
    return -EINVAL;
  }

  *do_resync = image_client_meta->resync_requested;

  return 0;
}

// called by MetadataListener::handle_update
template <typename I>
void Journal<I>::handle_metadata_updated() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  std::list<journal::ResyncListener *> resync_private_list;

  {
    Mutex::Locker l(m_lock);

    if (m_state == STATE_CLOSING || m_state == STATE_CLOSED ||
        m_state == STATE_UNINITIALIZED || m_state == STATE_STOPPING) {
      return;
    }

    bool do_resync = false;
    int r = check_resync_requested_internal(&do_resync);
    if (r < 0) {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to check if a resync was requested" << dendl;
      return;
    }

    if (do_resync) {

      // map<journal::ListenerType, list<journal::JournalListenerPtr> >
      // currently we only have one type, i.e., RESYNC
      for (const auto& listener :
                              m_listener_map[journal::ListenerType::RESYNC]) {
        journal::ResyncListener *rsync_listener =
                        boost::get<journal::ResyncListener *>(listener);

        resync_private_list.push_back(rsync_listener);
      }
    }
  }

  for (const auto& listener : resync_private_list) {
    listener->handle_resync();
  }
}

// called by ImageReplayer<I>::handle_bootstrap
template <typename I>
void Journal<I>::add_listener(journal::ListenerType type,
                              journal::JournalListenerPtr listener) {
  Mutex::Locker l(m_lock);
  m_listener_map[type].push_back(listener);
}

template <typename I>
void Journal<I>::remove_listener(journal::ListenerType type,
                                 journal::JournalListenerPtr listener) {
  Mutex::Locker l(m_lock);
  m_listener_map[type].remove(listener);
}

} // namespace librbd

template class librbd::Journal<librbd::ImageCtx>;
