// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "global/global_context.h"
#include "librbd/ObjectWatcher.h"
#include "librbd/internal.h"
#include "Replayer.h"
#include "Threads.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Replayer: " \
                           << this << " " << __func__ << ": "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

using librbd::cls_client::dir_get_name;

namespace rbd {
namespace mirror {

namespace {

// status, start, stop, restart, flush

class ReplayerAdminSocketCommand {
public:
  virtual ~ReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public ReplayerAdminSocketCommand {
public:
  explicit StatusCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->print_status(f, ss);
    return true;
  }

private:
  Replayer *replayer;
};

class StartCommand : public ReplayerAdminSocketCommand {
public:
  explicit StartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->start();
    return true;
  }

private:
  Replayer *replayer;
};

class StopCommand : public ReplayerAdminSocketCommand {
public:
  explicit StopCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->stop(true);
    return true;
  }

private:
  Replayer *replayer;
};

class RestartCommand : public ReplayerAdminSocketCommand {
public:
  explicit RestartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->restart();
    return true;
  }

private:
  Replayer *replayer;
};

class FlushCommand : public ReplayerAdminSocketCommand {
public:
  explicit FlushCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) {
    replayer->flush();
    return true;
  }

private:
  Replayer *replayer;
};

} // anonymous namespace

class ReplayerAdminSocketHook : public AdminSocketHook {
public:
  ReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			  Replayer *replayer) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + name;
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand(replayer);
    }

    command = "rbd mirror start " + name;
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StartCommand(replayer);
    }

    command = "rbd mirror stop " + name;
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StopCommand(replayer);
    }

    command = "rbd mirror restart " + name;
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror " + name);
    if (r == 0) {
      commands[command] = new RestartCommand(replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand(replayer);
    }
  }

  ~ReplayerAdminSocketHook() {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);

      delete i->second;
    }
  }

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    Commands::const_iterator i = commands.find(command);
    assert(i != commands.end());

    Formatter *f = Formatter::create(format);
    stringstream ss;

    bool r = i->second->call(f, &ss);

    delete f;
    out.append(ss);

    return r;
  }

private:
  typedef std::map<std::string, ReplayerAdminSocketCommand*> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

// member variable of Replayer, i.e.,
// std::unique_ptr<MirrorStatusWatchCtx> Replayer::m_status_watcher, which
// will be initialized by Replayer::mirror_image_status_init
class MirrorStatusWatchCtx {
public:
  MirrorStatusWatchCtx(librados::IoCtx &ioctx, ContextWQ *work_queue) {
    m_ioctx.dup(ioctx);
    m_watcher = new Watcher(m_ioctx, work_queue);
  }

  ~MirrorStatusWatchCtx() {
    delete m_watcher;
  }

  // called by
  // Replayer::mirror_image_status_init
  int register_watch() {
    C_SaferCond cond;
    m_watcher->register_watch(&cond);
    return cond.wait();
  }

  // called by
  // Replayer::mirror_image_status_shut_down
  int unregister_watch() {
    C_SaferCond cond;
    m_watcher->unregister_watch(&cond);
    return cond.wait();
  }

  std::string get_oid() const {
    return m_watcher->get_oid();
  }

private:
  class Watcher : public librbd::ObjectWatcher<> {
  public:
    Watcher(librados::IoCtx &ioctx, ContextWQ *work_queue) :
      ObjectWatcher<>(ioctx, work_queue) {
    }

    // pure virtual
    virtual std::string get_oid() const {
      return RBD_MIRRORING;
    }

    // pure virtual, so apparently we do nothing for this watch
    virtual void handle_notify(uint64_t notify_id, uint64_t handle,
			       bufferlist &bl) {
      bufferlist out;

      // ObjectWatcher<I>::acknowledge_notify, i.e., m_io_ctx.notify_ack, nothing special
      acknowledge_notify(notify_id, handle, out);
    }
  };

  librados::IoCtx m_ioctx;
  // will be allocated by MirrorStatusWatchCtx::MirrorStatusWatchCtx
  Watcher *m_watcher;
};

// will be created by Mirror::update_replayers for each <pool, peer> pair
Replayer::Replayer(Threads *threads, std::shared_ptr<ImageDeleter> image_deleter,
                   ImageSyncThrottlerRef<> image_sync_throttler,
                   int64_t local_pool_id, const peer_t &peer,
                   const std::vector<const char*> &args) :
  m_threads(threads),
  m_image_deleter(image_deleter),
  m_image_sync_throttler(image_sync_throttler),
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_args(args),
  m_local_pool_id(local_pool_id),
  m_asok_hook(nullptr),
  m_replayer_thread(this)
{
}

// replayer erased from Mirror::m_replayers will delete the Replayer
// instance thus stop the thread, see Mirror::update_replayers
Replayer::~Replayer()
{
  delete m_asok_hook;

  // will stop Replayer::run
  m_stopping.set(1);

  {
    Mutex::Locker l(m_lock);

    m_cond.Signal();
  }

  if (m_replayer_thread.is_started()) {
    m_replayer_thread.join();
  }
}

// called by
// Mirror::update_replayers
bool Replayer::is_blacklisted() const {
  Mutex::Locker locker(m_lock);

  // was set by Replayer::run
  return m_blacklisted;
}

// called by
// Mirror::update_replayers to start a new Replayer instance
int Replayer::init()
{
  dout(20) << "replaying for " << m_peer << dendl;

  // connect to local cluster
  int r = init_rados(g_ceph_context->_conf->cluster,
                     g_ceph_context->_conf->name.to_str(),
                     "local cluster", &m_local_rados);
  if (r < 0) {
    return r;
  }

  // connect to peer cluster
  r = init_rados(m_peer.cluster_name, m_peer.client_name,
                 std::string("remote peer ") + stringify(m_peer),
                 &m_remote_rados);
  if (r < 0) {
    return r;
  }

  r = m_local_rados->ioctx_create2(m_local_pool_id, m_local_io_ctx);
  if (r < 0) {
    derr << "error accessing local pool " << m_local_pool_id << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote_rados->ioctx_create(m_local_io_ctx.get_pool_name().c_str(),
                                   m_remote_io_ctx);
  if (r < 0) {
    derr << "error accessing remote pool " << m_local_io_ctx.get_pool_name()
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  m_remote_pool_id = m_remote_io_ctx.get_id();

  dout(20) << "connected to " << m_peer << dendl;

  // Bootstrap existing mirroring images
  init_local_mirroring_images();

  // NOTE: the PoolWatcher for remote peer is driven by timer

  m_pool_watcher.reset(new PoolWatcher(m_remote_io_ctx,
		       g_ceph_context->_conf->rbd_mirror_image_directory_refresh_interval,
		       m_lock, m_cond));

  // start periodic timer to get mirroring enabled images from peer, i.e.,
  // pool at remote cluster, and signal the replayer thread
  m_pool_watcher->refresh_images();

  // run Replayer::run
  m_replayer_thread.create("replayer");

  return 0;
}

// called by
// Replayer::init
int Replayer::init_rados(const std::string &cluster_name,
                         const std::string &client_name,
                         const std::string &description, RadosRef *rados_ref) {
  rados_ref->reset(new librados::Rados());

  // NOTE: manually bootstrap a CephContext here instead of via
  // the librados API to avoid mixing global singletons between
  // the librados shared library and the daemon
  // TODO: eliminate intermingling of global singletons within Ceph APIs
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);

  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << "error initializing cluster handle for " << description << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  cct->_conf->cluster = cluster_name;

  // librados::Rados::conf_read_file
  int r = cct->_conf->parse_config_files(nullptr, nullptr, 0);
  if (r < 0) {
    derr << "could not read ceph conf for " << description << ": "
	 << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }

  cct->_conf->parse_env();

  // librados::Rados::conf_parse_env
  std::vector<const char*> args;
  env_to_vec(args, nullptr);

  r = cct->_conf->parse_argv(args);
  if (r < 0) {
    derr << "could not parse environment for " << description << ":"
         << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }

  if (!m_args.empty()) {
    // librados::Rados::conf_parse_argv
    args = m_args;
    r = cct->_conf->parse_argv(args);
    if (r < 0) {
      derr << "could not parse command line args for " << description << ": "
	   << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  // disable unnecessary librbd cache
  cct->_conf->set_val_or_die("rbd_cache", "false");
  cct->_conf->apply_changes(nullptr);
  cct->_conf->complain_about_parse_errors(cct);

  r = (*rados_ref)->init_with_context(cct);
  assert(r == 0);

  cct->put();

  r = (*rados_ref)->connect();
  if (r < 0) {
    derr << "error connecting to " << description << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

// called by
// Replayer::init
void Replayer::init_local_mirroring_images() {
  rbd_mirror_mode_t mirror_mode;

  // PoolWatcher::refresh also check the mirroring mode of the pool
  int r = librbd::mirror_mode_get(m_local_io_ctx, &mirror_mode);
  if (r < 0) {
    derr << "could not tell whether mirroring was enabled for "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }

  if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
    dout(20) << "pool " << m_local_io_ctx.get_pool_name() << " "
             << "has mirroring disabled" << dendl;
    return;
  }

  std::set<InitImageInfo> images;

  std::string last_read = "";
  int max_read = 1024;
  do {

    // get all mirror enabled images of local pool
    // <image id, image global id>
    std::map<std::string, std::string> mirror_images;

    r = librbd::cls_client::mirror_image_list(&m_local_io_ctx, last_read,
                                              max_read, &mirror_images);
    if (r < 0) {
      derr << "error listing mirrored image directory: "
           << cpp_strerror(r) << dendl;
      continue;
    }

    for (auto it = mirror_images.begin(); it != mirror_images.end(); ++it) {

      // iterate mirror enabled images to get their names

      std::string image_name;

      // get image name by image id
      r = dir_get_name(&m_local_io_ctx, RBD_DIRECTORY, it->first, &image_name);
      if (r < 0) {
        derr << "error retrieving local image name: " << cpp_strerror(r)
             << dendl;
        continue;
      }

      // <image global id, image id, image name>
      images.insert(InitImageInfo(it->second, it->first, image_name));
    }

    if (!mirror_images.empty()) {
      last_read = mirror_images.rbegin()->first;
    }

    r = mirror_images.size();
  } while (r == max_read);

  // those images are mirror enabled of the local pool, including primary or secondary images
  m_init_images = std::move(images);
}

// called by
// Replayer::ReplayerThread::entry
void Replayer::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping.read()) {

    std::string asok_hook_name = m_local_io_ctx.get_pool_name() + " " +
                                 m_peer.cluster_name;

    if (m_asok_hook_name != asok_hook_name || m_asok_hook == nullptr) {
      m_asok_hook_name = asok_hook_name;

      delete m_asok_hook;

      // the commands are registered in the ctor
      m_asok_hook = new ReplayerAdminSocketHook(g_ceph_context,
                                                m_asok_hook_name, this);
    }

    Mutex::Locker locker(m_lock);

    if (m_pool_watcher->is_blacklisted()) {

      // stop Replayer thread for this <pool, peer> pair

      m_blacklisted = true;

      // stop the running thread, the Replayer instance will be deleted
      // by Mirror::update_replayers when it finds the replayer was blacklisted
      m_stopping.set(1);
    } else if (!m_manual_stop) {

      // start ImageReplayer to mirror those newly mirror enabled images of
      // the remote pool, and stop those disabled

      set_sources(m_pool_watcher->get_images());
    }

    // NOTE: manual stop, i.e., stop requested from admin socket, will not
    // stop the running thread, it only stops the imageplayers

    if (m_blacklisted) {
      break;
    }

    // will be signalled by Replayer::~Replayer, Replayer::stop and
    // PoolWatcher::refresh_images
    m_cond.WaitInterval(g_ceph_context, m_lock,
	utime_t(g_ceph_context->_conf->rbd_mirror_image_state_check_interval, 0));
  } // keep running

  // the Replayer is to stop, so stop all its ImageReplayer

  ImageIds empty_sources;

  while (true) {
    Mutex::Locker l(m_lock);

    set_sources(empty_sources);

    if (m_image_replayers.empty()) {
      break;
    }

    m_cond.WaitInterval(g_ceph_context, m_lock, seconds(1));
  }
}

// called by
// StatusCommand::call
void Replayer::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (f) {
    f->open_object_section("replayer_status");
    f->dump_string("pool", m_local_io_ctx.get_pool_name());
    f->dump_stream("peer") << m_peer;
    f->open_array_section("image_replayers");
  };

  // map<image id, image replayer>
  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;

    image_replayer->print_status(f, ss);
  }

  if (f) {
    f->close_section();
    f->close_section();
    f->flush(*ss);
  }
}

// called by
// StartCommand::call
// Mirror::start
void Replayer::start()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;

    // bootstrap and loop (replaying + replaying flush)
    image_replayer->start(nullptr, true);
  }
}

// called by
// StopCommand::call, manual is true
// Mirror::stop, manual is true
// Mirror::run, manual is false, i.e., the process is to be stopped
void Replayer::stop(bool manual)
{
  dout(20) << "enter: manual=" << manual << dendl;

  Mutex::Locker l(m_lock);

  if (!manual) {

    // process is to be stopped

    m_stopping.set(1);
    m_cond.Signal();

    return;
  } else if (m_stopping.read()) {

    // already stopped or in progress of stop

    return;
  }

  // admin socket requested stop, do not stop the running thread

  m_manual_stop = true;

  // admin op, stop all image replayers
  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;

    // stop an individual image replayer, this is a manually stop op
    image_replayer->stop(nullptr, true);
  }
}

// called by
// RestartCommand::call
void Replayer::restart()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;

    image_replayer->restart();
  }
}

// called by
// FlushCommand::call
void Replayer::flush()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read() || m_manual_stop) {
    return;
  }

  for (auto &kv : m_image_replayers) {
    auto &image_replayer = kv.second;

    image_replayer->flush();
  }
}

// called by
// Replayer::run
void Replayer::set_sources(const ImageIds &image_ids)
{
  dout(20) << "enter" << dendl;

  assert(m_lock.is_locked());

  // those local mirroring enabled images were got by
  // Replayer::init_local_mirroring_images which was called by Replayer::init
  if (!m_init_images.empty()) {

    // all local mirroring enabled images

    dout(20) << "scanning initial local image set" << dendl;

    // <image id, image name, image global id>
    for (auto &remote_image : image_ids) {

      // iterate remote mirroring enabled images

      auto it = m_init_images.find(InitImageInfo(remote_image.global_id));

      if (it != m_init_images.end()) {

        // this local mirroring enabled image only has a global id, no local id, no name,
        // so it is not a full blown mirroring enabled image, remove it from the initial images

        m_init_images.erase(it);
      }
    }

    // the remaining images in m_init_images must be deleted, except primary images
    for (auto &image : m_init_images) {
      dout(20) << "scheduling the deletion of init image: "
               << image.name << dendl;

      // those primary images in this pool will be ignored, see
      // ImageDeleter::process_image_delete
      m_image_deleter->schedule_image_delete(m_local_rados, m_local_pool_id,
                                             image.id, image.name,
                                             image.global_id);
    }

    // so those local mirror enabled images will only be deleted once
    m_init_images.clear();
  }

  // to stop stale image replayers and start new image replayers

  bool existing_image_replayers = !m_image_replayers.empty();

  // std::map<std::string, std::unique_ptr<ImageReplayer<> > >
  for (auto image_it = m_image_replayers.begin();
       image_it != m_image_replayers.end();) {

    if (image_ids.find(ImageId(image_it->first)) == image_ids.end()) {

      // the remote image has mirror disabled or deleted, so the image
      // replayer has to be stopped too

      if (image_it->second->is_running()) {
        dout(20) << "stop image replayer for "
                 << image_it->second->get_global_image_id() << dendl;
      }

      if (stop_image_replayer(image_it->second)) {
        image_it = m_image_replayers.erase(image_it);

        continue;
      }
    }

    // this image replayer should keep running
    ++image_it;
  }

  if (image_ids.empty()) {

    // no remote mirroring enabled images

    if (existing_image_replayers && m_image_replayers.empty()) {

      // all existing replayers are being shutdown, so shutdown the
      // image mirror status

      mirror_image_status_shut_down();
    }

    return;
  }

  // ok, we may have newly added remote mirroring enabled images to mirror

  // get local client id as a mirror peer
  std::string local_mirror_uuid;
  int r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                              &local_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve local mirror uuid from pool "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }

  // get remote client id as a mirror peer
  std::string remote_mirror_uuid;
  r = librbd::cls_client::mirror_uuid_get(&m_remote_io_ctx,
                                          &remote_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve remote mirror uuid from pool "
         << m_remote_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return;
  }

  if (m_image_replayers.empty() && !existing_image_replayers) {

    // this is the first time we start the mirror process for the remote pool

    // create entry for pool if it doesn't exist
    r = mirror_image_status_init();
    if (r < 0) {
      return;
    }
  }

  // set<image id, image name, image global id>
  for (auto &image_id : image_ids) {

    // iterate remote mirroring enabled images to see if we need to mirror any new image

    auto it = m_image_replayers.find(image_id.id);

    if (it == m_image_replayers.end()) {

      // to mirror a new image

      unique_ptr<ImageReplayer<> > image_replayer(new ImageReplayer<>(
        m_threads, m_image_deleter, m_image_sync_throttler, m_local_rados,
        m_remote_rados, local_mirror_uuid, remote_mirror_uuid, m_local_pool_id,
        m_remote_pool_id, image_id.id, image_id.global_id));

      it = m_image_replayers.insert(
        std::make_pair(image_id.id, std::move(image_replayer))).first;
    }

    if (!it->second->is_running()) {
      dout(20) << "starting image replayer for "
               << it->second->get_global_image_id() << dendl;
    }

    start_image_replayer(it->second, image_id.id, image_id.name);
  }
}

// called by
// Replayer::set_sources
int Replayer::mirror_image_status_init() {
  assert(!m_status_watcher);

  uint64_t instance_id = librados::Rados(m_local_io_ctx).get_instance_id();

  dout(20) << "pool_id=" << m_local_pool_id << ", "
           << "instance_id=" << instance_id << dendl;

  librados::ObjectWriteOperation op;

  // remove those mirror image status keys whose rbd-mirror daemon
  // has shutdown/disconnected, see image_status_remove_down
  librbd::cls_client::mirror_image_status_remove_down(&op);
  int r = m_local_io_ctx.operate(RBD_MIRRORING, &op);
  if (r < 0) {
    derr << "error initializing " << RBD_MIRRORING << "object: "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  unique_ptr<MirrorStatusWatchCtx> watch_ctx(
    new MirrorStatusWatchCtx(m_local_io_ctx, m_threads->work_queue));

  // watch RBD_MIRRORING object
  r = watch_ctx->register_watch();
  if (r < 0) {
    derr << "error registering watcher for " << watch_ctx->get_oid()
	 << " object: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_status_watcher = std::move(watch_ctx);

  return 0;
}

// called by
// Replayer::set_sources
void Replayer::mirror_image_status_shut_down() {
  assert(m_status_watcher);

  int r = m_status_watcher->unregister_watch();
  if (r < 0) {
    derr << "error unregistering watcher for " << m_status_watcher->get_oid()
	 << " object: " << cpp_strerror(r) << dendl;
  }

  // std::unique_ptr<MirrorStatusWatchCtx>
  m_status_watcher.reset();
}

// called by
// Replayer::set_sources
void Replayer::start_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer,
                                    const std::string &image_id,
                                    const boost::optional<std::string>& image_name)
{
  assert(m_lock.is_locked());

  dout(20) << "global_image_id=" << image_replayer->get_global_image_id()
           << dendl;

  if (!image_replayer->is_stopped()) {
    // the image replayer has not stopped yet

    return;
  } else if (image_replayer->is_blacklisted()) {
    derr << "blacklisted detected during image replay" << dendl;

    m_blacklisted = true;

    m_stopping.set(1);

    return;
  }

  // now m_state == STATE_STOPPED, try to start the replay for this remote image

  if (image_name) {

    // only mirror remote image that has a name

    FunctionContext *ctx = new FunctionContext(
        [this, image_id, image_name] (int r) {
          if (r == -ESTALE || r == -ECANCELED) {
            return;
          }

          Mutex::Locker locker(m_lock);

          // std::map<std::string, std::unique_ptr<ImageReplayer<> > >, key is remote image id
          auto it = m_image_replayers.find(image_id);
          if (it == m_image_replayers.end()) {
            return;
          }

          auto &image_replayer = it->second;
          if (r >= 0) {
            image_replayer->start();
          } else {
            start_image_replayer(image_replayer, image_id, image_name);
          }
       }
    );

    m_image_deleter->wait_for_scheduled_deletion(
      m_local_pool_id, image_replayer->get_global_image_id(), ctx, false);
  }
}

// called by
// Replayer::set_sources
bool Replayer::stop_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer)
{
  assert(m_lock.is_locked());

  dout(20) << "global_image_id=" << image_replayer->get_global_image_id()
           << dendl;

  // TODO: check how long it is stopping and alert if it is too long.
  if (image_replayer->is_stopped()) {
    m_image_deleter->cancel_waiter(m_local_pool_id,
                                   image_replayer->get_global_image_id());

    if (!m_stopping.read()) {
      dout(20) << "scheduling delete" << dendl;

      m_image_deleter->schedule_image_delete(
        m_local_rados,
        image_replayer->get_local_pool_id(),
        image_replayer->get_local_image_id(),
        image_replayer->get_local_image_name(),
        image_replayer->get_global_image_id());
    }

    return true;
  } else {
    if (!m_stopping.read()) {
      dout(20) << "scheduling delete after image replayer stopped" << dendl;
    }

    FunctionContext *ctx = new FunctionContext(
        [&image_replayer, this] (int r) {
          if (!m_stopping.read() && r >= 0) {
            m_image_deleter->schedule_image_delete(
              m_local_rados,
              image_replayer->get_local_pool_id(),
              image_replayer->get_local_image_id(),
              image_replayer->get_local_image_name(),
              image_replayer->get_global_image_id());
          }
        }
    );

    // stop the image replayer first, then delete the mirrored image
    // in its callback
    image_replayer->stop(ctx);
  }

  return false;
}

} // namespace mirror
} // namespace rbd
