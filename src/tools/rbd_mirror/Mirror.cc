// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/range/adaptor/map.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "Mirror.h"
#include "Threads.h"
#include "ImageSync.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Mirror: " << this << " " \
                           << __func__ << ": "

using std::list;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;
using librbd::mirror_peer_t;

namespace rbd {
namespace mirror {

namespace {

class MirrorAdminSocketCommand {
public:
  virtual ~MirrorAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public MirrorAdminSocketCommand {
public:
  explicit StatusCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->print_status(f, ss);
    return true;
  }

private:
  Mirror *mirror;
};

class StartCommand : public MirrorAdminSocketCommand {
public:
  explicit StartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->start();
    return true;
  }

private:
  Mirror *mirror;
};

class StopCommand : public MirrorAdminSocketCommand {
public:
  explicit StopCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->stop();
    return true;
  }

private:
  Mirror *mirror;
};

class RestartCommand : public MirrorAdminSocketCommand {
public:
  explicit RestartCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->restart();
    return true;
  }

private:
  Mirror *mirror;
};

class FlushCommand : public MirrorAdminSocketCommand {
public:
  explicit FlushCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->flush();
    return true;
  }

private:
  Mirror *mirror;
};

class LeaderReleaseCommand : public MirrorAdminSocketCommand {
public:
  explicit LeaderReleaseCommand(Mirror *mirror) : mirror(mirror) {}

  bool call(Formatter *f, stringstream *ss) override {
    mirror->release_leader();
    return true;
  }

private:
  Mirror *mirror;
};

} // anonymous namespace

// created by
// Mirror::Mirror
class MirrorAdminSocketHook : public AdminSocketHook {
public:
  MirrorAdminSocketHook(CephContext *cct, Mirror *mirror) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status";
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror");
    if (r == 0) {
      commands[command] = new StatusCommand(mirror);
    }

    command = "rbd mirror start";
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror");
    if (r == 0) {
      commands[command] = new StartCommand(mirror);
    }

    command = "rbd mirror stop";
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror");
    if (r == 0) {
      commands[command] = new StopCommand(mirror);
    }

    command = "rbd mirror restart";
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror");
    if (r == 0) {
      commands[command] = new RestartCommand(mirror);
    }

    command = "rbd mirror flush";
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror");
    if (r == 0) {
      commands[command] = new FlushCommand(mirror);
    }

    command = "rbd mirror leader release";
    r = admin_socket->register_command(command, command, this,
				       "release rbd mirror leader");
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand(mirror);
    }
  }

  ~MirrorAdminSocketHook() override {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) override {
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
  typedef std::map<std::string, MirrorAdminSocketCommand*> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

// created by
// src/tools/rbd_mirror/main.cc/main
Mirror::Mirror(CephContext *cct, const std::vector<const char*> &args) :
  m_cct(cct),
  m_args(args),
  m_lock("rbd::mirror::Mirror"),
  m_local(new librados::Rados()),
  m_asok_hook(new MirrorAdminSocketHook(cct, this))
{
  cct->lookup_or_create_singleton_object<Threads>(m_threads,
                                                  "rbd_mirror::threads");
}

Mirror::~Mirror()
{
  delete m_asok_hook;
}

// called by
// rbd_mirror/main.cc:handle_signal
void Mirror::handle_signal(int signum)
{
  m_stopping.set(1);

  {
    Mutex::Locker l(m_lock);

    // Mirror::run is waiting for this to stop
    m_cond.Signal();
  }
}

// called by
// rbd_mirror/main.cc:main
int Mirror::init()
{
  int r = m_local->init_with_context(m_cct);
  if (r < 0) {
    derr << "could not initialize rados handle" << dendl;
    return r;
  }

  // connect to local cluster to get local <pool, peer> pairs
  r = m_local->connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    return r;
  }

  m_local_cluster_watcher.reset(new ClusterWatcher(m_local, m_lock));

  m_image_deleter.reset(new ImageDeleter(m_threads->work_queue,
                                         m_threads->timer,
                                         &m_threads->timer_lock));

  m_image_sync_throttler.reset(new ImageSyncThrottler<>());

  return r;
}

// called by
// rbd_mirror/main.cc:main, this is running under the main thread of the rbd-mirror process
void Mirror::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping.read()) {

    // list local pools and get their corresponding mirroring peers
    m_local_cluster_watcher->refresh_pools();

    Mutex::Locker l(m_lock);

    // m_manual_stop was/will be set by Mirror::stop, if we are told to stop
    // manually, we will not stop the running thread, instead we skip
    // to update, i.e., delete stale replayers and create replayers for newly
    // added peers, replayers and let Mirror::stop to stop all replayers
    if (!m_manual_stop) {
      // each pool can have a list of peers, i.e., std::map<int64_t, Peers>

      // keep a Replayer instance running for each <pool, peer_t> pair, and
      // delete the replayer if 1) peer is blacklisted, or 2) the local pool removed,
      // or 3) no peers associated with the local pool, or 4) the peer has removed
      // for the local pool
      update_replayers(m_local_cluster_watcher->get_pool_peers());
    }

    m_cond.WaitInterval(
      m_lock,
      utime_t(m_cct->_conf->rbd_mirror_pool_replayers_refresh_interval, 0));
  }

  // we are to stop, stop all replayers in parallel
  Mutex::Locker locker(m_lock);

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->stop(false);
  }

  dout(20) << "return" << dendl;
}

// called by
// StatusCommand::call
void Mirror::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  if (f) {
    f->open_object_section("mirror_status");
    f->open_array_section("replayers");
  };

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->print_status(f, ss);
  }

  if (f) {
    f->close_section();
    f->open_object_section("image_deleter");
  }

  m_image_deleter->print_status(f, ss);

  if (f) {
    f->close_section();
    f->open_object_section("sync_throttler");
  }

  m_image_sync_throttler->print_status(f, ss);

  if (f) {
    f->close_section();
    f->close_section();
    f->flush(*ss);
  }
}

// called by
// StartCommand::call
void Mirror::start()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->start();
  }
}

// called by
// StopCommand::call
void Mirror::stop()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = true;

  // admin op, stop all replayers
  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->stop(true);
  }
}

// called by
// RestartCommand::call
void Mirror::restart()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_manual_stop = false;

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->restart();
  }
}

// called by
// FlushCommand::call
void Mirror::flush()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read() || m_manual_stop) {
    return;
  }

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;

    replayer->flush();
  }
}

// called by
// Mirror.cc/LeaderReleaseCommand::call
void Mirror::release_leader()
{
  dout(20) << "enter" << dendl;
  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  for (auto it = m_replayers.begin(); it != m_replayers.end(); it++) {
    auto &replayer = it->second;
    replayer->release_leader();
  }
}

// called by
// Mirror::run
// std::map<int64_t, Peers>, i.e., std::map<int64_t, std::set<peer_t>>,
// each pool can have a set of peers, and each peer is a tuple of <uuid, cluster name, client name>
void Mirror::update_replayers(const PoolPeers &pool_peers)
{
  dout(20) << "enter" << dendl;

  assert(m_lock.is_locked());

  // remove stale replayers before creating new replayers
  for (auto it = m_replayers.begin(); it != m_replayers.end();) {

    // iterate std::map<PoolPeer, std::unique_ptr<Replayer> >

    // peer_t
    auto &peer = it->first.second;

    // check if the local pool still exists or no peers associated with it
    auto pool_peer_it = pool_peers.find(it->first.first);

    // check if we need to stop this replayer or not, erase a Replayer
    // from m_replayers will delete it, then Replayer::~Replayer will stop
    // the replayer, i.e., set Replayer::m_stopping, and wait its thread to stop
    if (it->second->is_blacklisted()) {

      // PoolWatcher::refresh returned -EBLACKLISTED

      derr << "removing blacklisted replayer for " << peer << dendl;

      // TODO: make async

      it = m_replayers.erase(it);
    } else if (pool_peer_it == pool_peers.end() ||
               pool_peer_it->second.find(peer) == pool_peer_it->second.end()) {

      // local pool has been removed or the peer of the local pool has been removed

      dout(20) << "removing replayer for " << peer << dendl;

      // TODO: make async

      it = m_replayers.erase(it);
    } else {

      // this replayer can keep its way

      ++it;
    }
  }

  // we have deleted the stale replayers, now create the new replayer
  // instances for newly added peers

  for (auto &kv : pool_peers) {

    // iterate map<pool id, set<peer_t>>, i.e., existing local pools

    for (auto &peer : kv.second) {

      // iterate set<peer_t>, i.e., peers of local pool

      // <pool id, peer_t>, each <pool id, peer_t> pair is associated
      // with a Replayer instance
      PoolPeer pool_peer(kv.first, peer);

      if (m_replayers.find(pool_peer) == m_replayers.end()) {

        // if the replayer for the corresponding <pool, peer_t> does not
        // exist, so create it

        dout(20) << "starting replayer for " << peer << dendl;

        // <pool, peer> -> Replayer
        unique_ptr<Replayer> replayer(new Replayer(m_threads, m_image_deleter,
                                                   m_image_sync_throttler,
                                                   kv.first, peer, m_args));
        // TODO: make async, and retry connecting within replayer

        // 1) setup pool watcher, 2) setup periodic timer to drive pool watcher to
        // refresh local pool images, 3) create replayer thread
        int r = replayer->init();
        if (r < 0) {
	  continue;
        }

        m_replayers.insert(std::make_pair(pool_peer, std::move(replayer)));
      }
    }
  }
}

} // namespace mirror
} // namespace rbd
