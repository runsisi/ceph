// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "ClientServer.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr::ClientServer: " << __func__ << " "

ClientServer::ClientServer(Messenger *msgr, MonClient *monc,
  ClientStateIndex &client_state)
    : Dispatcher(g_ceph_context), msgr_(msgr), monc_(monc),
      client_state_(client_state),
      lock("ClientServer")
{
  msgr->add_dispatcher_head(this);
}

ClientServer::~ClientServer() {
}

bool ClientServer::ms_handle_reset(Connection *con)
{
  dout(0) << __func__ << " " << con << dendl;
  return false;
}

bool ClientServer::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

bool ClientServer::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock_);

  switch(m->get_type()) {
    case MSG_MGR_REPORT:
      return handle_report(static_cast<MMgrReport*>(m));
    case MSG_MGR_OPEN:
      return handle_open(static_cast<MMgrOpen*>(m));
    case MSG_COMMAND:
      return handle_command(static_cast<MCommand*>(m));
    default:
      dout(10) << "Unhandled message type " << m->get_type() << dendl;
      return false;
  };
}

bool ClientServer::handle_open(MMgrOpen *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  auto configure = new MMgrConfigure();
  configure->stats_period = g_conf->mgr_stats_period;
  m->get_connection()->send_message(configure);

  if (client_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    client_state.get(key)->perf_counters.clear();
  }

  m->put();
  return true;
}

bool ClientServer::handle_report(MMgrReport *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  DaemonStatePtr daemon;
  if (client_state.exists(key)) {
    dout(20) << "updating existing DaemonState for " << m->daemon_name << dendl;
    daemon = client_state.get(key);
  } else {
    dout(4) << "constructing new DaemonState for " << m->daemon_name << dendl;
    daemon = std::make_shared<DaemonState>(client_state.types);
    // FIXME: crap, we don't know the hostname at this stage.
    daemon->key = key;
    client_state.insert(daemon);
    // FIXME: we should request metadata at this stage
  }

  assert(daemon != nullptr);
  auto &daemon_counters = daemon->perf_counters;
  daemon_counters.update(m);
  
  m->put();
  return true;
}

struct MgrCommand {
  string cmdstring;
  string helpstring;
  string module;
  string perm;
  string availability;
} mgr_commands[] = {

#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability},

COMMAND("foo " \
	"name=bar,type=CephString", \
	"do a thing", "mgr", "rw", "cli")
};

bool ClientServer::handle_command(MCommand *m)
{
  int r = 0;
  std::stringstream ss;
  std::stringstream ds;
  bufferlist odata;
  std::string prefix;

  assert(lock.is_locked_by_me());

  cmdmap_t cmdmap;

  ConnectionRef con = m->get_connection();

  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -EINVAL;
    goto out;
  }

  dout(4) << "decoded " << cmdmap.size() << dendl;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  dout(4) << "prefix=" << prefix << dendl;

  if (prefix == "get_command_descriptions") {
    return false;
  } else {
    // Let's find you a handler!
    MgrPyModule *handler = nullptr;
    auto py_commands = py_modules.get_commands();
    for (const auto &pyc : py_commands) {
      auto pyc_prefix = cmddesc_get_prefix(pyc.cmdstring);
      dout(1) << "pyc_prefix: '" << pyc_prefix << "'" << dendl;
      if (pyc_prefix == prefix) {
        handler = pyc.handler;
        break;
      }
    }

    if (handler == nullptr) {
      ss << "No handler found for '" << prefix << "'";
      dout(4) << "No handler found for '" << prefix << "'" << dendl;
      r = -EINVAL;
      goto out;
    }

    // FIXME: go run this python part in another thread, not inline
    // with a ms_dispatch, so that the python part can block if it
    // wants to.
    dout(4) << "passing through " << cmdmap.size() << dendl;
    r = handler->handle_command(cmdmap, &ds, &ss);
    goto out;
  }

 out:

  // Let the connection drop as soon as we've sent our response
  if (m->get_connection()) {
    m->get_connection()->mark_disposable();
  }

  std::string rs;
  rs = ss.str();
  odata.append(ds);
  dout(1) << "do_command r=" << r << " " << rs << dendl;
  if (con) {
    MCommandReply *reply = new MCommandReply(r, rs);
    reply->set_tid(m->get_tid());
    reply->set_data(odata);
    con->send_message(reply);
  }

  m->put();
  return true;
}

