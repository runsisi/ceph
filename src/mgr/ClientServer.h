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

#ifndef CEPH_MGR_CLIENT_SERVER_H_
#define CEPH_MGR_CLIENT_SERVER_H_

#include <set>
#include <string>

#include "common/Mutex.h"

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "ClientState.h"

class MMgrReport;
class MMgrOpen;
class MCommand;

class ClientServer : public Dispatcher
{
public:
  ClientServer(Messenger *msgr, MonClient *monc, ClientStateIndex &client_state);
  ~ClientServer() override;

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  bool handle_open(MMgrOpen *m);
  bool handle_report(MMgrReport *m);
  bool handle_command(MCommand *m);

private:
  Messenger *msgr_;
  MonClient *monc_;
  ClientStateIndex &client_state_;

  Mutex lock_;
};

#endif // CEPH_MGR_CLIENT_SERVER_H_

