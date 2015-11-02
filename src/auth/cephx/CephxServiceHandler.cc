// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "CephxServiceHandler.h"
#include "CephxProtocol.h"

#include "../Auth.h"

#include "mon/Monitor.h"

#include <errno.h>
#include <sstream>

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx server " << entity_name << ": "

int CephxServiceHandler::start_session(EntityName& name, bufferlist::iterator& indata, bufferlist& result_bl, AuthCapsInfo& caps)
{
  // note down peer's entity name
  entity_name = name;

  get_random_bytes((char *)&server_challenge, sizeof(server_challenge));
  if (!server_challenge)
    server_challenge = 1;  // always non-zero.
  ldout(cct, 10) << "start_session server_challenge " << hex << server_challenge << dec << dendl;

  CephXServerChallenge ch;
  ch.server_challenge = server_challenge;
  ::encode(ch, result_bl);
  return CEPH_AUTH_CEPHX;
}

int CephxServiceHandler::handle_request(bufferlist::iterator& indata, bufferlist& result_bl, uint64_t& global_id, AuthCapsInfo& caps, uint64_t *auid)
{
  int ret = 0;

  struct CephXRequestHeader cephx_header;
  ::decode(cephx_header, indata);


  switch (cephx_header.request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      ldout(cct, 10) << "handle_request get_auth_session_key for " << entity_name << dendl;

      CephXAuthenticate req;
      ::decode(req, indata);

      CryptoKey secret;
      // apparently, mon must know peer's key
      if (!key_server->get_secret(entity_name, secret)) {
        ldout(cct, 0) << "couldn't find entity name: " << entity_name << dendl;
	ret = -EPERM;
	break;
      }

      // we already generate this in auth_handler->start_session for every session
      if (!server_challenge) {
	ret = -EPERM;
	break;
      }      

      uint64_t expected_key;
      std::string error;
      // use peer's key to encrypt challenges and compress to a 64 bits integer
      cephx_calc_client_server_challenge(cct, secret, server_challenge,
					 req.client_challenge, &expected_key, error);
      if (!error.empty()) {
	ldout(cct, 0) << " cephx_calc_client_server_challenge error: " << error << dendl;
	ret = -EPERM;
	break;
      }

      ldout(cct, 20) << " checking key: req.key=" << hex << req.key
	       << " expected_key=" << expected_key << dec << dendl;
      if (req.key != expected_key) {
        ldout(cct, 0) << " unexpected key: req.key=" << hex << req.key
		<< " expected_key=" << expected_key << dec << dendl;
        ret = -EPERM;
	break;
      }

      CryptoKey session_key;
      CephXSessionAuthInfo info;
      bool should_enc_ticket = false;

      EntityAuth eauth;
      // get peer's EntityAuth, key_server will get all auth info from
      // mon's db in Monitor::preinit -> Monitor::init_paxos 
      // -> Monitor::refresh_from_paxos -> PaxosService::refresh 
      // -> AuthMonitor::update_from_paxos
      if (! key_server->get_auth(entity_name, eauth)) {
	ret = -EPERM;
	break;
      }
      CephXServiceTicketInfo old_ticket_info;

      // peer may carry an old ticket, decrypt old_ticket_info from req.old_ticket.blob
      // KeyStore is the base class of KeyServer
      if (cephx_decode_ticket(cct, key_server, CEPH_ENTITY_TYPE_AUTH,
			      req.old_ticket, old_ticket_info)) {
        global_id = old_ticket_info.ticket.global_id;
        ldout(cct, 10) << "decoded old_ticket with global_id=" << global_id << dendl;
        should_enc_ticket = true;
      }

      // setup CephXSessionAuthInfo.ticket (type of struct AuthTicket)
      info.ticket.init_timestamps(ceph_clock_now(cct), cct->_conf->auth_mon_ticket_ttl);
      info.ticket.name = entity_name;
      info.ticket.global_id = global_id;
      info.ticket.auid = eauth.auid;
      info.validity += cct->_conf->auth_mon_ticket_ttl;

      if (auid) *auid = eauth.auid;

      // generate a new session key
      key_server->generate_secret(session_key);

      info.session_key = session_key;   // this session key will send to peer
      info.service_id = CEPH_ENTITY_TYPE_AUTH;
      // get service key and its id (actually secret's version) of the service
      // type CEPH_ENTITY_TYPE_AUTH, KeyServer::_check_rotating_secrets will update
      // those service keys
      if (!key_server->get_service_secret(CEPH_ENTITY_TYPE_AUTH, info.service_secret, info.secret_id)) {
        ldout(cct, 0) << " could not get service secret for auth subsystem" << dendl;
        ret = -EIO;
        break;
      }

      vector<CephXSessionAuthInfo> info_vec;
      info_vec.push_back(info); // one ticket to generate

      // generate a CephXResponseHeader
      build_cephx_response_header(cephx_header.request_type, 0, result_bl);

      // build a vector of (CephXServiceTicket(encrypted by eauth.key) 
      // + CephXTicketBlob(encrypted by service key, if old ticket is carried, then
      // encrypted by old_ticket_info.session_key again))
      if (!cephx_build_service_ticket_reply(cct, eauth.key, info_vec, should_enc_ticket,
					    old_ticket_info.session_key, result_bl)) {
	ret = -EIO;
      }

      if (!key_server->get_service_caps(entity_name, CEPH_ENTITY_TYPE_MON, caps)) {
        ldout(cct, 0) << " could not get mon caps for " << entity_name << dendl;
      }
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    {
      ldout(cct, 10) << "handle_request get_principal_session_key" << dendl;

      bufferlist tmp_bl;

      /* 
        CephXServiceTicketInfo contains:
                AuthTicket ticket;
                CryptoKey session_key;
                
        AuthTicket is like a certification.
        AuthTicket contains:
                EntityName name;
                uint64_t global_id;
                uint64_t auid;
                utime_t created, renew_after, expires;
                AuthCapsInfo caps;
                __u32 flags;
                
        CephXServiceTicketInfo is encrypted to a CephXTicketBlob
        CephXTicketBlob contains:
                uint64_t secret_id;
                bufferlist blob;
      */
      CephXServiceTicketInfo auth_ticket_info;
      // verify if the peer send us the valid ticket (we can found the service key
      // the peer specified in the request, and we can use the found service key
      // to decrypt the encrypted ticket, then with the session key in the ticket,
      // we can decrypt the CephXAuthorize)
      if (!cephx_verify_authorizer(cct, key_server, indata, auth_ticket_info, tmp_bl)) {
        ret = -EPERM;
	break;
      }

      CephXServiceTicketRequest ticket_req;
      ::decode(ticket_req, indata);

      // ticket_req.keys is set to "need" in CephxClientHandler
      ldout(cct, 10) << " ticket_req.keys = " << ticket_req.keys << dendl;

      ret = 0;
      vector<CephXSessionAuthInfo> info_vec;
      for (uint32_t service_id = 1; service_id <= ticket_req.keys; service_id <<= 1) {
        if (ticket_req.keys & service_id) {
          // we need to get service key of this service
	  ldout(cct, 10) << " adding key for service " << ceph_entity_type_name(service_id) << dendl;
          CephXSessionAuthInfo info;
          // construct a CephXSessionAuthInfo instance for use in cephx_build_service_ticket_reply
          // auth_ticket_info is decrypted from peer's authorizer
          int r = key_server->build_session_auth_info(service_id, auth_ticket_info, info);
          if (r < 0) {
            ret = r;
            break;
          }
          info.validity += cct->_conf->auth_service_ticket_ttl;
          info_vec.push_back(info);
        }
      }
      CryptoKey no_key;
      build_cephx_response_header(cephx_header.request_type, ret, result_bl);
      // build a vector of (CephXServerTicket + CephXTicketBlob) from info_vec
      cephx_build_service_ticket_reply(cct, auth_ticket_info.session_key, info_vec, false, no_key, result_bl);
    }
    break;

  case CEPHX_GET_ROTATING_KEY:
    {
      ldout(cct, 10) << "handle_request getting rotating secret for " << entity_name << dendl;
      build_cephx_response_header(cephx_header.request_type, 0, result_bl);
      key_server->get_rotating_encrypted(entity_name, result_bl);
      ret = 0;
    }
    break;

  default:
    ldout(cct, 10) << "handle_request unknown op " << cephx_header.request_type << dendl;
    return -EINVAL;
  }
  return ret;
}

void CephxServiceHandler::build_cephx_response_header(int request_type, int status, bufferlist& bl)
{
  struct CephXResponseHeader header;
  header.request_type = request_type;
  header.status = status;
  ::encode(header, bl);
}
