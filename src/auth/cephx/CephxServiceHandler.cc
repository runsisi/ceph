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
#include "CephxKeyServer.h"
#include <errno.h>
#include <sstream>

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx server " << entity_name << ": "

// called by
// AuthMonitor::prep_auth
int CephxServiceHandler::start_session(EntityName& name, bufferlist::iterator& indata, bufferlist& result_bl, AuthCapsInfo& caps)
{
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
      // get client's key
      if (!key_server->get_secret(entity_name, secret)) {
        ldout(cct, 0) << "couldn't find entity name: " << entity_name << dendl;
	ret = -EPERM;
	break;
      }

      if (!server_challenge) {
	ret = -EPERM;
	break;
      }      

      uint64_t expected_key;
      std::string error;
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
      // client's name was got by CephxServiceHandler::start_session
      if (! key_server->get_auth(entity_name, eauth)) {
	ret = -EPERM;
	break;
      }

      CephXServiceTicketInfo old_ticket_info;

      if (cephx_decode_ticket(cct, key_server, CEPH_ENTITY_TYPE_AUTH,
			      req.old_ticket, old_ticket_info)) {

        // get rotating key for the old ticket, then use the key to decrypt
        // CephXServiceTicketInfo from the old ticket

        global_id = old_ticket_info.ticket.global_id;

        ldout(cct, 10) << "decoded old_ticket with global_id=" << global_id << dendl;

        // should encrypt the new ticket using the old ticket
        should_enc_ticket = true;
      }

      info.ticket.init_timestamps(ceph_clock_now(), cct->_conf->auth_mon_ticket_ttl);
      info.ticket.name = entity_name;
      info.ticket.global_id = global_id;
      info.ticket.auid = eauth.auid;
      info.validity += cct->_conf->auth_mon_ticket_ttl;

      if (auid) *auid = eauth.auid;

      key_server->generate_secret(session_key);

      // session key, will be used for encrypting later communication
      info.session_key = session_key;
      info.service_id = CEPH_ENTITY_TYPE_AUTH;

      // get a rotating key and its id for CEPH_ENTITY_TYPE_AUTH
      if (!key_server->get_service_secret(CEPH_ENTITY_TYPE_AUTH, info.service_secret, info.secret_id)) {
        ldout(cct, 0) << " could not get service secret for auth subsystem" << dendl;
        ret = -EIO;
        break;
      }

      vector<CephXSessionAuthInfo> info_vec;
      info_vec.push_back(info);

      build_cephx_response_header(cephx_header.request_type, 0, result_bl);

      // encrypt msg_a using client's key and encrypt CephXServiceTicketInfo, i.e.,
      // ticket using rotating key of CEPH_ENTITY_TYPE_AUTH
      if (!cephx_build_service_ticket_reply(cct, eauth.key, info_vec, should_enc_ticket,
					    old_ticket_info.session_key, result_bl)) {
	ret = -EIO;
      }

      if (!key_server->get_service_caps(entity_name, CEPH_ENTITY_TYPE_MON, caps)) {
        ldout(cct, 0) << " could not get mon caps for " << entity_name << dendl;
        ret = -EACCES;
      } else {
        char *caps_str = caps.caps.c_str();
        if (!caps_str || !caps_str[0]) {
          ldout(cct,0) << "mon caps null for " << entity_name << dendl;
          ret = -EACCES;
        }
      }
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    {
      ldout(cct, 10) << "handle_request get_principal_session_key" << dendl;

      bufferlist tmp_bl;
      CephXServiceTicketInfo auth_ticket_info;
      // get rotating key for CEPH_ENTITY_TYPE_AUTH to decrypt the ticket info for
      // CEPH_ENTITY_TYPE_AUTH, during this process will verify if the ticket is valid
      if (!cephx_verify_authorizer(cct, key_server, indata, auth_ticket_info, tmp_bl)) {
        ret = -EPERM;
	break;
      }

      CephXServiceTicketRequest ticket_req;
      // i.e., AuthClientHandler::need
      ::decode(ticket_req, indata);

      ldout(cct, 10) << " ticket_req.keys = " << ticket_req.keys << dendl;

      ret = 0;
      vector<CephXSessionAuthInfo> info_vec;
      int found_services = 0;
      int service_err = 0;

      for (uint32_t service_id = 1; service_id <= ticket_req.keys;
	   service_id <<= 1) {
        if (ticket_req.keys & service_id) {
	  ldout(cct, 10) << " adding key for service "
			 << ceph_entity_type_name(service_id) << dendl;

	  CephXSessionAuthInfo info;
	  // build CephXSessionAuthInfo for later building tickets for services
          int r = key_server->build_session_auth_info(service_id,
						      auth_ticket_info, info);
	  // tolerate missing MGR rotating key for the purposes of upgrades.
          if (r < 0) {
	    ldout(cct, 10) << "   missing key for service "
			   << ceph_entity_type_name(service_id) << dendl;
	    service_err = r;
	    continue;
	  }

          // default 60*60
          info.validity += cct->_conf->auth_service_ticket_ttl;

          info_vec.push_back(info);

	  ++found_services;
        }
      }

      if (!found_services && service_err) {
	ldout(cct, 10) << __func__ << " did not find any service keys" << dendl;
	ret = service_err;
      }

      CryptoKey no_key;
      build_cephx_response_header(cephx_header.request_type, ret, result_bl);

      // encrypt msg_a using client's key and encrypt CephXServiceTicketInfo, i.e.,
      // ticket using rotating key of each service
      cephx_build_service_ticket_reply(cct, auth_ticket_info.session_key, info_vec, false, no_key, result_bl);
    }
    break;

  case CEPHX_GET_ROTATING_KEY:
    {
      ldout(cct, 10) << "handle_request getting rotating secret for " << entity_name << dendl;

      build_cephx_response_header(cephx_header.request_type, 0, result_bl);

      // encrypt rotating keys using client's key
      if (!key_server->get_rotating_encrypted(entity_name, result_bl)) {
        ret = -EPERM;
        break;
      }
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
