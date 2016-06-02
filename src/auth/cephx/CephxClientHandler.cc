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


#include <errno.h>

#include "CephxClientHandler.h"
#include "CephxProtocol.h"

#include "auth/KeyRing.h"
#include "common/config.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx client: "

// called by
// MonClient::handle_auth
// MonClient::_check_auth_tickets
int CephxClientHandler::build_request(bufferlist& bl) const
{
  ldout(cct, 10) << "build_request" << dendl;

  RWLock::RLocker l(lock);

  if (need & CEPH_ENTITY_TYPE_AUTH) {
    /* authenticate */
    CephXRequestHeader header;
    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;
    ::encode(header, bl);

    CryptoKey secret;
    const bool got = keyring->get_secret(cct->_conf->name, secret);
    if (!got) {
      ldout(cct, 20) << "no secret found for entity: " << cct->_conf->name << dendl;
      return -ENOENT;
    }

    // is the key OK?
    if (!secret.get_secret().length()) {
      ldout(cct, 20) << "secret for entity " << cct->_conf->name << " is invalid" << dendl;
      return -EINVAL;
    }

    CephXAuthenticate req;
    get_random_bytes((char *)&req.client_challenge, sizeof(req.client_challenge));
    std::string error;

    // server_challenge was got by CephxClientHandler::handle_response for the
    // first response
    cephx_calc_client_server_challenge(cct, secret, server_challenge,
				       req.client_challenge, &req.key, error);
    if (!error.empty()) {
      ldout(cct, 20) << "cephx_calc_client_server_challenge error: " << error << dendl;
      return -EIO;
    }

    req.old_ticket = ticket_handler->ticket;

    if (req.old_ticket.blob.length()) {
      ldout(cct, 20) << "old ticket len=" << req.old_ticket.blob.length() << dendl;
    }

    ::encode(req, bl);

    ldout(cct, 10) << "get auth session key: client_challenge "
		   << hex << req.client_challenge << dendl;
    return 0;
  }

  if (_need_tickets()) {
    /* get service tickets */
    ldout(cct, 10) << "get service keys: want=" << want << " need=" << need << " have=" << have << dendl;

    CephXRequestHeader header;
    header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;
    ::encode(header, bl);

    // encode service id, i.e., CEPH_ENTITY_TYPE_AUTH, ticket, etc., into CephXAuthorizer::bl
    CephXAuthorizer *authorizer = ticket_handler->build_authorizer(global_id);
    if (!authorizer)
      return -EINVAL;

    bl.claim_append(authorizer->bl);
    delete authorizer;

    CephXServiceTicketRequest req;
    req.keys = need;

    ::encode(req, bl);
  }

  return 0;
}

bool CephxClientHandler::_need_tickets() const
{
  // do not bother (re)requesting tickets if we *only* need the MGR
  // ticket; that can happen during an upgrade and we want to avoid a
  // loop.  we'll end up re-requesting it later when the secrets
  // rotating.
  return need && need != CEPH_ENTITY_TYPE_MGR;
}

int CephxClientHandler::handle_response(int ret, bufferlist::iterator& indata)
{
  ldout(cct, 10) << "handle_response ret = " << ret << dendl;

  RWLock::WLocker l(lock);
  
  if (ret < 0)
    return ret; // hrm!

  if (starting) {

    // was set by CephxClientHandler::reset

    CephXServerChallenge ch;
    ::decode(ch, indata);

    server_challenge = ch.server_challenge;
    ldout(cct, 10) << " got initial server challenge "
		   << hex << server_challenge << dendl;
    starting = false;

    tickets.invalidate_ticket(CEPH_ENTITY_TYPE_AUTH);

    // will request CEPHX_GET_AUTH_SESSION_KEY immediately, see MonClient::handle_auth

    return -EAGAIN;
  }

  struct CephXResponseHeader header;
  ::decode(header, indata);

  switch (header.request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      ldout(cct, 10) << " get_auth_session_key" << dendl;

      CryptoKey secret;
      const bool got = keyring->get_secret(cct->_conf->name, secret);
      if (!got) {
	ldout(cct, 0) << "key not found for " << cct->_conf->name << dendl;
	return -ENOENT;
      }
	
      // get session key and ticket for CEPH_ENTITY_TYPE_AUTH
      if (!tickets.verify_service_ticket_reply(secret, indata)) {
	ldout(cct, 0) << "could not verify service_ticket reply" << dendl;
	return -EPERM;
      }

      ldout(cct, 10) << " want=" << want << " need=" << need << " have=" << have << dendl;

      validate_tickets();

      // still need other tickets for other services
      if (_need_tickets())
        // will request CEPHX_GET_PRINCIPAL_SESSION_KEY immediately, see MonClient::handle_auth
	ret = -EAGAIN;
      else
	ret = 0;
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    {
      CephXTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);

      ldout(cct, 10) << " get_principal_session_key session_key " << ticket_handler.session_key << dendl;
  
      // decrypt msg_a using client's key and decode/decrypt tickets using old session keys
      // for all requested services except CEPH_ENTITY_TYPE_AUTH which has been got during
      // handling CEPHX_GET_AUTH_SESSION_KEY
      if (!tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        ldout(cct, 0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }

      validate_tickets();

      if (!_need_tickets()) {
	ret = 0;
      }
    }
    break;

  case CEPHX_GET_ROTATING_KEY:
    // the request was built by CephxClientHandler::build_rotating_request which
    // was called by MonClient::_check_auth_rotating, only OSD/MDS/MGR needs this
    {
      ldout(cct, 10) << " get_rotating_key" << dendl;

      if (rotating_secrets) {

        // was allocated by MonClient::init, should never be null

	RotatingSecrets secrets;
	CryptoKey secret_key;

	const bool got = keyring->get_secret(cct->_conf->name, secret_key);
        if (!got) {
          ldout(cct, 0) << "key not found for " << cct->_conf->name << dendl;
          return -ENOENT;
        }

        std::string error;
        // decrypt rotating keys using client's key
	if (decode_decrypt(cct, secrets, secret_key, indata, error)) {
	  ldout(cct, 0) << "could not set rotating key: decode_decrypt failed. error:"
	    << error << dendl;
	  return -EINVAL;
	} else {
	  // RotatingKeyRing *rotating_secrets was allocated by MonClient::init
	  rotating_secrets->set_secrets(secrets);
	}
      }
    }
    break;

  default:
   ldout(cct, 0) << " unknown request_type " << header.request_type << dendl;
   ceph_abort();
  }

  return ret;
}


// called by
// msgr->get_authorizer eventually, see Pipe::connect
AuthAuthorizer *CephxClientHandler::build_authorizer(uint32_t service_id) const
{
  RWLock::RLocker l(lock);

  ldout(cct, 10) << "build_authorizer for service " << ceph_entity_type_name(service_id) << dendl;

  // build a CephXAuthorizer instance,
  // CephXAuthorizer = <session_key, nonce, bl>, bl = <global_id, service_id, ticket, encrypted msg>
  return tickets.build_authorizer(service_id);
}

// called by
// MonClient::_check_auth_rotating
bool CephxClientHandler::build_rotating_request(bufferlist& bl) const
{
  ldout(cct, 10) << "build_rotating_request" << dendl;

  CephXRequestHeader header;
  header.request_type = CEPHX_GET_ROTATING_KEY;

  ::encode(header, bl);

  return true;
}

// called by
// MonClient::handle_auth
// MonClient::_check_auth_tickets
void CephxClientHandler::prepare_build_request()
{
  RWLock::WLocker l(lock);

  ldout(cct, 10) << "validate_tickets: want=" << want << " need=" << need
		 << " have=" << have << dendl;

  // to set AuthClientHandler::have, need
  validate_tickets();

  ldout(cct, 10) << "want=" << want << " need=" << need << " have=" << have
		 << dendl;

  // CephXTicketHandler
  ticket_handler = &(tickets.get_handler(CEPH_ENTITY_TYPE_AUTH));
}

// called by
// CephxClientHandler::handle_response
// CephxClientHandler::prepare_build_request
// CephxClientHandler::need_tickets
// AuthClientHandler::set_want_keys
void CephxClientHandler::validate_tickets()
{
  // want is a mask initialized at the very beginning by MonClient::set_want_keys,
  // and should not be changed later, except for CEPH_ENTITY_TYPE_MGR which requires
  // kraken features, see MonClient::handle_auth
  // lock should be held for write
  tickets.validate_tickets(want, have, need);
}

// called by
// MonClient::_check_auth_tickets
bool CephxClientHandler::need_tickets()
{
  RWLock::WLocker l(lock);

  // set AuthClientHandler::need, have
  validate_tickets();

  ldout(cct, 20) << "need_tickets: want=" << want
		 << " have=" << have
		 << " need=" << need
		 << dendl;

  // AuthClientHandler::need != 0
  return _need_tickets();
}

