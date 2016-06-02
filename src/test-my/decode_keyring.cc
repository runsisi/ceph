/*
 * decode_keyring.cc
 *
 *  Created on: Aug 26, 2017
 *      Author: runsisi
 */

#include "Crypto.h"
#include <string>
#include "include/msgr.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
# include <nspr/nspr.h>
# include <nss/nss.h>
# include <nss/pk11pub.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <iostream>
using namespace std;

int main() {
  std::string key = "AQBe4aBZAAAAABAA7TrWxpnV4UxmHrm+FLRqeg==";

  CephContext *cct1 = new CephContext(CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_LIBRARY, 0);
  //cct1->init_crypto();

  NSSInitParameters init_params;
  memset(&init_params, 0, sizeof(init_params));
  init_params.length = sizeof(init_params);
  uint32_t flags = (NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
  flags |= (NSS_INIT_NOCERTDB | NSS_INIT_NOMODDB);
  NSSInitContext *crypto_context = NULL;
  crypto_context = NSS_InitContext("", "", "", SECMOD_DB, &init_params, flags);
  //NSS_Initialize("", "", "", SECMOD_DB, flags);

  cout << "parent fork" << endl;
  if (fork() == 0) {
    CephContext *cct2 = new CephContext(CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_LIBRARY, 0);
    cct2->init_crypto();

    cout << "child decoding" << endl;
    CryptoKey ckey;
    ckey.decode_base64(key);
    cout << "child sleeping" << endl;
    sleep(1);
    cout << "child exit" << endl;
  } else {
    cout << "parent waiting for child" << endl;
    int status;
    wait(&status);
    cout << "parent exit" << endl;
  }

  return 0;
}

// build command line:
// g++ -o decode decode.cc -I/usr/include/nss -I/usr/include/nspr
//      -I../auth -I.. -I../../build/include -I../include  -L../../build/lib
//      -std=c++11 -lceph-common -lnspr4 -lnss3 -ggdb3
