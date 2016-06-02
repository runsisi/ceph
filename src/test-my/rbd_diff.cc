/*
 * rbd_diff.cc
 *
 *  Created on: Nov 1, 2017
 *      Author: runsisi
 */

#include "rados/librados.hpp"
#include "rbd/librbd.hpp"
#include <string>
#include <vector>
#include <iostream>

using namespace std;
using namespace librados;
using namespace librbd;

static int callback(uint64_t offset, size_t len, int exists,
                    void *arg) {
  cout << "offset: " << offset << ", len: " << len << ", exists: " << exists
       << endl;

  return 0;
}

int main(int argc, const char **argv) {
  int r = 0;
  Rados client;
  client.init2("client.admin", "ceph", 0);
  client.conf_read_file(nullptr);
  client.conf_parse_argv(argc, argv);

  client.connect();

  IoCtx ioctx;
  r = client.ioctx_create("test001", ioctx);
  cout << "rados.ioctx_create: r = " << r << endl;
  if (r < 0)
    return -1;

  RBD rbd;
  Image image;
  rbd.open(ioctx, image, "i1");

  string data(4096, 'a');
  bufferlist bl;
  bl.append(data);

  r = image.write(0, 4096, bl);
  cout << "image.write: r= " << r << endl;

  r = image.write(4 * 1024 * 1024 - 1024, 4096, bl);
  cout << "image.write: r= " << r << endl;

  image.diff_iterate(0, 0, 200 * 1024 * 1024, callback, nullptr);

  return 0;
}
