/*
 * list_noent.cc
 *
 *  Created on: May 17, 2017
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

int main(int argc, const char **argv) {
  int r = 0;
  Rados client;
  client.init2("client.admin", "ceph", 0);
  client.conf_read_file(nullptr);
  client.conf_parse_argv(argc, argv);

  client.connect();

  IoCtx ioctx;
  r = client.ioctx_create("p1", ioctx);
  cout << "rados.ioctx_create: r = " << r << endl;
  if (r < 0)
    return -1;

  RBD rbd_api;

  // list image
  vector<string> names;
  r = rbd_api.list(ioctx, names);
  cout << "rbd.list: r = " << r << endl;
  for (auto &i : names) {
    cout << i << endl;
  }

  // list trash
  vector<trash_image_info_t> entries;
  r = rbd_api.trash_list(ioctx, entries);
  cout << "rbd.trash_list: r = " << r << endl;
  for (auto &i : entries) {
    cout << i.name << endl;
  }

  // list group
  vector<string> groups;
  r = rbd_api.group_list(ioctx, &groups);
  cout << "rbd.group_list: r = " << r << endl;
  for (auto &i : groups) {
    cout << i << endl;
  }

  Image image;
  const char *image_name = "i1";
  r = rbd_api.open(ioctx, image, image_name);
  cout << "rbd.open: r = " << r << endl;
  if (r < 0)
    return -1;

  // list children, TODO: must be snapshot
  set<pair<string, string> > children;
  r = image.list_children(&children);
  cout << "rbd.list_children: r = " << r << endl;
  for (auto &i : children) {
    cout << i.first << ", " << i.second << endl;
  }


  // list snap
  vector<librbd::snap_info_t> snaps;
  r = image.snap_list(snaps);
  cout << "rbd.snap_list: r = " << r << endl;
  for (auto &i : snaps) {
    cout << i.name << endl;
  }

  // list lock owner
  list<librbd::locker_t> lockers;
  bool exclusive;
  string tag;
  r = image.list_lockers(&lockers, &exclusive, &tag);
  cout << "rbd.list_lockers: r = " << r << endl;
  for (auto &i : lockers) {
    cout << i.address << endl;
  }

  return 0;
}


