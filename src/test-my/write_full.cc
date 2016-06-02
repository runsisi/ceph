/*
 * write_full.cc
 *
 *  Created on: May 10, 2017
 *      Author: runsisi
 */

#include "rados/librados.hpp"
#include "rados/buffer.h"
#include <string>
#include <vector>

using namespace std;
using namespace librados;

int main() {
    Rados client;
    client.init2("client.admin", "ceph", 0);
    client.conf_read_file(nullptr);

    client.connect();

    IoCtx ioctx;
    client.ioctx_create("rbd", ioctx);

    string data1(4096, 'a');
    bufferlist bl1;
    bl1.append(data1);
    ioctx.write("xxx_oid", bl1, bl1.length(), 0);

    uint64_t snapid;
    ioctx.selfmanaged_snap_create(&snapid);

    vector<snap_t> snaps{snapid};
    ioctx.selfmanaged_snap_set_write_ctx(snapid, snaps);

    string data2(512, 'b');
    bufferlist bl2;
    bl2.append(data2);
    ioctx.write_full("xxx_oid", bl2);

    return 0;
}
