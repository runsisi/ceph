/*
 * write_quota_full.cc
 *
 *  Created on: Sep 22, 2017
 *      Author: runsisi
 */

#include "rados/librados.hpp"
#include "rados/buffer.h"
#include <string>
#include <vector>
#include <stdio.h>

using namespace std;
using namespace librados;

int main() {
    Rados client;
    client.init2("client.admin", "ceph", 0);
    client.conf_read_file(nullptr);

    client.connect();

    IoCtx ioctx;
    client.ioctx_create("p1", ioctx);

    string data1(4 * 1024 * 1024, 'a');
    bufferlist bl1;
    bl1.append(data1);
    ioctx.write("xxx", bl1, 4 * 1024 * 1024, 0);

    ObjectWriteOperation op;
    //op.create(true);
    op.zero(0, 4096);

    ioctx.operate("xxx", &op);

    return 0;
}
