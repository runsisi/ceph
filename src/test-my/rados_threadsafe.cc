/*
 * rados_threadsafe.cc
 *
 *  Created on: Aug 16, 2018
 *      Author: runsisi
 */

#include "rados/librados.hpp"
#include "rbd/librbd.hpp"
#include <string>
#include <vector>
#include <iostream>
#include <vector>
#include <thread>

using namespace std;
using namespace librados;
using namespace librbd;

int main(int argc, char **argv) {
    Rados client;
    int r = client.init2("client.admin", "ceph", 0);
    if (r < 0) {
        cout << "client.init2 failed: " << r << endl;
        return -1;
    }

    r = client.conf_read_file(nullptr);
    if (r < 0) {
        cout << "client.conf_read_file failed: " << r << endl;
        return -1;
    }

    r = client.connect();
    if (r < 0) {
        cout << "client.connect failed: " << r << endl;
        return -1;
    }

    int thread_count = 8;
    int iterate_count = 16;
    int create_or_remove = 1;
    std::string pool_name = "rbd";
    if (argc > 1) {
        thread_count = std::stoi(argv[1]);
    }
    if (argc > 2) {
        iterate_count = std::stoi(argv[2]);
    }
    if (argc > 3) {
        create_or_remove = std::stoi(argv[3]);
    }
    if (argc > 4) {
        pool_name = argv[4];
    }

    // create worker threads
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; i ++) {
        auto t = std::thread([&client, i, iterate_count, create_or_remove,
                              pool_name]() {
            IoCtx ioctx;
            int r = client.ioctx_create(pool_name.c_str(), ioctx);
            if (r < 0) {
                cout << "client.ioctx_create failed: " << r << endl;
                return -2;
            }

            RBD rbd;

            string thread_name("thread");
            thread_name += std::to_string(i);
            uint64_t size = 1024 * 1024 * 1024 * 10UL;
            int order = 22;

            if (create_or_remove & 1) {
                for (int j = 0; j < iterate_count; j++) {
                    cout << "--> thread: " << i << ", create iterate: " << j << endl;

                    string name = thread_name + "-" + std::to_string(j);
                    rbd.create(ioctx, name.c_str(), size, &order);
//                    vector<string> names;
//                    rbd.list(ioctx, names);
                }
            }

            if (create_or_remove & 2) {
                for (int j = 0; j < iterate_count; j++) {
                    cout << "--> thread: " << i << ", remove iterate: " << j << endl;

                    string name = thread_name + "-" + std::to_string(j);
//                    vector<string> names;
//                    rbd.list(ioctx, names);
                    rbd.remove(ioctx, name.c_str());
                }
            }
        });

        threads.push_back(std::move(t));
    }

    for (int i = 0; i < thread_count; i++) {
        threads[i].join();
    }

    cout << "main exit" << endl;

    return 0;
}

// g++ -o rados_threads -std=c++11 rados_threads.cc -lrados -lrbd -lpthread -ggdb3
//        -I../../build/include -I../include  -L../../build/lib
