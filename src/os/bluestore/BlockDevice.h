// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_BLOCKDEVICE_H
#define CEPH_OS_BLUESTORE_BLOCKDEVICE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <list>

#include "acconfig.h"
#include "os/fs/aio.h"

#define SPDK_PREFIX "spdk:"

// created by
// BlueFS::add_block_device
/// track in-flight io
struct IOContext {
  CephContext* cct;
  void *priv;
#ifdef HAVE_SPDK
  void *nvme_task_first = nullptr;
  void *nvme_task_last = nullptr;
#endif

  std::mutex lock;
  std::condition_variable cond;

  std::list<aio_t> pending_aios;    ///< not yet submitted
  std::list<aio_t> running_aios;    ///< submitting or submitted
  std::atomic_int num_pending = {0};
  std::atomic_int num_running = {0};

  explicit IOContext(CephContext* cct, void *p)
    : cct(cct), priv(p)
    {}

  // no copying
  IOContext(const IOContext& other) = delete;
  IOContext &operator=(const IOContext& other) = delete;

  bool has_pending_aios() {
    return num_pending.load();
  }

  // wait until num_running && num_reading reach both 0
  void aio_wait();

  // called by
  // KernelDevice::_aio_thread
  // KernelDevice::read
  // NVMEDevice::io_complete
  void aio_wake() {
    std::lock_guard<std::mutex> l(lock);
    cond.notify_all();
    --num_running;
    assert(num_running == 0);
  }
};


class BlockDevice {
public:
  CephContext* cct;
private:
  std::mutex ioc_reap_lock;
  std::vector<IOContext*> ioc_reap_queue;
  std::atomic_int ioc_reap_count = {0};

protected:
  bool rotational = true;

public:
  BlockDevice(CephContext* cct) : cct(cct) {}
  virtual ~BlockDevice() = default;
  typedef void (*aio_callback_t)(void *handle, void *aio);

  static BlockDevice *create(
    CephContext* cct, const std::string& path, aio_callback_t cb, void *cbpriv);
  virtual bool supported_bdev_label() { return true; }
  virtual bool is_rotational() { return rotational; }

  virtual void aio_submit(IOContext *ioc) = 0;

  virtual uint64_t get_size() const = 0;
  virtual uint64_t get_block_size() const = 0;

  virtual int collect_metadata(std::string prefix, std::map<std::string,std::string> *pm) const = 0;

  virtual int read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc,
    bool buffered) = 0;
  virtual int read_random(
    uint64_t off,
    uint64_t len,
    char *buf,
    bool buffered) = 0;
  virtual int write(
    uint64_t off,
    bufferlist& bl,
    bool buffered) = 0;

  virtual int aio_read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc) = 0;
  virtual int aio_write(
    uint64_t off,
    bufferlist& bl,
    IOContext *ioc,
    bool buffered) = 0;
  virtual int flush() = 0;

  void queue_reap_ioc(IOContext *ioc);
  void reap_ioc();

  // for managing buffered readers/writers
  virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
  virtual int open(const std::string& path) = 0;
  virtual void close() = 0;
};

#endif //CEPH_OS_BLUESTORE_BLOCKDEVICE_H
