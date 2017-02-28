// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/ReleaseRequest.h"
#include "librbd/Watcher.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::ReleaseRequest: " \
                            << this << " " << __func__ << ": "

namespace librbd {
namespace managed_lock {

using util::detail::C_AsyncCallback;
using util::create_context_callback;
using util::create_rados_callback;

// created by
// ManagedLock<I>::revert_to_unlock_state
// ManagedLock<I>::handle_pre_release_lock
// ManagedLock<I>::handle_shutdown_pre_release
template <typename I>
ReleaseRequest<I>* ReleaseRequest<I>::create(librados::IoCtx& ioctx,
                                             Watcher *watcher,
                                             ContextWQ *work_queue,
                                             const string& oid,
                                             const string& cookie,
                                             Context *on_finish) {
  return new ReleaseRequest(ioctx, watcher, work_queue, oid, cookie,
                            on_finish);
}

template <typename I>
ReleaseRequest<I>::ReleaseRequest(librados::IoCtx& ioctx, Watcher *watcher,
                                  ContextWQ *work_queue, const string& oid,
                                  const string& cookie, Context *on_finish)
  : m_ioctx(ioctx), m_watcher(watcher), m_oid(oid), m_cookie(cookie),
    m_on_finish(new C_AsyncCallback<ContextWQ>(work_queue, on_finish)) {
}

template <typename I>
ReleaseRequest<I>::~ReleaseRequest() {
}


template <typename I>
void ReleaseRequest<I>::send() {
  send_unlock();
}

template <typename I>
void ReleaseRequest<I>::send_unlock() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << "cookie=" << m_cookie << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::unlock(&op, RBD_LOCK_NAME, m_cookie);

  using klass = ReleaseRequest;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_unlock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ReleaseRequest<I>::handle_unlock(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to unlock: " << cpp_strerror(r) << dendl;
  }

  finish();
}

template <typename I>
void ReleaseRequest<I>::finish() {
  m_on_finish->complete(0);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::ReleaseRequest<librbd::ImageCtx>;

