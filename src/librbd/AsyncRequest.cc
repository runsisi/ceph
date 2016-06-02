// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "common/WorkQueue.h"

namespace librbd
{

template <typename T>
AsyncRequest<T>::AsyncRequest(T &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_canceled(false),
    m_xlist_item(this) {
  assert(m_on_finish != NULL);
  
  // push back of ImageCtx::async_requests
  start_request();
}

template <typename T>
AsyncRequest<T>::~AsyncRequest() {
}

template <typename T>
void AsyncRequest<T>::async_complete(int r) {
  m_image_ctx.op_work_queue->queue(create_callback_context(), r);
}

template <typename T>
librados::AioCompletion *AsyncRequest<T>::create_callback_completion() {
  return util::create_rados_safe_callback(this);
}

template <typename T>
Context *AsyncRequest<T>::create_callback_context() {
  return util::create_context_callback(this);
}

// called in TrimRequest<I>::send_clean_boundary
template <typename T>
Context *AsyncRequest<T>::create_async_callback_context() {
  // TODO: can use create_async_context_callback instead ???
  // i.e. return util::create_async_context_callback(m_image_ctx, create_callback_context())
  return util::create_context_callback<AsyncRequest<T>,
                                       &AsyncRequest<T>::async_complete>(this);
}

// private, called by ctor of AsyncRequest
template <typename T>
void AsyncRequest<T>::start_request() {
  Mutex::Locker async_ops_locker(m_image_ctx.async_ops_lock);
  
  m_image_ctx.async_requests.push_back(&m_xlist_item);
}

// private
template <typename T>
void AsyncRequest<T>::finish_request() {
  decltype(m_image_ctx.async_requests_waiters) waiters;

  {
    Mutex::Locker async_ops_locker(m_image_ctx.async_ops_lock);

    assert(m_xlist_item.remove_myself());

    if (m_image_ctx.async_requests.empty()) {

      // pushed by ImageCtx::cancel_async_requests
      waiters = std::move(m_image_ctx.async_requests_waiters);
    }
  }

  for (auto ctx : waiters) {
    ctx->complete(0);
  }
}

} // namespace librbd

template class librbd::AsyncRequest<librbd::ImageCtx>;
