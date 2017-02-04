// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "common/errno.h"
#include "include/assert.h"
#include "librbd/Utils.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "journal/Settings.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/RemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal::CreateRequest: "

namespace librbd {

using util::create_context_callback;

namespace journal {

// created by
// EnableFeaturesRequest<I>::send_create_journal
// Journal<I>::create
template<typename I>
CreateRequest<I>::CreateRequest(IoCtx &ioctx, const std::string &imageid,
                                              uint8_t order, uint8_t splay_width,
                                              const std::string &object_pool,
                                              uint64_t tag_class, TagData &tag_data,
                                              const std::string &client_id,
                                              ContextWQ *op_work_queue,
                                              Context *on_finish)
  : m_image_id(imageid), m_order(order), m_splay_width(splay_width),
    m_object_pool(object_pool), m_tag_class(tag_class), m_tag_data(tag_data),
    m_image_client_id(client_id), m_op_work_queue(op_work_queue),
    m_on_finish(on_finish) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
void CreateRequest<I>::send() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (m_order > 64 || m_order < 12) {
    lderr(m_cct) << "order must be in the range [12, 64]" << dendl;
    complete(-EDOM);
    return;
  }

  if (m_splay_width == 0) {
    complete(-EINVAL);
    return;
  }

  get_pool_id();
}

template<typename I>
void CreateRequest<I>::get_pool_id() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (m_object_pool.empty()) {
    create_journal();
    return;
  }

  librados::Rados rados(m_ioctx);
  IoCtx data_ioctx;
  int r = rados.ioctx_create(m_object_pool.c_str(), data_ioctx);
  if (r != 0) {
    lderr(m_cct) << "failed to create journal: "
                 << "error opening journal object pool '" << m_object_pool
                 << "': " << cpp_strerror(r) << dendl;
    complete(r);
    return;
  }

  // journal data pool provided, git its id
  m_pool_id = data_ioctx.get_id();

  create_journal();
}

template<typename I>
void CreateRequest<I>::create_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  ImageCtx::get_timer_instance(m_cct, &m_timer, &m_timer_lock);

  // create an Journaler instance, the same as RemoveRequest<I>::stat_journal
  m_journaler = new Journaler(m_op_work_queue, m_timer, m_timer_lock,
                              m_ioctx, m_image_id, m_image_client_id, {});

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_create_journal>(this);

  // register omaps, i.e., "order", "splay_width", etc., for "journal.xxx"
  // metadata object
  m_journaler->create(m_order, m_splay_width, m_pool_id, ctx);
}

template<typename I>
Context *CreateRequest<I>::handle_create_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to create journal: " << cpp_strerror(*result) << dendl;
    shut_down_journaler(*result);
    return nullptr;
  }

  allocate_journal_tag();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::allocate_journal_tag() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_tag>(this);

  // if we are creating journal for local mirror image, then tag_data.mirror_uuid
  // is remote cluster's mirror uuid, else it is LOCAL_MIRROR_UUID which means
  // we are creating a primary image
  ::encode(m_tag_data, m_bl);

  // Tag::TAG_CLASS_NEW, create the first tag to identify if we are
  // a local mirror image, i.e., a non-primary image or a primary image
  // note: only ImageReplayer can create a new image which is non-primary,
  // all other non-primary images are from demotion when mirror is enabled
  // for these images
  m_journaler->allocate_tag(m_tag_class, m_bl, &m_tag, ctx);
}

template<typename I>
Context *CreateRequest<I>::handle_journal_tag(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to allocate tag: " << cpp_strerror(*result) << dendl;
    shut_down_journaler(*result);
    return nullptr;
  }

  // register image client
  register_client();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::register_client() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  m_bl.clear();
  ::encode(ClientData{ImageClientMeta{m_tag.tag_class}}, m_bl);

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_register_client>(this);

  // register <m_image_client_id, m_bl> on journal metadata object omap
  m_journaler->register_client(m_bl, ctx);
}

template<typename I>
Context *CreateRequest<I>::handle_register_client(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to register client: " << cpp_strerror(*result) << dendl;
  }

  shut_down_journaler(*result);
  return nullptr;
}

template<typename I>
void CreateRequest<I>::shut_down_journaler(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  m_r_saved = r;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journaler_shutdown>(this);

  // remove metadata listener registered by m_trimmer, unwatch metadata object
  // NOTE: we only called m_journaler->create(), without calling
  // m_journaler->start_replay() and m_journaler->start_append(), so
  // Journaler::m_player and Journaler::m_recorder are both NULL
  m_journaler->shut_down(ctx);
}

template<typename I>
Context *CreateRequest<I>::handle_journaler_shutdown(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to shut down journaler: " << cpp_strerror(*result) << dendl;
  }

  delete m_journaler;

  if (!m_r_saved) {
    complete(0);
    return nullptr;
  }

  // there was an error during journal creation, so we rollback
  // what ever was done. the easiest way to do this is to invoke
  // journal remove state machine, although it's not the most
  // cleanest approach when it comes to redundancy, but that's
  // ok during the failure path.
  remove_journal();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::remove_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_journal>(this);

  RemoveRequest<I> *req = RemoveRequest<I>::create(
    m_ioctx, m_image_id, m_image_client_id, m_op_work_queue, ctx);

  req->send();
}

template<typename I>
Context *CreateRequest<I>::handle_remove_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up journal after creation failed: "
                 << cpp_strerror(*result) << dendl;
  }

  complete(m_r_saved);
  return nullptr;
}

template<typename I>
void CreateRequest<I>::complete(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (r == 0) {
    ldout(m_cct, 20) << "done." << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::CreateRequest<librbd::ImageCtx>;
