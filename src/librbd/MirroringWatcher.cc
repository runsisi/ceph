// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/MirroringWatcher.h"
#include "include/rbd_types.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MirroringWatcher: "

namespace librbd {

using namespace mirroring_watcher;
using namespace watcher;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

// never be instanced, others always call its static methods
template <typename I>
MirroringWatcher<I>::MirroringWatcher(librados::IoCtx &io_ctx,
                                      ContextWQ *work_queue)
  : Watcher(io_ctx, work_queue, RBD_MIRRORING) {
}

// static
// called by
// librbd::mirror_mode_set
template <typename I>
int MirroringWatcher<I>::notify_mode_updated(librados::IoCtx &io_ctx,
                                              cls::rbd::MirrorMode mirror_mode) {
  C_SaferCond ctx;
  notify_mode_updated(io_ctx, mirror_mode, &ctx);
  return ctx.wait();
}

// static
// called by
// MirroringWatcher<I>::notify_mode_updated, i.e., above
template <typename I>
void MirroringWatcher<I>::notify_mode_updated(librados::IoCtx &io_ctx,
                                              cls::rbd::MirrorMode mirror_mode,
                                              Context *on_finish) {
  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{ModeUpdatedPayload{mirror_mode}}, bl);

  librados::AioCompletion *comp = util::create_rados_ack_callback(on_finish);
  int r = io_ctx.aio_notify(RBD_MIRRORING, comp, bl, NOTIFY_TIMEOUT_MS,
                            nullptr);
  assert(r == 0);
  comp->release();
}

// static
// never called
template <typename I>
int MirroringWatcher<I>::notify_image_updated(
    librados::IoCtx &io_ctx, cls::rbd::MirrorImageState mirror_image_state,
    const std::string &image_id, const std::string &global_image_id) {
  C_SaferCond ctx;
  notify_image_updated(io_ctx, mirror_image_state, image_id, global_image_id,
                       &ctx);
  return ctx.wait();
}

// static
// called by
// DisableRequest<I>::send_notify_mirroring_watcher
// DisableRequest<I>::send_notify_mirroring_watcher_removed
// EnableRequest<I>::send_notify_mirroring_watcher
template <typename I>
void MirroringWatcher<I>::notify_image_updated(
    librados::IoCtx &io_ctx, cls::rbd::MirrorImageState mirror_image_state,
    const std::string &image_id, const std::string &global_image_id,
    Context *on_finish) {

  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{ImageUpdatedPayload{
      mirror_image_state, image_id, global_image_id}}, bl);

  librados::AioCompletion *comp = util::create_rados_ack_callback(on_finish);
  int r = io_ctx.aio_notify(RBD_MIRRORING, comp, bl, NOTIFY_TIMEOUT_MS,
                            nullptr);
  assert(r == 0);
  comp->release();

}

template <typename I>
void MirroringWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                        uint64_t notifier_id, bufferlist &bl) {
  CephContext *cct = this->m_cct;
  ldout(cct, 15) << ": notify_id=" << notify_id << ", "
                 << "handle=" << handle << dendl;


  NotifyMessage notify_message;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_message, iter);
  } catch (const buffer::error &err) {
    lderr(cct) << ": error decoding image notification: " << err.what()
               << dendl;
    Context *ctx = new C_NotifyAck(this, notify_id, handle);
    ctx->complete(0);
    return;
  }

  // no one handles the notify of RBD_MIRRORING
  apply_visitor(HandlePayloadVisitor<MirroringWatcher<I>>(this, notify_id,
                                                          handle),
                notify_message.payload);
}

template <typename I>
bool MirroringWatcher<I>::handle_payload(const ModeUpdatedPayload &payload,
                                         Context *on_notify_ack) {
  CephContext *cct = this->m_cct;
  ldout(cct, 20) << ": mode updated: " << payload.mirror_mode << dendl;

  // pure virtual
  handle_mode_updated(payload.mirror_mode, on_notify_ack);
  return true;
}

template <typename I>
bool MirroringWatcher<I>::handle_payload(const ImageUpdatedPayload &payload,
                                         Context *on_notify_ack) {
  CephContext *cct = this->m_cct;
  ldout(cct, 20) << ": image state updated" << dendl;

  // pure virtual
  handle_image_updated(payload.mirror_image_state, payload.image_id,
                       payload.global_image_id, on_notify_ack);
  return true;
}

template <typename I>
bool MirroringWatcher<I>::handle_payload(const UnknownPayload &payload,
                                         Context *on_notify_ack) {
  return true;
}

} // namespace librbd

template class librbd::MirroringWatcher<librbd::ImageCtx>;
