// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "CloseImageRequest.h"
#include "CreateImageRequest.h"
#include "OpenImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/ImageSyncThrottler.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::BootstrapRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;
using librbd::util::unique_lock_name;

template <typename I>
BootstrapRequest<I>::BootstrapRequest(
        librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        std::shared_ptr<ImageSyncThrottler<I>> image_sync_throttler,
        I **local_image_ctx,
        const std::string &local_image_name,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        ContextWQ *work_queue, SafeTimer *timer,
        Mutex *timer_lock,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        Journaler *journaler,
        MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        rbd::mirror::ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::image_replayer::BootstrapRequest",
		reinterpret_cast<CephContext*>(local_io_ctx.cct()), on_finish),
    m_local_io_ctx(local_io_ctx), m_remote_io_ctx(remote_io_ctx),
    m_image_sync_throttler(image_sync_throttler),
    m_local_image_ctx(local_image_ctx), m_local_image_name(local_image_name),
    m_remote_image_id(remote_image_id), m_global_image_id(global_image_id),
    m_work_queue(work_queue), m_timer(timer), m_timer_lock(timer_lock),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid), m_journaler(journaler),
    m_client_meta(client_meta), m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("BootstrapRequest::m_lock", this)) {
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  assert(m_remote_image_ctx == nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {

  // try to get local image id by remote image's global id, so we can know
  // if the local pool has created the local mirror image of the remote image

  get_local_image_id();
}

// called by ImageReplayer<I>::stop if we are in bootstrap stage
template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_canceled = true;

  m_image_sync_throttler->cancel_sync(m_local_io_ctx, m_local_image_id);
}

template <typename I>
void BootstrapRequest<I>::get_local_image_id() {
  dout(20) << dendl;

  update_progress("GET_LOCAL_IMAGE_ID");

  // attempt to cross-reference a local image by the global image id
  // try to check if the local pool has the image mirror enabled,
  // see cls_rbd::mirror_image_set
  librados::ObjectReadOperation op;

  // try to get local image id by global image id
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_local_image_id>(
      this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_image_id(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {

    // we already have a local mirror image of the remote image, so
    // we know the local image id already

    bufferlist::iterator iter = m_out_bl.begin();

    r = librbd::cls_client::mirror_image_get_image_id_finish(
      &iter, &m_local_image_id);
  }

  if (r == -ENOENT) {

    // the global image id does not have a corresponding local image id
    // registered in local pool's RBD_MIRRORING object

    dout(10) << ": image not registered locally" << dendl;
  } else if (r < 0) {
    derr << ": failed to retrieve local image id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  // r == -ENOENT/0, the local image may have not been created
  get_remote_tag_class();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tag_class() {
  dout(20) << dendl;

  update_progress("GET_REMOTE_TAG_CLASS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tag_class>(
      this);

  // get local, i.e., master, client of remote journal
  // note: m_journaler::m_client_id is m_local_mirror_uuid, see
  // ImageReplayer<I>::start, but we can still get other client info
  // of the journaler, every image, either primary or secondary must
  // register a image master client, i.e., whose client id is IMAGE_CLIENT_ID,
  // see Journal<I>::create
  m_journaler->get_client(librbd::Journal<>::IMAGE_CLIENT_ID, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tag_class(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to retrieve remote client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  // get local, i.e., master, client data of remote journal

  librbd::journal::ClientData client_data;
  bufferlist::iterator it = m_client.data.begin();
  try {
    ::decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << ": failed to decode remote client meta data: " << err.what()
         << dendl;
    finish(-EBADMSG);
    return;
  }

  librbd::journal::ImageClientMeta *client_meta =
    boost::get<librbd::journal::ImageClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << ": unknown remote client registration" << dendl;
    finish(-EINVAL);
    return;
  }

  m_remote_tag_class = client_meta->tag_class;

  dout(10) << ": remote tag class=" << m_remote_tag_class << dendl;

  // get mirror peer client of remote journal
  get_client();
}

template <typename I>
void BootstrapRequest<I>::get_client() {
  dout(20) << dendl;

  update_progress("GET_CLIENT");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_client>(
      this);

  // get mirror peer client of the remote journal
  // note: BootstrapRequest<I>::get_remote_tag_class also calls this
  // to get the master client
  m_journaler->get_client(m_local_mirror_uuid, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << ": client not registered" << dendl;
  } else if (r < 0) {
    derr << ": failed to retrieve client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (decode_client_meta()) {

    // if our mirror peer client has registered with the remote journal,
    // then we will decode m_local_image_id from the MirrorPeerClientMeta

    // skip registration if it already exists
    open_remote_image();
    return;
  }

  // r == -ENOENT, the client identified by the m_local_mirror_uuid does
  // not exist, so register it
  register_client();
}

// m_journaler->m_client_id is m_local_mirror_uuid, we are to register
// a mirror peer client of the remote journal
template <typename I>
void BootstrapRequest<I>::register_client() {
  dout(20) << dendl;

  update_progress("REGISTER_CLIENT");

  // record an place-holder record, we do not know m_local_image_id currently,
  // will update the client data in BootstrapRequest<I>::update_client
  librbd::journal::ClientData client_data{
    librbd::journal::MirrorPeerClientMeta{m_local_image_id}};

  bufferlist client_data_bl;
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_register_client>(
      this);

  // register a mirror peer client(Journal<I>::handle_initialized) of
  // the remote Journaler, the client id is m_local_mirror_uuid
  m_journaler->register_client(client_data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_register_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to register with remote journal: " << cpp_strerror(r)
         << dendl;

    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta(m_local_image_id);

  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  dout(20) << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_remote_image>(
      this);

  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_remote_io_ctx, &m_remote_image_ctx, m_remote_image_id, false,
    m_work_queue, ctx);

  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  // deduce the class type for the journal to support unit tests
  typedef typename std::decay<decltype(*I::journal)>::type Journal;

  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open remote image: " << cpp_strerror(r) << dendl;
    assert(m_remote_image_ctx == nullptr);
    finish(r);
    return;
  }

  // open remote image succeeded, check if the remote image is primary

  // TODO: make async
  bool tag_owner;
  r = Journal::is_tag_owner(m_remote_image_ctx, &tag_owner);
  if (r < 0) {
    derr << ": failed to query remote image primary status: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  if (!tag_owner) {

    // the last exclusive lock owner of the remote image is not the
    // master image client

    dout(5) << ": remote image is not primary -- skipping image replay"
            << dendl;

    m_ret_val = -EREMOTEIO;

    close_remote_image();
    return;
  }

  // ok, the remote image is primary

  // default local image name to the remote image name if not provided
  if (m_local_image_name.empty()) {
    m_local_image_name = m_remote_image_ctx->name;
  }

  // TODO: BootstrapRequest<I>::decode_client_meta always update the m_local_image_id ???

  if (m_local_image_id.empty()) {

    // local mirror image has not been registered as a mirror image of
    // the remote primary image, try to create it

    create_local_image();

    return;
  }

  // 1) the local mirror image exists, see BootstrapRequest<I>::get_local_image_id
  // or 2) we have registered a mirror peer client, see BootstrapRequest<I>::decode_client_meta
  // called by BootstrapRequest<I>::handle_get_client
  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  dout(20) << dendl;

  update_progress("OPEN_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);

  // if m_local_image_id is empty, means this is a newly created local
  // mirror image, see BootstrapRequest<I>::create_local_image

  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, m_local_image_ctx,
    (!m_local_image_id.empty() ? std::string() : m_local_image_name),
    m_local_image_id, m_work_queue, ctx);

  // OpenLocalImageRequest will open the local mirror image and check
  // the tag owner, if the local mirror image is not primary then we
  // will try to get the exclusive lock and return
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {

    // if we mirrored a remote image before and then we deleted the local
    // mirror image manually, then we may got a stale, i.e., non-existing,
    // m_local_image_id in BootstrapRequest<I>::handle_get_client, so we
    // need to create a local mirror image with the same name as the remote
    // primary image

    assert(*m_local_image_ctx == nullptr);

    dout(10) << ": local image missing" << dendl;

    create_local_image();

    return;
  } else if (r == -EREMOTEIO) {

    // local image is primary too, maybe we have promoted this local mirror
    // image to primary manually, this is not permitted for mirroring,
    // only if the remote image is primary and the local image is secondary
    // then we can start the mirror process

    assert(*m_local_image_ctx == nullptr);

    dout(10) << "local image is primary -- skipping image replay" << dendl;

    m_ret_val = r;

    close_remote_image();

    return;
  } else if (r < 0) {
    assert(*m_local_image_ctx == nullptr);

    derr << ": failed to open local image: " << cpp_strerror(r) << dendl;

    m_ret_val = r;

    close_remote_image();

    return;
  }

  // local mirror image opened, update the MirrorPeerClientMeta of the
  // remote image journal in case this is a newly created local mirror
  // image, becoz in BootstrapRequest<I>::register_client we registered
  // a place holder mirror peer client
  update_client();
}

template <typename I>
void BootstrapRequest<I>::remove_local_image() {
  dout(20) << dendl;

  update_progress("REMOVE_LOCAL_IMAGE");

  // TODO
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  // TODO
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(20) << dendl;

  // create a local mirror image with the same name as the remote
  // image, currently we do not know the id of the local mirror image
  // to create, will be set after the local mirror image has been
  // opened, see BootstrapRequest<I>::update_client

  // we need to reset m_local_image_id in case BootstrapRequest<I>::decode_client_meta
  // set a non-exsisting local image id, so open_local_image called later
  // will not open the newly created local mirror image with image id but
  // with image name
  m_local_image_id = "";

  update_progress("CREATE_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_create_local_image>(
      this);

  // m_global_image_id is used to enable mirroring of the newly created local mirror
  // image if the mirror mode is pool mode, the newly create tag.mirror_uuid is set
  // to m_remote_mirror_uuid
  CreateImageRequest<I> *request = CreateImageRequest<I>::create(
    m_local_io_ctx, m_work_queue, m_global_image_id, m_remote_mirror_uuid,
    m_local_image_name, m_remote_image_ctx, ctx);

  // create a local mirror image allocate tag with tag.mirror_uuid set to m_remote_mirror_uuid,
  // if the remote image is a clone then we need to mirror the remote parent image first
  // the created image is auto mirror enabled, see librbd::create_v2
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create local image: " << cpp_strerror(r) << dendl;

    m_ret_val = r;
    close_remote_image();
    return;
  }

  m_created_local_image = true;

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::update_client() {
  dout(20) << dendl;

  update_progress("UPDATE_CLIENT");

  if (m_client_meta->image_id == (*m_local_image_ctx)->id) {

    // this is
    // already registered local mirror peer client with the remote journal

    get_remote_tags();

    return;
  }

  // newly created local mirror image, we do not know the image
  // id until now
  m_local_image_id = (*m_local_image_ctx)->id;

  dout(20) << dendl;

  librbd::journal::MirrorPeerClientMeta client_meta;

  // update mirror peer client of remote journal
  client_meta.image_id = m_local_image_id;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client>(
      this);

  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;

    m_ret_val = r;
    close_local_image();
    return;
  }

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;

    m_ret_val = -ECANCELED;
    close_local_image();
    return;
  }

  // update data in memory finally
  m_client_meta->image_id = m_local_image_id;

  // get tags of remote journal
  get_remote_tags();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tags() {
  dout(20) << dendl;

  update_progress("GET_REMOTE_TAGS");

  if (m_created_local_image) {
    // optimization -- no need to compare remote tags if we just created
    // the image locally
    image_sync();
    return;
  }

  // detect split-brain and do image sync

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tags>(this);

  // the remote tag class was got by BootstrapRequest<I>::get_remote_tag_class
  // get the tags of mirror peer client of the remote journaler
  // m_journler->m_client_id is initialized to ImageReplayer::m_local_mirror_uuid, see
  // ImageReplayer<I>::start
  m_journaler->get_tags(m_remote_tag_class, &m_remote_tags, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tags(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to retrieve remote tags: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;
    m_ret_val = -ECANCELED;
    close_local_image();
    return;
  }

  // decode the remote tags
  librbd::journal::TagData remote_tag_data;

  // list<cls::journal::Tag>
  for (auto &tag : m_remote_tags) {

    // iterate remote tags to check if we can find the remote mirrored
    // local demotion tag

    try {
      bufferlist::iterator it = tag.data.begin();

      ::decode(remote_tag_data, it);
    } catch (const buffer::error &err) {
      derr << ": failed to decode remote tag: " << err.what() << dendl;
      m_ret_val = -EBADMSG;
      close_local_image();
      return;
    }

    dout(10) << ": decoded remote tag: " << remote_tag_data << dendl;

    // tag_data.predecessor_mirror_uuid is set by prev_tag_data.mirror_uuid while
    // tag_data.mirror_uuid is set by caller provided mirror_uuid, see allocate_journaler_tag

    if (remote_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
        remote_tag_data.predecessor_mirror_uuid == m_local_mirror_uuid) {

      // remote tag is chained off a local tag demotion

      break;
    }
  }

  // At this point, the local image was existing and non-primary and the remote
  // image is primary.  Attempt to link the local image's most recent tag
  // to the remote image's tag chain.
  I *local_image_ctx = (*m_local_image_ctx);

  {
    RWLock::RLocker snap_locker(local_image_ctx->snap_lock);

    if (local_image_ctx->journal == nullptr) {
      derr << ": local image does not support journaling" << dendl;
      m_ret_val = -EINVAL;
      close_local_image();
      return;
    }

    // return Journal::m_tag_data, note: local_image_ctx->journal has
    // been opened by BootstrapRequest<I>::open_local_image, the Journal::m_client_id
    // is IMAGE_CLIENT_ID, see Journal<I>::create_journaler
    librbd::journal::TagData tag_data =
      local_image_ctx->journal->get_tag_data();

    dout(20) << ": local tag data: " << tag_data << dendl;

    if (tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
	remote_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
	remote_tag_data.predecessor_mirror_uuid == m_local_mirror_uuid) {

      // remote image: secondary -> (promoted) -> primary
      // local image: primary -> (demoted) -> secondary

      dout(20) << ": local image was demoted" << dendl;
    } else if (tag_data.mirror_uuid == m_remote_mirror_uuid &&
	       m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {

      // image sync has finished
      //

      dout(20) << ": local image is in clean replay state" << dendl;
    } else if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_SYNCING) {

      // previous image sync has not finished

      dout(20) << ": previous sync was canceled" << dendl;
    } else {
      derr << ": split-brain detected -- skipping image replay" << dendl;

      m_ret_val = -EEXIST;

      close_local_image();
      return;
    }
  }

  // no split-brain detected

  image_sync();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  dout(20) << dendl;

  update_progress("IMAGE_SYNC");

  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {
    // clean replay state -- no image sync required
    close_remote_image();
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(
      this);

  {
    Mutex::Locker locker(m_lock);
    if (!m_canceled) {
      m_image_sync_throttler->start_sync(*m_local_image_ctx,
                                         m_remote_image_ctx, m_timer,
                                         m_timer_lock,
                                         m_local_mirror_uuid, m_journaler,
                                         m_client_meta, m_work_queue, ctx,
                                         m_progress_ctx);
      return;
    }
  }

  dout(10) << ": request canceled" << dendl;
  m_ret_val = -ECANCELED;
  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(20) << ": r=" << r << dendl;

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (r < 0) {
    derr << ": failed to sync remote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  // we will open remote journal and do external replay, so no need to
  // keep opening the remote image anymore
  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_local_image() {
  dout(20) << dendl;

  update_progress("CLOSE_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_local_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_local_image_ctx, m_work_queue, false, ctx);

  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing local image: " << cpp_strerror(r)
         << dendl;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_remote_image() {
  dout(20) << dendl;

  update_progress("CLOSE_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_remote_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_remote_image_ctx, m_work_queue, false, ctx);

  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing remote image: " << cpp_strerror(r)
         << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
bool BootstrapRequest<I>::decode_client_meta() {
  dout(20) << dendl;

  librbd::journal::ClientData client_data;

  bufferlist::iterator it = m_client.data.begin();
  try {
    ::decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << ": failed to decode client meta data: " << err.what() << dendl;
    return true;
  }

  librbd::journal::MirrorPeerClientMeta *client_meta =
    boost::get<librbd::journal::MirrorPeerClientMeta>(&client_data.client_meta);

  if (client_meta == nullptr) {
    derr << ": unknown peer registration" << dendl;
    return true;
  } else if (!client_meta->image_id.empty()) {
    // have an image id -- use that to open the image
    m_local_image_id = client_meta->image_id;
  }

  *m_client_meta = *client_meta;

  dout(20) << ": client found: image_id=" << m_local_image_id
	   << ", client_meta=" << *m_client_meta << dendl;

  return true;
}

template <typename I>
void BootstrapRequest<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
