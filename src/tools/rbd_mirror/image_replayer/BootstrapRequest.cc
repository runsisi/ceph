// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "CloseImageRequest.h"
#include "CreateImageRequest.h"
#include "IsPrimaryRequest.h"
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
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/ImageSync.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::BootstrapRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::unique_lock_name;

// created by
// ImageReplayer<I>::bootstrap
template <typename I>
BootstrapRequest<I>::BootstrapRequest(
        librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        InstanceWatcher<I> *instance_watcher,
        I **local_image_ctx,
        const std::string &local_image_id,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        ContextWQ *work_queue, SafeTimer *timer,
        Mutex *timer_lock,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        Journaler *journaler,
        cls::journal::ClientState *client_state,
        MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        bool *do_resync,
        rbd::mirror::ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::image_replayer::BootstrapRequest",
		reinterpret_cast<CephContext*>(local_io_ctx.cct()), on_finish),
    m_local_io_ctx(local_io_ctx), m_remote_io_ctx(remote_io_ctx),
    m_instance_watcher(instance_watcher), m_local_image_ctx(local_image_ctx),
    m_local_image_id(local_image_id), m_remote_image_id(remote_image_id),
    m_global_image_id(global_image_id), m_work_queue(work_queue),
    m_timer(timer), m_timer_lock(timer_lock),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid), m_journaler(journaler),
    m_client_state(client_state), m_client_meta(client_meta),
    m_progress_ctx(progress_ctx), m_do_resync(do_resync),
    m_lock(unique_lock_name("BootstrapRequest::m_lock", this)) {
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  assert(m_remote_image_ctx == nullptr);
}

template <typename I>
bool BootstrapRequest<I>::is_syncing() const {
  Mutex::Locker locker(m_lock);
  return (m_image_sync != nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {
  // will be checked and set by BootstrapRequest<I>::handle_open_local_image
  *m_do_resync = false;

  get_remote_tag_class();
}

// called by
// ImageReplayer<I>::stop, for STATE_STARTING
template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  m_canceled = true;

  if (m_image_sync != nullptr) {
    m_image_sync->cancel();
  }
}

template <typename I>
void BootstrapRequest<I>::get_remote_tag_class() {
  dout(20) << dendl;

  update_progress("GET_REMOTE_TAG_CLASS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tag_class>(
      this);


  // NOTE: m_journaler::m_client_id is m_local_mirror_uuid, see
  // ImageReplayer<I>::start -> ImageReplayer<I>::bootstrap

  // m_journaler should be renamed to m_remote_journaler

  // get image client data of remote journal from remote journal
  // metadata object's omap entry "client_" + client_id
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

  // get image client metadata of remote journal

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

  // NOTE: there are three types of client meta data, i.e.,
  // ImageClientMeta, MirrorPeerCLientMeta, CliClientMeta

  librbd::journal::ImageClientMeta *client_meta =
    boost::get<librbd::journal::ImageClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << ": unknown remote client registration" << dendl;
    finish(-EINVAL);
    return;
  }

  // can be used by BootstrapRequest<I>::get_remote_tags later
  m_remote_tag_class = client_meta->tag_class;

  dout(10) << ": remote tag class=" << m_remote_tag_class << dendl;

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
    ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open remote image: " << cpp_strerror(r) << dendl;
    assert(m_remote_image_ctx == nullptr);
    finish(r);
    return;
  }

  is_primary();
}

template <typename I>
void BootstrapRequest<I>::is_primary() {
  dout(20) << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_is_primary>(
      this);
  IsPrimaryRequest<I> *request = IsPrimaryRequest<I>::create(m_remote_image_ctx,
                                                             &m_primary, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_is_primary(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error querying remote image primary status: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  if (!m_primary) {
    dout(5) << ": remote image is not primary -- skipping image replay"
            << dendl;

    m_ret_val = -EREMOTEIO;

    // set mirror peer client state to MIRROR_PEER_STATE_REPLAYING
    // prevent syncing to non-primary image after failover, see beaef37
    update_client_state();

    return;
  }

  if (!m_client_meta->image_id.empty()) {
    // have an image id -- use that to open the image since a deletion (resync)
    // will leave the old image id registered in the peer
    m_local_image_id = m_client_meta->image_id;
  }

  if (m_local_image_id.empty()) {
    update_client_image();
    return;
  }

  open_local_image();
}

// the remote image is not primary, maybe a local mirror image
// or a demoted image, if the remote image be promoted later, then
// we can proceed
template <typename I>
void BootstrapRequest<I>::update_client_state() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {
    // state already set for replaying upon failover
    close_remote_image();
    return;
  }

  // update mirror peer client's state so if the remote image promoted
  // again, we need to check split-brain in BootstrapRequest<I>::get_remote_tags

  dout(20) << dendl;

  update_progress("UPDATE_CLIENT_STATE");

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);

  client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client_state>(
      this);

  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client_state(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;
  } else {
    m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  }

  // close remote image and finish the bootstrap process
  // NOTE: we have registered us as a mirror peer client of the remote
  // journal, we are not to unregister it, it remains
  close_remote_image();
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
    m_local_io_ctx, m_local_image_ctx, m_local_image_id, m_work_queue,
    ctx);

  // OpenLocalImageRequest will open the local mirror image and check
  // the tag owner, if the local mirror image is not primary then we
  // will request the exclusive lock before return
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
    unregister_client();
    return;
  } else if (r == -EREMOTEIO) {

    // local image is primary too, maybe we have promoted this local mirror
    // image to primary manually, this is not permitted for mirroring,
    // only if the remote image is primary and the local image is non-primary
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

  I *local_image_ctx = (*m_local_image_ctx);


  {
    RWLock::RLocker snap_locker(local_image_ctx->snap_lock);

    if (local_image_ctx->journal == nullptr) {
      derr << ": local image does not support journaling" << dendl;
      m_ret_val = -EINVAL;
      close_local_image();
      return;
    }

    r = (*m_local_image_ctx)->journal->is_resync_requested(m_do_resync);
    if (r < 0) {
      derr << ": failed to check if a resync was requested" << dendl;
      m_ret_val = r;
      close_local_image();
      return;
    }
  }

  if (*m_do_resync) {
    close_remote_image();
    return;
  }

  if (*m_client_state == cls::journal::CLIENT_STATE_DISCONNECTED) {
    dout(10) << ": client flagged disconnected -- skipping bootstrap" << dendl;

    // The caller is expected to detect disconnect initializing remote journal.
    m_ret_val = 0;

    close_remote_image();
    return;
  }

  get_remote_tags();
}

template <typename I>
void BootstrapRequest<I>::unregister_client() {
  dout(20) << dendl;
  update_progress("UNREGISTER_CLIENT");

  // create a local mirror image with the same name as the remote
  // image, currently we do not know the id of the local mirror image
  // to create, will be set after the local mirror image has been
  // opened, see BootstrapRequest<I>::update_client

  // we need to reset m_local_image_id in case BootstrapRequest<I>::decode_client_meta
  // set a non-exsisting local image id, so open_local_image called later
  // will not open the newly created local mirror image with image id but
  // with image name
  m_local_image_id = "";
  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_unregister_client>(
      this);
  m_journaler->unregister_client(ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_unregister_client(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to unregister with remote journal: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta("");
  register_client();
}

// open local mirror image succeeded, now update the MirrorPeerClientMeta
// if this is a newly created local mirror image
template <typename I>
void BootstrapRequest<I>::register_client() {
  dout(20) << dendl;

  update_progress("REGISTER_CLIENT");

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  bufferlist client_data_bl;
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_register_client>(
      this);
  m_journaler->register_client(client_data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_register_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  *m_client_state = cls::journal::CLIENT_STATE_CONNECTED;
  *m_client_meta = librbd::journal::MirrorPeerClientMeta(m_local_image_id);
  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  is_primary();
}

template <typename I>
void BootstrapRequest<I>::update_client_image() {
  dout(20) << dendl;
  update_progress("UPDATE_CLIENT_IMAGE");

  assert(m_local_image_id.empty());
  m_local_image_id = librbd::util::generate_image_id<I>(m_local_io_ctx);

  librbd::journal::MirrorPeerClientMeta client_meta{m_local_image_id};
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client_image>(
      this);

  // update MirrorPeerClientMeta::image_id
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;

    m_ret_val = r;
    close_remote_image();
    return;
  }

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;

    m_ret_val = -ECANCELED;
    close_remote_image();
    return;
  }

  *m_client_meta = {m_local_image_id};
  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  create_local_image();
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(20) << dendl;
  update_progress("CREATE_LOCAL_IMAGE");

  m_remote_image_ctx->snap_lock.get_read();
  std::string image_name = m_remote_image_ctx->name;
  m_remote_image_ctx->snap_lock.put_read();

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_create_local_image>(
      this);
  CreateImageRequest<I> *request = CreateImageRequest<I>::create(
    m_local_io_ctx, m_work_queue, m_global_image_id, m_remote_mirror_uuid,
    image_name, m_local_image_id, m_remote_image_ctx, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << ": parent image does not exist" << dendl;
    } else {
      derr << ": failed to create local image: " << cpp_strerror(r) << dendl;
    }
    m_ret_val = r;
    close_remote_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tags() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_SYNCING) {
    // optimization -- no need to compare remote tags if we just created
    // the image locally or sync was interrupted
    image_sync();
    return;
  }

  // if the previous sync was interrupted and then the primary image was
  // demoted, then the restarted ImageReplayer will update the MirrorPeerClientMeta
  // state to MIRROR_PEER_STATE_REPLAYING, see
  // BootstrapRequest<I>::handle_open_remote_image -> BootstrapRequest<I>::update_client_state
  // if then the remote image promoted and we are continue to mirror, we
  // need to do something more

  // to detect split-brain and do image sync afterwards

  dout(20) << dendl;
  update_progress("GET_REMOTE_TAGS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tags>(this);

  // NOTE: the remote tag class was got by BootstrapRequest<I>::get_remote_tag_class,
  // and m_journler->m_client_id is initialized to ImageReplayer::m_local_mirror_uuid,
  // see ImageReplayer<I>::start, the client id id used to exclude the committed
  // tags of the client

  // get uncommitted tags of the mirror peer client, i.e., us, of the remote journal
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

  // At this point, the local image was existing, non-primary, and replaying;
  // and the remote image is primary.  Attempt to link the local image's most
  // recent tag to the remote image's tag chain.
  uint64_t local_tag_tid;
  librbd::journal::TagData local_tag_data;
  I *local_image_ctx = (*m_local_image_ctx);

  {
    RWLock::RLocker snap_locker(local_image_ctx->snap_lock);

    if (local_image_ctx->journal == nullptr) {
      derr << ": local image does not support journaling" << dendl;
      m_ret_val = -EINVAL;
      close_local_image();
      return;
    }

    // NOTE: OpenLocalImageRequest<I>::send_lock_image has acquired the exclusive lock
    // Journal::m_tag_tid and Journal::m_tag_data are updated with
    // newly allocated tags accordingly
    local_tag_tid = local_image_ctx->journal->get_tag_tid();
    local_tag_data = local_image_ctx->journal->get_tag_data();

    dout(20) << ": local tag " << local_tag_tid << ": "
             << local_tag_data << dendl;
  }

  bool remote_tag_data_valid = false;
  librbd::journal::TagData remote_tag_data;
  boost::optional<uint64_t> remote_orphan_tag_tid =
    boost::make_optional<uint64_t>(false, 0U);
  bool reconnect_orphan = false;

  // decode the remote tags
  for (auto &remote_tag : m_remote_tags) {

    // iterate through remote tags, i.e., list<cls::journal::Tag>,
    // that has not been committed by local mirror peer client

    if (local_tag_data.predecessor.commit_valid &&
        local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
        local_tag_data.predecessor.tag_tid > remote_tag.tid) {

      // local mirror peer client ever committed, and the local tags were
      // mirrored from the remote tags

      // NOTE: if journal feature enabled, then the image should always
      // create the journal metadata object and create the initial tag,
      // see rbd::mirror::CreateImageRequest<I>::create_image ->
      // librbd::image::CreateRequest<I>::journal_create for local mirror
      // image creation
      // NOTE: the initial tag only set tag.mirror_uuid, so for the initial
      // tag, the predecessor.commit_valid field was always set to false

      dout(20) << ": skipping processed predecessor remote tag "
               << remote_tag.tid << dendl;

      continue;
    }

    try {
      bufferlist::iterator it = remote_tag.data.begin();
      ::decode(remote_tag_data, it);

      remote_tag_data_valid = true;
    } catch (const buffer::error &err) {
      derr << ": failed to decode remote tag " << remote_tag.tid << ": "
           << err.what() << dendl;
      m_ret_val = -EBADMSG;
      close_local_image();
      return;
    }

    dout(10) << ": decoded remote tag " << remote_tag.tid << ": "
             << remote_tag_data << dendl;

    // NOTE: only newly created journal, i.e., by image creation or journal reset,
    // or Journal<I>::promote will set commit_valid to definitely true,
    // while Journal<I>::demote and Journal<I>::allocate_local_tag will set
    // commit_valid to true only if client.commit_position.object_positions
    // is not empty, which means the local mirror peer client ever committed,
    // journal can be reset, while the client.commit_position can not be
    // reset, so client.commit_position can used to determine if the local
    // mirror peer client has

    if (!local_tag_data.predecessor.commit_valid) {

      // local mirror peer client 1) never committed, i.e., never written
      // data to, or 2) local journal for local mirror image was reset, i.e.,
      // we want a re-mirror

      // newly synced local image (no predecessor) replays from the first tag

      if (remote_tag_data.mirror_uuid != librbd::Journal<>::LOCAL_MIRROR_UUID) {
        dout(20) << ": skipping non-primary remote tag" << dendl;

        continue;
      }

      dout(20) << ": using initial primary remote tag" << dendl;

      break;
    }

    // local mirror peer client has a valid predecessor tag, i.e., ever
    // committed, next we are to check remote image's demotion/promotion chain

    if (local_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID) {

      // local mirror image currently in demoted state

      // demotion last available local epoch

      if (remote_tag_data.mirror_uuid == local_tag_data.mirror_uuid &&
          remote_tag_data.predecessor.commit_valid &&
          remote_tag_data.predecessor.tag_tid ==
            local_tag_data.predecessor.tag_tid) {

        // remote_tag_data.mirror_uuid == ORPHAN_MIRROR_UUID, i.e., remote
        // image was also in demoted state at this tag point
        // to check if:
        // 1) this remote demotion was replayed from the local image or
        // 2) the local demotion was replayed from the remote image

        // demotion matches remote epoch

        if (remote_tag_data.predecessor.mirror_uuid == m_local_mirror_uuid &&
            local_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {

          // the remote demotion was replayed from the local demotion, then
          // to check if the remote image promoted followed by the local
          // demotion

          // local demoted and remote has matching event

          dout(20) << ": found matching local demotion tag" << dendl;

          remote_orphan_tag_tid = remote_tag.tid;

          continue;
        }

        if (local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
            remote_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {

          // the local demotion was replayed from the remote demotion, then
          // to check if the remote image promoted again followed by its
          // demotion

          // remote demoted and local has matching event

          dout(20) << ": found matching remote demotion tag" << dendl;

          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }
      }

      if (remote_tag_data.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID &&
          remote_tag_data.predecessor.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
          remote_tag_data.predecessor.commit_valid && remote_orphan_tag_tid &&
          remote_tag_data.predecessor.tag_tid == *remote_orphan_tag_tid) {

        // remote image promoted again after the previous demotion event(either
        // demoted by administrator manually or replay from local demotion)

        // remote promotion tag chained to remote/local demotion tag

        dout(20) << ": found chained remote promotion tag" << dendl;

        reconnect_orphan = true;

        break;
      }

      // promotion must follow demotion
      remote_orphan_tag_tid = boost::none;
    } // local image in demoted state
  } // for (auto &remote_tag : m_remote_tags)

  if (remote_tag_data_valid &&
      local_tag_data.mirror_uuid == m_remote_mirror_uuid) {

    // ever decoded a remote tag, i.e., remote tags are not empty and
    // the last replaying has not finished yet, so continue

    // NOTE: for local mirror image the initial tag has mirror_uuid set
    // to m_remote_mirror_uuid, see
    // BootstrapRequest<I>::create_local_image -> librbd::image::CreateRequest<I>::journal_create

    // we are in replaying

    dout(20) << ": local image is in clean replay state" << dendl;
  } else if (reconnect_orphan) {

    // clean demotion/promotion

    dout(20) << ": remote image was demoted/promoted" << dendl;
  } else {
    derr << ": split-brain detected -- skipping image replay" << dendl;

    m_ret_val = -EEXIST;
    close_local_image();
    return;
  }

  // no split-brain detected

  image_sync();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {
    // clean replay state -- no image sync required
    close_remote_image();
    return;
  }

  dout(20) << dendl;
  {
    Mutex::Locker locker(m_lock);
    if (m_canceled) {
      m_ret_val = -ECANCELED;
    } else {
      assert(m_image_sync == nullptr);

      Context *ctx = create_context_callback<
        BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(this);
      m_image_sync = ImageSync<I>::create(
          *m_local_image_ctx, m_remote_image_ctx, m_timer, m_timer_lock,
          m_local_mirror_uuid, m_journaler, m_client_meta, m_work_queue,
          m_instance_watcher, ctx, m_progress_ctx);

      m_image_sync->get();

      m_lock.Unlock();
      update_progress("IMAGE_SYNC");
      m_lock.Lock();

      m_image_sync->send();
      return;
    }
  }

  dout(10) << ": request canceled" << dendl;
  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(20) << ": r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_image_sync->put();
    m_image_sync = nullptr;

    if (m_canceled) {
      dout(10) << ": request canceled" << dendl;
      m_ret_val = -ECANCELED;
    }

    if (r < 0) {
      derr << ": failed to sync remote image: " << cpp_strerror(r) << dendl;
      m_ret_val = r;
    }
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
    m_local_image_ctx, ctx);
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
    &m_remote_image_ctx, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing remote image: " << cpp_strerror(r)
         << dendl;
  }

  // call BaseRequest::finish
  finish(m_ret_val);
}

// called by
// BootstrapRequest<I>::handle_get_client
template <typename I>
void BootstrapRequest<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  // i.e., ImageReplayer::m_progress_cxt
  if (m_progress_ctx) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
