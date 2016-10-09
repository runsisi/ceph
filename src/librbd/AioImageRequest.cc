// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequest.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/journal/Types.h"
#include "include/rados/librados.hpp"
#include "common/WorkQueue.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequest: "

namespace librbd {

using util::get_image_ctx;

namespace {

template <typename ImageCtxT = ImageCtx>
struct C_DiscardJournalCommit : public Context {
  typedef std::vector<ObjectExtent> ObjectExtents;

  ImageCtxT &image_ctx;
  AioCompletion *aio_comp;
  ObjectExtents object_extents;

  C_DiscardJournalCommit(ImageCtxT &_image_ctx, AioCompletion *_aio_comp,
                         const ObjectExtents &_object_extents, uint64_t tid)
    : image_ctx(_image_ctx), aio_comp(_aio_comp),
      object_extents(_object_extents) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_DiscardJournalCommit: "
                   << "delaying cache discard until journal tid " << tid << " "
                   << "safe" << dendl;

    aio_comp->add_request();
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_DiscardJournalCommit: "
                   << "journal committed: discarding from cache" << dendl;

    Mutex::Locker cache_locker(image_ctx.cache_lock);
    image_ctx.object_cacher->discard_set(image_ctx.object_set, object_extents);
    aio_comp->complete_request(r);
  }
};

template <typename ImageCtxT = ImageCtx>
struct C_FlushJournalCommit : public Context {
  ImageCtxT &image_ctx;
  AioCompletion *aio_comp;

  C_FlushJournalCommit(ImageCtxT &_image_ctx, AioCompletion *_aio_comp,
                       uint64_t tid)
    : image_ctx(_image_ctx), aio_comp(_aio_comp) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_FlushJournalCommit: "
                   << "delaying flush until journal tid " << tid << " "
                   << "safe" << dendl;

    aio_comp->add_request();
  }

  virtual void finish(int r) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << this << " C_FlushJournalCommit: journal committed"
                   << dendl;
    aio_comp->complete_request(r);
  }
};

template <typename ImageCtxT>
class C_AioRead : public C_AioRequest {
public:
  C_AioRead(AioCompletion *completion)
    : C_AioRequest(completion), m_req(nullptr) {
  }

  virtual void finish(int r) {
    m_completion->lock.Lock();
    CephContext *cct = m_completion->ictx->cct;
    ldout(cct, 10) << "C_AioRead::finish() " << this << " r = " << r << dendl;

    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(cct, 10) << " got " << m_req->get_extent_map()
                     << " for " << m_req->get_buffer_extents()
                     << " bl " << m_req->data().length() << dendl;
      // reads from the parent don't populate the m_ext_map and the overlap
      // may not be the full buffer.  compensate here by filling in m_ext_map
      // with the read extent when it is empty.
      if (m_req->get_extent_map().empty()) {
        m_req->get_extent_map()[m_req->get_offset()] = m_req->data().length();
      }

      m_completion->destriper.add_partial_sparse_result(
          cct, m_req->data(), m_req->get_extent_map(), m_req->get_offset(),
          m_req->get_buffer_extents());
      r = m_req->get_length();
    }
    m_completion->lock.Unlock();

    C_AioRequest::finish(r);
  }

  void set_req(AioObjectRead<ImageCtxT> *req) {
    m_req = req;
  }
private:
  AioObjectRead<ImageCtxT> *m_req;
};

template <typename ImageCtxT>
class C_ImageCacheRead : public C_AioRequest {
public:
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  C_ImageCacheRead(AioCompletion *completion, const Extents &image_extents)
    : C_AioRequest(completion), m_image_extents(image_extents) {
  }

  inline bufferlist &get_data() {
    return m_bl;
  }

protected:
  virtual void finish(int r) {
    CephContext *cct = m_completion->ictx->cct;
    ldout(cct, 10) << "C_ImageCacheRead::finish() " << this << ": r=" << r
                   << dendl;
    if (r >= 0) {
      size_t length = 0;
      for (auto &image_extent : m_image_extents) {
        length += image_extent.second;
      }
      assert(length == m_bl.length());

      m_completion->lock.Lock();
      m_completion->destriper.add_partial_result(cct, m_bl, m_image_extents);
      m_completion->lock.Unlock();
      r = length;
    }

    C_AioRequest::finish(r);
  }

private:
  bufferlist m_bl;
  Extents m_image_extents;
};

template <typename ImageCtxT>
class C_ObjectCacheRead : public Context {
public:
  explicit C_ObjectCacheRead(ImageCtxT &ictx, AioObjectRead<ImageCtxT> *req)
    : m_image_ctx(ictx), m_req(req), m_enqueued(false) {}

  virtual void complete(int r) {
    if (!m_enqueued) {
      // cache_lock creates a lock ordering issue -- so re-execute this context
      // outside the cache_lock
      m_enqueued = true;
      m_image_ctx.op_work_queue->queue(this, r);
      return;
    }
    Context::complete(r);
  }

protected:
  virtual void finish(int r) {
    m_req->complete(r);
  }

private:
  ImageCtxT &m_image_ctx;
  AioObjectRead<ImageCtxT> *m_req;
  bool m_enqueued;
};

} // anonymous namespace

// static
// AioImageRequestWQ::aio_read will construct std::vector<std::pair<uint64_t,uint64_t> >
// from user provided <off, len>
template <typename I>
void AioImageRequest<I>::aio_read(I *ictx, AioCompletion *c,
                                  Extents &&image_extents, char *buf,
                                  bufferlist *pbl, int op_flags) {
  // std::vector<std::pair<uint64_t,uint64_t> >
  AioImageRead<I> req(*ictx, c, std::move(image_extents), buf, pbl, op_flags);
  req.send();
}

// static
template <typename I>
void AioImageRequest<I>::aio_write(I *ictx, AioCompletion *c, uint64_t off,
                                   size_t len, const char *buf, int op_flags) {
  AioImageWrite<I> req(*ictx, c, off, len, buf, op_flags);
  req.send();
}

// static
// never been used, AioImageRequestWQ::aio_write uses the above method,
// AioImageWrite has ctors accept <off, len> and std::vector<std::pair<uint64_t,uint64_t> >
template <typename I>
void AioImageRequest<I>::aio_write(I *ictx, AioCompletion *c,
                                   Extents &&image_extents, bufferlist &&bl,
                                   int op_flags) {
  AioImageWrite<I> req(*ictx, c, std::move(image_extents), std::move(bl),
                       op_flags);
  req.send();
}

// static
template <typename I>
void AioImageRequest<I>::aio_discard(I *ictx, AioCompletion *c,
                                     uint64_t off, uint64_t len) {
  AioImageDiscard<I> req(*ictx, c, off, len);
  req.send();
}

// static
template <typename I>
void AioImageRequest<I>::aio_flush(I *ictx, AioCompletion *c) {
  AioImageFlush<I> req(*ictx, c);
  req.send();
}

template <typename I>
void AioImageRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;

  assert(m_aio_comp->is_initialized(get_aio_type()));
  assert(m_aio_comp->is_started() ^ (get_aio_type() == AIO_TYPE_FLUSH));

  CephContext *cct = image_ctx.cct;
  AioCompletion *aio_comp = this->m_aio_comp;

  ldout(cct, 20) << get_request_type() << ": ictx=" << &image_ctx << ", "
                 << "completion=" << aio_comp <<  dendl;

  aio_comp->get();

  int r = clip_request();
  if (r < 0) {
    m_aio_comp->fail(r);
    return;
  }

  if (m_bypass_image_cache || m_image_ctx.image_cache == nullptr) {
    // overrided by AioImageRead, AbstractAioImageWrite, AioImageFlush
    send_request();
  } else {
    send_image_cache_request();
  }
}

// only AioImageFlush will override this and always return 0, becoz it does not
// have the <off, len> parameter pair
template <typename I>
int AioImageRequest<I>::clip_request() {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

  for (auto &image_extent : m_image_extents) {
    size_t clip_len = image_extent.second;

    // do not operate beyond the image size, especially check if the snapshost
    // we previously operated has been removed
    int r = clip_io(get_image_ctx(&m_image_ctx), image_extent.first, &clip_len);
    if (r < 0) {
      return r;
    }

    image_extent.second = clip_len;
  }

  return 0;
}

template <typename I>
void AioImageRequest<I>::fail(int r) {
  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->get();
  aio_comp->fail(r);
}

// called by AioImageRequest<I>::send when image cache bypassed or disabled (never enabled)
template <typename I>
void AioImageRead<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  auto &image_extents = this->m_image_extents;

  if (image_ctx.object_cacher && image_ctx.readahead_max_bytes > 0 &&
      !(m_op_flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
    readahead(get_image_ctx(&image_ctx), image_extents);
  }

  AioCompletion *aio_comp = this->m_aio_comp;
  librados::snap_t snap_id;
  map<object_t,vector<ObjectExtent> > object_extents;
  uint64_t buffer_ofs = 0;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    snap_id = image_ctx.snap_id;

    // map image extents to object extents
    for (auto &extent : image_extents) {

      // there should be only 1 extent, becoz rbd_aio_read only accepts <off, len> parameter

      if (extent.second == 0) {
        // if the io beyonds the image size, clip_request called previously will
        // truncate the requested extent(s)
        continue;
      }

      Striper::file_to_extents(cct, image_ctx.format_string, &image_ctx.layout,
                               extent.first, extent.second, 0, object_extents,
                               buffer_ofs);

      buffer_ofs += extent.second;
    }
  }

  aio_comp->read_buf = m_buf;
  aio_comp->read_buf_len = buffer_ofs;
  aio_comp->read_bl = m_pbl;

  // pre-calculate the expected number of read requests
  uint32_t request_count = 0;
  // map<object_t,vector<ObjectExtent> >
  for (auto &object_extent : object_extents) {
    request_count += object_extent.second.size();
  }

  // used by AioCompletion::complete_request to test if the AioImageRequest
  // has completed
  aio_comp->set_request_count(request_count);

  // issue the requests
  for (auto &object_extent : object_extents) {
    for (auto &extent : object_extent.second) {
      ldout(cct, 20) << " oid " << extent.oid << " " << extent.offset << "~"
                     << extent.length << " from " << extent.buffer_extents
                     << dendl;

      C_AioRead<I> *req_comp = new C_AioRead<I>(aio_comp);
      AioObjectRead<I> *req = AioObjectRead<I>::create(
        &image_ctx, extent.oid.name, extent.objectno, extent.offset,
        extent.length, extent.buffer_extents, snap_id, true, req_comp,
        m_op_flags);

      // associate object request and object request completion
      req_comp->set_req(req);

      if (image_ctx.object_cacher) {
        // object cacher enabled, try to read from cache
        C_ObjectCacheRead<I> *cache_comp = new C_ObjectCacheRead<I>(image_ctx,
                                                                    req);
        image_ctx.aio_read_from_cache(extent.oid, extent.objectno,
                                      &req->data(), extent.length,
                                      extent.offset, cache_comp, m_op_flags);
      } else {

        // read from backend directly

        req->send();
      }
    }
  }

  aio_comp->put();

  image_ctx.perfcounter->inc(l_librbd_rd);
  image_ctx.perfcounter->inc(l_librbd_rd_bytes, buffer_ofs);
}

// called by AioImageRequest<I>::send when image cache enabled, actually it never enabled
template <typename I>
void AioImageRead<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;

  aio_comp->set_request_count(1);

  C_ImageCacheRead<I> *req_comp = new C_ImageCacheRead<I>(
    aio_comp, this->m_image_extents);

  image_ctx.image_cache->aio_read(std::move(this->m_image_extents),
                                  &req_comp->get_data(), m_op_flags,
                                  req_comp);
}

// called by AioImageRequest<I>::send when image cache bypassed or disabled (never enabled)
template <typename I>
void AbstractAioImageWrite<I>::send_request() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  // image extents -> std::vector<ObjectExtent>
  // AioImageRequest -> AioObjectRequest
  // append journal event
  // write object cache

  RWLock::RLocker md_locker(image_ctx.md_lock);

  bool journaling = false;

  AioCompletion *aio_comp = this->m_aio_comp;
  uint64_t clip_len = 0;

  // std::vector<ObjectExtent>, AioImageRequest will be divided into AioObjectRequest(s),
  // each AioObjectRequest is identified by an ObjectExtent
  ObjectExtents object_extents;

  ::SnapContext snapc;

  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(image_ctx.snap_lock);

    if (image_ctx.snap_id != CEPH_NOSNAP || image_ctx.read_only) {
      aio_comp->fail(-EROFS);
      return;
    }

    for (auto &extent : this->m_image_extents) {

      // there should be only 1 extent, becoz rbd_aio_write only accepts <off, len> parameter

      if (extent.second == 0) {
        // clip_request thinks this is a valid request
        continue;
      }

      // map to object extents
      // <off, len> in image -> std::vector<ObjectExtent>
      Striper::file_to_extents(cct, image_ctx.format_string, &image_ctx.layout,
                               extent.first, extent.second, 0, object_extents);

      // used for updating perf counter, the original request may have been clipped, the
      // requested length after clipping is not known directly
      clip_len += extent.second;
    }

    snapc = image_ctx.snapc;

    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  // only implemented by AioImageDiscard, to skip discarding extents that
  // are not on the object trailing border
  prune_object_extents(object_extents);

  if (!object_extents.empty()) {

    // AioImageWrite or AioImageDiscard, has block data to write or discard

    uint64_t journal_tid = 0;

    // set AioCompletion::pending_count
    // for AioImageDiscard, if objecter cache and journaling enabled, then +1
    // object request count, i.e., an extra ref to AioCompletion for
    // waiting journaled AioDiscardEvent finish
    aio_comp->set_request_count(
      object_extents.size() + get_object_cache_request_count(journaling));

    AioObjectRequests requests;

    // divide AioImageRequest into AioObjectRequest(s) and stash or send them
    // depends on whether we are journaling the AioImageRequest or not

    // NOTE: for AioImageWrite if the object cacher is enabled, we do nothing,
    // we will delay the whole process and delegate to object cacher
    send_object_requests(object_extents, snapc,
                         (journaling ? &requests : nullptr));

    if (journaling) {
      // in-flight ops are flushed prior to closing the journal
      assert(image_ctx.journal != NULL);

      // append AioWriteEvent(s) or AioDiscardEvent journal entry and
      // associate the journal::Event id with the AioCompletion,
      // AioCompletion::complete will use this id to inform the journal
      // that the AioImageRequest has been completed, i.e., committed

      // NOTE: for AioImageWrite, if object cacher is enabled, then 'requests'
      // should be empty, bc we did nothing in 'send_object_requests', and
      // we will not associate the journal event id with the AioCompletion either,
      // i.e., we only do journaling, nothing else, journal::Event is not associated
      // with AioObjectRequest(s) and AioCompletion is not associated with
      // the journal::Event id

      // the m_synchronous field will never be set, so always be false
      // journal::Event is created on each image extent
      // Journal<I>::handle_io_event_safe will send the AioObjectRequest(s), i.e., requests
      journal_tid = append_journal_event(requests, m_synchronous);
    }

    if (image_ctx.object_cacher != NULL) {

      // object cacher enabled

      // for AioImageWrite, AioObjectRequest(s) have not been created, journaling
      // only recorded the AioImageWrite, i.e., the user IO, now delegate the
      // write to objecter cache and pass the journaled Event id to it

      // for AioImageDiscard, AioObjectRequests(s) and journaling have been
      // ready,

      send_object_cache_requests(object_extents, journal_tid);
    }
  } else {
    // no IO to perform -- fire completion
    aio_comp->unblock();
  }

  // update perf counter
  update_stats(clip_len);

  aio_comp->put();
}

// called by AbstractAioImageWrite<I>::send_request
template <typename I>
void AbstractAioImageWrite<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    AioObjectRequests *aio_object_requests) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  AioCompletion *aio_comp = this->m_aio_comp;

  for (ObjectExtents::const_iterator p = object_extents.begin();
       p != object_extents.end(); ++p) {

    // an AioImageRequest may be divided into multiple AioObjectRequest(s)

    ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;

    // callback for each AioObjectRequest, will call m_completion->complete_request
    // to try to complete the AioImageRequest
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);

    // to allocate AioObjectRequest, i.e., AioObjectWrite, AioObjectRemove,
    // AioObjectTruncate, AioObjectZero, for AioImageWrite or AioImageDiscard
    AioObjectRequestHandle *request = create_object_request(*p, snapc,
                                                            req_comp);

    if (request != NULL) {

      // actually, the request should not be NULL, it should always be
      // one of AioObjectWrite, AioObjectRemove, AioObjectTruncate,
      // AioObjectZero

      if (aio_object_requests != NULL) {

        // journaling enabled, the caller want to stash the requests
        // instead of sending it directly

        aio_object_requests->push_back(request);
      } else {
        request->send();
      }
    }
  }
}

template <typename I>
void AioImageWrite<I>::assemble_extent(const ObjectExtent &object_extent,
                                    bufferlist *bl) {
  for (auto q = object_extent.buffer_extents.begin();
       q != object_extent.buffer_extents.end(); ++q) {
    bufferlist sub_bl;
    sub_bl.substr_of(m_bl, q->first, q->second);
    bl->claim_append(sub_bl);
  }
}

// called by AbstractAioImageWrite<I>::send_request
// synchronous always be false
template <typename I>
uint64_t AioImageWrite<I>::append_journal_event(
    const AioObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid;
  uint64_t buffer_offset = 0;

  assert(!this->m_image_extents.empty());

  for (auto &extent : this->m_image_extents) {

    // each image extent has an journal::Event

    bufferlist sub_bl;

    // <off, len> in AioImageRequest::m_bl, i.e., user provided buffer
    sub_bl.substr_of(m_bl, buffer_offset, extent.second);

    buffer_offset += extent.second;

    // <off, len> in image, an user provided AioImageWrite extent may fit into
    // multiple journal::EventEntry(s), and fit into one journal::Event
    // an tid identifies an journal::Event
    tid = image_ctx.journal->append_write_event(extent.first, extent.second,
                                                sub_bl, requests, synchronous);
  }

  // if object cacher enabled, then the writeback handler will do this
  if (image_ctx.object_cacher == NULL) {
    AioCompletion *aio_comp = this->m_aio_comp;

    // when AioCompletion completed it will notify the journal
    aio_comp->associate_journal_event(tid);
  }

  // return the last journal::Event id, if we have multiple image extent to
  // write, which is not true in current implementation, see rbd_aio_write
  return tid;
}

// called by AioImageRequest<I>::send when image cache enabled, actually it never enabled
template <typename I>
void AioImageWrite<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;

  aio_comp->set_request_count(1);

  C_AioRequest *req_comp = new C_AioRequest(aio_comp);

  image_ctx.image_cache->aio_write(std::move(this->m_image_extents),
                                   std::move(m_bl), m_op_flags, req_comp);
}

// called by AbstractAioImageWrite<I>::send_request when object cacher enabled
template <typename I>
void AioImageWrite<I>::send_object_cache_requests(const ObjectExtents &object_extents,
                                                  uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;

  for (auto p = object_extents.begin(); p != object_extents.end(); ++p) {

    // an AioImageRequest may be divided into multiple AioObjectRequest(s)

    const ObjectExtent &object_extent = *p;

    bufferlist bl;

    // AioImageWrite::m_bl -> bl
    assemble_extent(object_extent, &bl);

    AioCompletion *aio_comp = this->m_aio_comp;

    // m_completion->complete_request
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);

    // object cache enabled, so AioCompletion has not been associated with
    // the journal::Event id yet, see AioImageWrite<I>::append_journal_event
    image_ctx.write_to_cache(object_extent.oid, bl, object_extent.length,
                             object_extent.offset, req_comp, m_op_flags,
                               journal_tid);
  }
}

template <typename I>
void AioImageWrite<I>::send_object_requests(
    const ObjectExtents &object_extents, const ::SnapContext &snapc,
    AioObjectRequests *aio_object_requests) {
  I &image_ctx = this->m_image_ctx;

  // cache handles creating object requests during writeback
  if (image_ctx.object_cacher == NULL) {
    AbstractAioImageWrite<I>::send_object_requests(object_extents, snapc,
                                                aio_object_requests);
  }
}

// called by AbstractAioImageWrite<I>::send_object_requests
template <typename I>
AioObjectRequestHandle *AioImageWrite<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  // if object cacher enabled, AioImageWrite will not create object requests, instead
  // the object cacher will do all these things
  assert(image_ctx.object_cacher == NULL);

  bufferlist bl;

  // object_extent->buffer_extents denotes vector<pair<uint64_t,uint64_t> > in image
  assemble_extent(object_extent, &bl);

  AioObjectRequest<I> *req = AioObjectRequest<I>::create_write(
    &image_ctx, object_extent.oid.name, object_extent.objectno,
    object_extent.offset, bl, snapc, on_finish, m_op_flags);

  return req;
}

template <typename I>
void AioImageWrite<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_wr);
  image_ctx.perfcounter->inc(l_librbd_wr_bytes, length);
}

// synchronous always be false
template <typename I>
uint64_t AioImageDiscard<I>::append_journal_event(
    const AioObjectRequests &requests, bool synchronous) {
  I &image_ctx = this->m_image_ctx;

  uint64_t tid;

  assert(!this->m_image_extents.empty());

  for (auto &extent : this->m_image_extents) {
    journal::EventEntry event_entry(journal::AioDiscardEvent(extent.first,
                                                             extent.second));

    // an journal::Event
    tid = image_ctx.journal->append_io_event(std::move(event_entry),
                                             requests, extent.first,
                                             extent.second, synchronous);
  }

  AioCompletion *aio_comp = this->m_aio_comp;

  // when AioCompletion completed it will notify the journal
  aio_comp->associate_journal_event(tid);

  return tid;
}

template <typename I>
void AioImageDiscard<I>::prune_object_extents(ObjectExtents &object_extents) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  
  // default false
  if (!cct->_conf->rbd_skip_partial_discard) {
    return;
  }

  // AioImageDiscard will create mutiple AioObjectRemove, AioObjectTruncate or
  // AioObjectZero, if rbd_skip_partial_discard is enabled, we do not create
  // AioObjectZero

  for (auto p = object_extents.begin(); p != object_extents.end(); ) {
    // do not zero the trailing part of the object
    if (p->offset + p->length < image_ctx.layout.object_size) {

      ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~"
		     << p->length << " from " << p->buffer_extents
		     << ": skip partial discard" << dendl;

      p = object_extents.erase(p);
    } else {
      ++p;
    }
  }
}

template <typename I>
uint32_t AioImageDiscard<I>::get_object_cache_request_count(bool journaling) const {
  // extra completion request is required for tracking journal commit
  I &image_ctx = this->m_image_ctx;
  return (image_ctx.object_cacher != nullptr && journaling ? 1 : 0);
}

template <typename I>
void AioImageDiscard<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(this->m_image_extents.size());
  for (auto &extent : this->m_image_extents) {
    C_AioRequest *req_comp = new C_AioRequest(aio_comp);
    image_ctx.image_cache->aio_discard(extent.first, extent.second, req_comp);
  }
}

template <typename I>
void AioImageDiscard<I>::send_object_cache_requests(const ObjectExtents &object_extents,
                                                    uint64_t journal_tid) {
  I &image_ctx = this->m_image_ctx;

  if (journal_tid == 0) {

    // journaling disabled, AioObjectRequest(s) have been sent directly
    // by AbstractAioImageWrite<I>::send_object_requests

    Mutex::Locker cache_locker(image_ctx.cache_lock);

    image_ctx.object_cacher->discard_set(image_ctx.object_set,
                                         object_extents);
  } else {
    // cannot discard from cache until journal has committed

    // AioObjectRequest(s) have been stashed and recorded in journal::Event,
    // the journal::Event id is

    assert(image_ctx.journal != NULL);

    AioCompletion *aio_comp = this->m_aio_comp;

    // push callback back of journal::Event::on_safe_contexts, so will
    // call m_image_ctx.object_cacher->discard_set until journaled AioImageRequest,
    // i.e., user IO, be safe
    image_ctx.journal->wait_event(
      journal_tid, new C_DiscardJournalCommit<I>(image_ctx, aio_comp,
                                                 object_extents, journal_tid));
  }
}

// called by AbstractAioImageWrite<I>::send_object_requests
template <typename I>
AioObjectRequestHandle *AioImageDiscard<I>::create_object_request(
    const ObjectExtent &object_extent, const ::SnapContext &snapc,
    Context *on_finish) {
  I &image_ctx = this->m_image_ctx;

  AioObjectRequest<I> *req;

  if (object_extent.length == image_ctx.layout.object_size) {
    req = AioObjectRequest<I>::create_remove(
      &image_ctx, object_extent.oid.name, object_extent.objectno, snapc,
      on_finish);
  } else if (object_extent.offset + object_extent.length ==
               image_ctx.layout.object_size) {
    req = AioObjectRequest<I>::create_truncate(
      &image_ctx, object_extent.oid.name, object_extent.objectno,
      object_extent.offset, snapc, on_finish);
  } else {
    req = AioObjectRequest<I>::create_zero(
      &image_ctx, object_extent.oid.name, object_extent.objectno,
      object_extent.offset, object_extent.length, snapc, on_finish);
  }

  return req;
}

template <typename I>
void AioImageDiscard<I>::update_stats(size_t length) {
  I &image_ctx = this->m_image_ctx;
  image_ctx.perfcounter->inc(l_librbd_discard);
  image_ctx.perfcounter->inc(l_librbd_discard_bytes, length);
}

template <typename I>
void AioImageFlush<I>::send_request() {
  I &image_ctx = this->m_image_ctx;

  image_ctx.user_flushed();

  bool journaling = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    journaling = (image_ctx.journal != nullptr &&
                  image_ctx.journal->is_journal_appending());
  }

  AioCompletion *aio_comp = this->m_aio_comp;

  if (journaling) {
    // in-flight ops are flushed prior to closing the journal
    uint64_t journal_tid = image_ctx.journal->append_io_event(
      journal::EventEntry(journal::AioFlushEvent()),
      AioObjectRequests(), 0, 0, false);

    aio_comp->set_request_count(1);
    aio_comp->associate_journal_event(journal_tid);

    FunctionContext *flush_ctx = new FunctionContext(
      [aio_comp, &image_ctx, journal_tid] (int r) {
        C_FlushJournalCommit<I> *ctx = new C_FlushJournalCommit<I>(image_ctx,
                                                                 aio_comp,
                                                                 journal_tid);
        image_ctx.journal->flush_event(journal_tid, ctx);

        // track flush op for block writes
        aio_comp->start_op(true);
        aio_comp->put();
    });

    image_ctx.flush_async_operations(flush_ctx);
  } else {
    // flush rbd cache only when journaling is not enabled
    aio_comp->set_request_count(1);

    C_AioRequest *req_comp = new C_AioRequest(aio_comp);

    image_ctx.flush(req_comp);

    aio_comp->start_op(true);
    aio_comp->put();
  }

  image_ctx.perfcounter->inc(l_librbd_aio_flush);
}

template <typename I>
void AioImageFlush<I>::send_image_cache_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.image_cache != nullptr);

  AioCompletion *aio_comp = this->m_aio_comp;
  aio_comp->set_request_count(1);
  C_AioRequest *req_comp = new C_AioRequest(aio_comp);
  image_ctx.image_cache->aio_flush(req_comp);
}

} // namespace librbd

template class librbd::AioImageRequest<librbd::ImageCtx>;
template class librbd::AbstractAioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageWrite<librbd::ImageCtx>;
template class librbd::AioImageDiscard<librbd::ImageCtx>;
template class librbd::AioImageFlush<librbd::ImageCtx>;
