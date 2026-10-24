// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "client/ZeroCopyClient.h"

#include <errno.h>

#include <mutex>

#include "client/Client.h"
#include "client/ClientSnapRealm.h"
#include "client/Fh.h"
#include "client/Inode.h"
#include "client/inode_lock.h"
#include "common/Clock.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "include/ceph_fs.h"
#include "osdc/Filer.h"
#include "osdc/Objecter.h"

namespace ceph {

void ZeroCopyClient::queue_user_cb(Client* clnt, IoCallback cb, int64_t result,
                                   bufferlist data)
{
  ceph_assert(clnt);
  clnt->objecter_finisher.queue(make_lambda_context(
      [cb = std::move(cb), result, data = std::move(data)](int) mutable {
        cb(IoResult{result, std::move(data)});
      }));
}

/**
 * Filer read completion. Objecter may invoke without locks; we take inode_lock
 * (like C_Read_Sync_NonBlocking::finish_locked), finish short-read handling,
 * drop FILE_RD, unlock, then run the user callback.
 */
class C_ZC_Read : public Context {
public:
  Client* clnt;
  Fh* fh;
  InodeRef in;
  uint64_t off;
  uint64_t len;
  bufferlist* bl;
  IoCallback cb;
  uint64_t pos;
  uint64_t left;
  uint64_t wanted;
  uint64_t read_bytes = 0;
  bufferlist tbl;
  Filer* filer;
  file_layout_t layout;
  snapid_t snapid;
  uint64_t truncate_size;
  uint32_t truncate_seq;
  bool fini = false; // like C_Read_Sync_NonBlocking: survive short-read retries

  C_ZC_Read(Client* clnt, Fh* fh, InodeRef in, uint64_t off, uint64_t len,
            bufferlist* bl, IoCallback cb, Filer* filer, file_layout_t layout,
            snapid_t snapid, uint64_t truncate_size, uint32_t truncate_seq) :
    clnt(clnt), fh(fh), in(std::move(in)), off(off), len(len), bl(bl),
    cb(std::move(cb)), pos(off), left(len), wanted(len), filer(filer),
    layout(std::move(layout)), snapid(snapid), truncate_size(truncate_size),
    truncate_seq(truncate_seq)
  {
    fh->get();
  }

  ~C_ZC_Read() override
  {
    delete bl;
    // finish() unlocks before the user callback, then complete() deletes us
    // without inode_lock. Need _put_fh under inode_lock (Fh::~Fh touches
    // inode->fhs).
    std::unique_lock in_lock(*in);
    clnt->_put_fh(fh);
  }

  void start() { retry(); }

  void retry()
  {
    // Queue through objecter_finisher so finish() does not take inode_lock
    // under Objecter's completion_lock (same ordering as C_Read_Sync_NonBlocking).
    filer->read_trunc(in->ino, &layout, snapid, pos, left, &tbl, 0,
                      truncate_size, truncate_seq,
                      new C_OnFinisher(this, &clnt->objecter_finisher));
  }

  // Default Context::complete always deletes this; short-read retries re-submit
  // the same object to Filer, so only destroy when finish sets fini.
  void complete(int r) override
  {
    finish(r);
    if (fini) {
      delete this;
    }
  }

  void finish(int r) override
  {
    std::unique_lock in_lock(*in);

    if (r == -ENOENT) {
      r = 0;
    } else if (r < 0) {
      goto done;
    }

    if (tbl.length()) {
      r = tbl.length();
      read_bytes += r;
      pos += r;
      left -= r;
      bl->claim_append(tbl);
    }

    if (r >= 0 && static_cast<uint64_t>(r) < wanted) {
      // Short OSD read: zero up to known EOF like C_Read_Sync_NonBlocking,
      // then re-check size. Retry the remainder only if the file grew / we
      // have not reached EOF yet.
      auto effective_size = in->effective_size();
      if (pos < effective_size) {
        int64_t some = effective_size - pos;
        if (static_cast<uint64_t>(some) > left) {
          some = left;
        }
        auto z = buffer::ptr_node::create(some);
        z->zero();
        bl->push_back(std::move(z));
        read_bytes += some;
        pos += some;
        left -= some;
        if (left == 0) {
          r = read_bytes;
          goto done;
        }
      }

      {
        // _getattr drops inode_lock across make_request if held.
        int gr = clnt->_getattr(in, CEPH_STAT_CAP_SIZE, fh->actor_perms);
        if (gr < 0) {
          r = gr;
          goto done;
        }
      }

      effective_size = in->effective_size();
      if (pos >= effective_size || left == 0) {
        r = read_bytes;
        goto done;
      }

      wanted = left;
      truncate_size = in->truncate_size;
      truncate_seq = in->truncate_seq;
      // Drop inode_lock before re-submit (match C_Read_Sync_NonBlocking).
      in_lock.unlock();
      retry();
      return;
    }

    if (r >= 0) {
      r = read_bytes;
    }

  done:
    fini = true;
    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_RD);
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    IoCallback user_cb = std::move(cb);
    bufferlist out;
    if (r >= 0) {
      out = std::move(*bl);
    }
    in_lock.unlock();
    user_cb(IoResult{r, std::move(out)});
  }
};

/** Optional post-write fsync; runs under inode_lock (advance() holds it). */
class C_ZC_Fsync : public Context {
public:
  Client* clnt;
  InodeRef in;
  IoCallback cb;
  int64_t io_result;

  C_ZC_Fsync(Client* clnt, InodeRef in, IoCallback cb, int64_t io_result) :
    clnt(clnt), in(std::move(in)), cb(std::move(cb)), io_result(io_result)
  {}

  void finish(int r) override
  {
    ceph_assert(ceph_mutex_is_locked_by_me(*in));
    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_WR);
    // Match C_Write_Finisher: count the op complete only after optional fsync.
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    ZeroCopyClient::queue_user_cb(clnt, std::move(cb), r < 0 ? r : io_result,
                                  {});
  }
};

/**
 * Non-blocking fsync slot acquisition for ZeroCopy writes.
 *
 * Never call Client::_fsync_enter() on objecter_finisher: an in-flight fsync
 * may wait for FILE_BUFFER refs held by other C_ZC_Write contexts still queued
 * on the same finisher (same deadlock C_Write_Finisher::kickoff_nonblocking_fsync
 * avoids). Own @c fsync_ctx so C_ZC_Write can be deleted after deferral.
 */
class C_ZC_FsyncSlotWaiter : public Context {
public:
  Client* clnt;
  InodeRef in;
  bool syncdataonly;
  Context* fsync_ctx; // C_ZC_Fsync*; owned until handed to fsync state

  C_ZC_FsyncSlotWaiter(Client* clnt, InodeRef in, bool syncdataonly,
                       Context* fsync_ctx) :
    clnt(clnt), in(std::move(in)), syncdataonly(syncdataonly),
    fsync_ctx(fsync_ctx)
  {}

  ~C_ZC_FsyncSlotWaiter() override
  {
    // Should have transferred ownership in kickoff(); fail-safe if not.
    if (fsync_ctx) {
      fsync_ctx->complete(-ECANCELED);
      fsync_ctx = nullptr;
    }
  }

  void finish(int r) override
  {
    std::unique_lock in_lock(*in);
    // If deferred again, a fresh waiter is queued and owns fsync_ctx; *this*
    // is deleted by Context::complete after we return.
    (void)kickoff();
  }

  /**
   * Caller must hold inode_lock. May drop/reacquire around try_enter.
   * @return true if fsync was started (caller may delete this); false if
   *         deferred (a waiter now owns fsync_ctx — do not delete that waiter).
   */
  bool kickoff()
  {
    ceph_assert(ceph_mutex_is_locked_by_me(*in));
    ceph_assert(fsync_ctx);

    Inode* inp = in.get();
    ceph::unique_unlock<Inode> in_drop(*inp);
    // Never pass *this* while a caller may still delete us (C_ZC_Write path).
    auto* waiter = new C_ZC_FsyncSlotWaiter(clnt, in, syncdataonly, fsync_ctx);
    fsync_ctx = nullptr;
    if (!clnt->_fsync_try_enter(inp, waiter)) {
      // waiter owns fsync_ctx; will retry via finish() when the slot frees
      return false;
    }
    Context* ctx = waiter->fsync_ctx;
    waiter->fsync_ctx = nullptr;
    delete waiter;

    auto* state = new Client::C_nonblocking_fsync_state(
        clnt, inp, syncdataonly, ctx, true /* fsync_slot_held */);
    in_drop.reacquire();
    state->advance();
    return true;
  }
};

/**
 * Filer write completion under inode_lock (like C_Write_Finisher::finish_io_complete).
 * Updates size, drops BUFFER/WR (WR after optional fsync), then user cb.
 * Holds Client* only — never ZeroCopyClient* (stack wrappers must not be pinned).
 */
class C_ZC_Write : public Context {
public:
  Client* clnt;
  Fh* fh;
  InodeRef in;
  utime_t start;
  uint64_t offset;
  uint64_t size;
  uint64_t fpos; // 0 = do not update fh pos
  bool do_fsync;
  bool syncdataonly;
  IoCallback cb;
  bufferlist* held_bl; // keeps payload alive for Objecter

  C_ZC_Write(Client* clnt, Fh* fh, InodeRef in, utime_t start, uint64_t offset,
             uint64_t size, uint64_t fpos, bool do_fsync, bool syncdataonly,
             IoCallback cb, bufferlist* held_bl) :
    clnt(clnt), fh(fh), in(std::move(in)), start(start), offset(offset),
    size(size), fpos(fpos), do_fsync(do_fsync), syncdataonly(syncdataonly),
    cb(std::move(cb)), held_bl(held_bl)
  {
    fh->get();
  }

  ~C_ZC_Write() override
  {
    delete held_bl;
    std::unique_lock in_lock(*in);
    clnt->_put_fh(fh);
  }

  void finish(int r) override
  {
    std::unique_lock in_lock(*in);

    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_BUFFER);

    int64_t result = r;
    if (r >= 0) {
      result = clnt->_write_success(fh, start, fpos, offset, size, offset, size,
                                    in.get(), false);
    }

    if (r >= 0 && do_fsync) {
      // Match C_Write_Finisher::kickoff_nonblocking_fsync — never block the
      // objecter_finisher on _fsync_enter (deadlocks with FILE_BUFFER waiters
      // still queued on this finisher).
      auto* fsync_ctx = new C_ZC_Fsync(clnt, in, std::move(cb), result);
      auto* starter = new C_ZC_FsyncSlotWaiter(clnt, in, syncdataonly,
                                               fsync_ctx);
      // kickoff always clears starter->fsync_ctx (moved to state or a queued
      // waiter), so starter is safe to delete either way.
      (void)starter->kickoff();
      delete starter;
      return;
    }

    clnt->put_cap_ref(in.get(), CEPH_CAP_FILE_WR);
    clnt->logger->inc(l_c_aio_completions);
    clnt->dec_aio_in_flight();
    ZeroCopyClient::queue_user_cb(clnt, std::move(cb), result, {});
  }
};

ZeroCopyClient::ZeroCopyClient(Client* client) : client(client) {}

bufferlist ZeroCopyClient::alloc_buffers(size_t len)
{
  bufferlist bl;
  if (len == 0) {
    return bl;
  }
  bl.push_back(bufferptr(buffer::create_page_aligned(len)));
  return bl;
}

bufferlist ZeroCopyClient::wrap_static(char* buf, size_t len)
{
  if (len == 0 || buf == nullptr) {
    return bufferlist();
  }
  return bufferlist::static_from_mem(buf, len);
}

bufferlist ZeroCopyClient::wrap_static(const struct iovec* iov, int iovcnt)
{
  bufferlist bl;
  if (iov == nullptr || iovcnt <= 0) {
    return bl;
  }
  for (int i = 0; i < iovcnt; ++i) {
    if (iov[i].iov_base == nullptr || iov[i].iov_len == 0) {
      continue;
    }
    bl.push_back(buffer::create_static(iov[i].iov_len,
                                       static_cast<char*>(iov[i].iov_base)));
  }
  return bl;
}

int ZeroCopyClient::bufferlist_to_iovec(const bufferlist& bl, struct iovec* iov,
                                        int max_iov)
{
  if (iov == nullptr && bl.get_num_buffers() > 0) {
    return -EINVAL;
  }
  if (max_iov < 0) {
    return -EINVAL;
  }

  int n = 0;
  for (const auto& p : bl.buffers()) {
    if (p.length() == 0) {
      continue;
    }
    if (n >= max_iov) {
      return -ERANGE;
    }
    iov[n].iov_base = const_cast<char*>(p.c_str());
    iov[n].iov_len = p.length();
    ++n;
  }
  return n;
}

int ZeroCopyClient::async_read(Fh* fh, uint64_t off, uint64_t len, IoCallback cb)
{
  if (!client || !cb) {
    return -EINVAL;
  }

  Client::RWRef_t mref_reader(client->mount_state, Client::CLIENT_MOUNTING);
  if (!mref_reader.is_state_satisfied()) {
    queue_user_cb(client, std::move(cb), -ENOTCONN, {});
    return 0;
  }

  // _ll_fh_exists takes client_lock; must not hold inode_lock yet
  // (client_lock → inode_lock is forbidden).
  if (fh == nullptr || !client->_ll_fh_exists(fh)) {
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if ((fh->mode & CEPH_FILE_MODE_RD) == 0) {
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if (len == 0) {
    queue_user_cb(client, std::move(cb), 0, {});
    return 0;
  }

  Inode* in = fh->inode.get();
  std::unique_lock in_lock(*in);

  int have = 0;
  // Prefer CACHE for issue; still always use Filer (no ObjectCacher).
  int r = client->get_caps(fh, CEPH_CAP_FILE_RD, CEPH_CAP_FILE_CACHE, &have, -1);
  if (r < 0) {
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }

  auto effective_size = in->effective_size();
  if (off >= effective_size) {
    client->put_cap_ref(in, CEPH_CAP_FILE_RD);
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), 0, {});
    return 0;
  }
  if (off + len > effective_size) {
    len = effective_size - off;
  }

  auto* bl = new bufferlist();
  auto* read_ctx = new C_ZC_Read(client, fh, InodeRef(in), off, len, bl,
                                 std::move(cb), client->filer.get(), in->layout,
                                 in->snapid, in->truncate_size,
                                 in->truncate_seq);

  // Match _read / C_Read_Finisher: count from Filer submit through final
  // completion (short-read retries stay a single in-flight op).
  client->logger->inc(l_c_aio_ops);
  client->inc_aio_in_flight();

  in_lock.unlock();
  read_ctx->start();
  return 0;
}

int ZeroCopyClient::async_write(Fh* fh, uint64_t off, bufferlist data,
                                IoCallback cb, bool fsync, bool syncdataonly)
{
  if (!client || !cb) {
    return -EINVAL;
  }

  const uint64_t size = data.length();
  if (size < 1) {
    return -EINVAL;
  }

  Client::RWRef_t mref_reader(client->mount_state, Client::CLIENT_MOUNTING);
  if (!mref_reader.is_state_satisfied()) {
    queue_user_cb(client, std::move(cb), -ENOTCONN, {});
    return 0;
  }

  if (fh == nullptr || !client->_ll_fh_exists(fh)) {
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }
  if ((fh->mode & CEPH_FILE_MODE_WR) == 0) {
    queue_user_cb(client, std::move(cb), -EBADF, {});
    return 0;
  }

  Inode* in = fh->inode.get();
  std::unique_lock in_lock(*in);

  if (in->snapid != CEPH_NOSNAP) {
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), -EROFS, {});
    return 0;
  }

  uint64_t offset = off;
  uint64_t fpos = 0;
  if (static_cast<int64_t>(off) < 0) {
    client->lock_fh_pos(fh);
    if (fh->flags & O_APPEND) {
      auto lr = client->_lseek(fh, 0, SEEK_END);
      if (lr < 0) {
        client->unlock_fh_pos(fh);
        in_lock.unlock();
        queue_user_cb(client, std::move(cb), lr, {});
        return 0;
      }
    }
    offset = fh->pos;
    fpos = offset + size;
    client->unlock_fh_pos(fh);
  }

  if ((offset + size) > client->mdsmap->get_max_filesize() &&
      (offset + size) > in->size) {
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), -EFBIG, {});
    return 0;
  }
  if (client->objecter->osdmap_pool_full(in->layout.pool_id)) {
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), -ENOSPC, {});
    return 0;
  }

  uint64_t endoff = offset + size;
  if (endoff > in->size) {
    const int64_t new_bytes = endoff - in->size;
    {
      ceph::unique_unlock<Inode> in_drop(*in);
      if (client->is_quota_bytes_exceeded(in, new_bytes, fh->actor_perms)) {
        queue_user_cb(client, std::move(cb), -EDQUOT, {});
        return 0;
      }
    }
  }

  utime_t start = mono_clock_now();

  int have = 0;
  int r = client->get_caps(fh, CEPH_CAP_FILE_WR | CEPH_CAP_AUTH_SHARED,
                           CEPH_CAP_FILE_BUFFER, &have, endoff);
  if (r < 0) {
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }
  client->put_cap_ref(in, CEPH_CAP_AUTH_SHARED);

  r = client->clear_suid_sgid(in, fh->actor_perms);
  if (r < 0) {
    client->put_cap_ref(in, CEPH_CAP_FILE_WR);
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), r, {});
    return 0;
  }

  if (!in->snaprealm) {
    client->put_cap_ref(in, CEPH_CAP_FILE_WR);
    in_lock.unlock();
    queue_user_cb(client, std::move(cb), -EINVAL, {});
    return 0;
  }

  client->get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

  auto* held_bl = new bufferlist(std::move(data));
  auto* write_ctx = new C_ZC_Write(client, fh, InodeRef(in), start, offset,
                                   size, fpos, fsync, syncdataonly,
                                   std::move(cb), held_bl);

  // C_OnFinisher only — take inode_lock in C_ZC_Write::finish (same as
  // C_Read_Sync_NonBlocking). Do not wrap with C_Lock_Client_Finisher:
  // client_lock then inode_lock is forbidden.
  Context* filer_onfinish =
      new C_OnFinisher(write_ctx, &client->objecter_finisher);

  SnapContext snapc = in->snaprealm->get_snap_context();
  file_layout_t layout = in->layout;
  uint64_t trunc_size = in->truncate_size;
  uint32_t trunc_seq = in->truncate_seq;
  inodeno_t ino = in->ino;

  // Match _write / C_Write_Finisher (held through optional fsync).
  client->logger->inc(l_c_aio_ops);
  client->inc_aio_in_flight();

  in_lock.unlock();

  client->filer->write_trunc(ino, &layout, snapc, offset, size, *held_bl,
                             ceph::real_clock::now(), 0, trunc_size, trunc_seq,
                             filer_onfinish);
  return 0;
}

int64_t ZeroCopyClient::read(Fh* fh, uint64_t off, uint64_t len, bufferlist* out)
{
  if (!out) {
    return -EINVAL;
  }
  C_SaferCond cond;
  int r = async_read(fh, off, len, [&cond, out](IoResult&& res) {
    if (res.result >= 0) {
      *out = std::move(res.data);
    }
    cond.complete(res.result);
  });
  if (r < 0) {
    return r;
  }
  return cond.wait();
}

int64_t ZeroCopyClient::write(Fh* fh, uint64_t off, bufferlist data, bool fsync)
{
  C_SaferCond cond;
  int r = async_write(
      fh, off, std::move(data),
      [&cond](IoResult&& res) { cond.complete(res.result); }, fsync, false);
  if (r < 0) {
    return r;
  }
  return cond.wait();
}

} // namespace ceph
