// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ClientCaps.h"
#include "Client.h"
#include "Inode.h"
#include "Fh.h"
#include "MetaSession.h"
#include "UserPerm.h"
#include "InodeRef.h"
#include "ClientSnapRealm.h"
#include "Dentry.h"

#include "common/config.h"
#include "common/errno.h"
#include "include/cephfs/types.h"
#include "osdc/ObjectCacher.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSession.h"

#if defined(__linux__)
#include "FSCrypt.h"
#endif

#include <algorithm>

#define dout_subsys ceph_subsys_client

using std::string;

ClientCaps::ClientCaps(Client *client, CephContext *cct)
  : client(client),
    cct(cct),
    last_cap_renew(ceph::coarse_mono_clock::now()),
    caps_release_delay(cct->_conf.get_val<std::chrono::seconds>("client_caps_release_delay"))
{
}

ClientCaps::~ClientCaps()
= default;

void ClientCaps::inc_pinned_icaps()
{
  std::scoped_lock lock(caps_lock);
  client->inc_pinned_icaps();
}

void ClientCaps::dec_pinned_icaps(uint64_t nr)
{
  std::scoped_lock lock(caps_lock);
  client->dec_pinned_icaps(nr);
}

void ClientCaps::set_cap_epoch_barrier(epoch_t e)
{
  ldout(cct, 5) << __func__ << " epoch = " << e << dendl;
  std::scoped_lock lock(caps_lock);
  cap_epoch_barrier = e;
}

epoch_t ClientCaps::get_cap_epoch_barrier() const
{
  std::scoped_lock lock(caps_lock);
  return cap_epoch_barrier;
}

int ClientCaps::get_num_flushing_caps() const
{
  std::scoped_lock lock(caps_lock);
  return num_flushing_caps;
}

void ClientCaps::inc_num_flushing_caps()
{
  std::scoped_lock lock(caps_lock);
  num_flushing_caps++;
}

void ClientCaps::dec_num_flushing_caps()
{
  std::scoped_lock lock(caps_lock);
  num_flushing_caps--;
}

ceph_tid_t ClientCaps::get_last_flush_tid() const
{
  std::scoped_lock lock(caps_lock);
  return last_flush_tid;
}

ceph_tid_t ClientCaps::allocate_flush_tid()
{
  std::scoped_lock lock(caps_lock);
  return ++last_flush_tid;
}

int ClientCaps::get_caps_used(Inode *in)
{
  unsigned used = in->caps_used();
  if (!(used & CEPH_CAP_FILE_CACHE)) {
    bool is_empty = client->objectcacher->set_is_empty(&in->oset);
    if (!is_empty)
      used |= CEPH_CAP_FILE_CACHE;
  }
  return used;
}

void ClientCaps::check_cap_issue(Inode *in, unsigned issued)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  unsigned had = in->caps_issued();

  if ((issued & CEPH_CAP_FILE_CACHE) &&
      !(had & CEPH_CAP_FILE_CACHE))
    in->cache_gen++;

  if ((issued & CEPH_CAP_FILE_SHARED) !=
      (had & CEPH_CAP_FILE_SHARED)) {
    if (issued & CEPH_CAP_FILE_SHARED)
      in->shared_gen++;
    if (in->is_dir())
      client->clear_dir_complete_and_ordered(in, true);
  }
}

void ClientCaps::add_update_cap(Inode *in, MetaSession *mds_session, 
                                uint64_t cap_id, unsigned issued, 
                                unsigned wanted, unsigned seq, unsigned mseq,
                                inodeno_t realm, int flags, 
                                const UserPerm& cap_perms)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  if (!in->is_any_caps()) {
    ceph_assert(in->snaprealm == 0);
    in->snaprealm = client->get_snap_realm(realm);
    in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
    ldout(cct, 15) << __func__ << " first one, opened snaprealm " << in->snaprealm << dendl;
  } else {
    ceph_assert(in->snaprealm);
    if ((flags & CEPH_CAP_FLAG_AUTH) &&
	realm != inodeno_t(-1) && in->snaprealm->ino != realm) {
      in->snaprealm_item.remove_myself();
      auto oldrealm = in->snaprealm;
      in->snaprealm = client->get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      client->put_snap_realm(oldrealm);
    }
  }

  mds_rank_t mds = mds_session->mds_num;
  const auto &capem = in->caps.emplace(std::piecewise_construct, 
                                       std::forward_as_tuple(mds), 
                                       std::forward_as_tuple(*in, mds_session));
  Cap &cap = capem.first->second;
  uint64_t session_cap_gen;
  {
    std::scoped_lock s_lock(mds_session->session_lock);
    session_cap_gen = mds_session->cap_gen;
    if (!capem.second && cap.gen < session_cap_gen)
      cap.issued = cap.implemented = CEPH_CAP_PIN;
  }
  if (!capem.second) {

    /*
     * auth mds of the inode changed. we received the cap export
     * message, but still haven't received the cap import message.
     * handle_cap_export() updated the new auth MDS' cap.
     *
     * "ceph_seq_cmp(seq, cap->seq) <= 0" means we are processing
     * a message that was send before the cap import message. So
     * don't remove caps.
     */
    if (ceph_seq_cmp(seq, cap.seq) <= 0) {
      if (&cap != in->auth_cap)
         ldout(cct, 0) << "WARNING: " <<  "inode " << *in 
                       << " caps on mds." << mds << " != auth_cap." << dendl;

      ceph_assert(cap.cap_id == cap_id);
      seq = cap.seq;
      mseq = cap.mseq;
      issued |= cap.issued;
      flags |= CEPH_CAP_FLAG_AUTH;
    }
  } else {
    inc_pinned_icaps();
  }

  check_cap_issue(in, issued);

  if (flags & CEPH_CAP_FLAG_AUTH) {
    if (in->auth_cap != &cap &&
        (!in->auth_cap || ceph_seq_cmp(in->auth_cap->mseq, mseq) < 0)) {
      if (in->auth_cap) {
        if (in->flushing_cap_item.is_on_list()) {
          ldout(cct, 10) << __func__ << " changing auth cap: "
                         << "add myself to new auth MDS' flushing caps list"
                         << dendl;
          adjust_session_flushing_caps(in, in->auth_cap->session, mds_session);
        }
        if (in->dirty_cap_item.is_on_list()) {
          ldout(cct, 10) << __func__ << " changing auth cap: "
                         << "add myself to new auth MDS' dirty caps list" << dendl;
          // Need to remove from old session's dirty_list with its lock held,
          // then add to new session's dirty_list with its lock held
          in->auth_cap->session->with_dirty_list([in](auto& old_dirty_list) {
            in->dirty_cap_item.remove_myself();
          });
          mds_session->with_dirty_list([in](auto& dirty_list) {
            dirty_list.push_back(&in->dirty_cap_item);
          });
        }
      }

      in->auth_cap = &cap;
    }
  }

  unsigned old_caps = cap.issued;
  cap.cap_id = cap_id;
  cap.issued = issued;
  cap.implemented |= issued;
  if (ceph_seq_cmp(mseq, cap.mseq) > 0)
    cap.wanted = wanted;
  else
    cap.wanted |= wanted;
  cap.seq = seq;
  cap.issue_seq = seq;
  cap.mseq = mseq;
  cap.gen = session_cap_gen;
  cap.latest_perms = cap_perms;
  ldout(cct, 10) << __func__ << " issued " << ccap_string(old_caps) << " -> " << ccap_string(cap.issued)
    << " from mds." << mds
    << " on " << *in
    << dendl;

  if ((issued & ~old_caps) && in->auth_cap == &cap) {
    // non-auth MDS is revoking the newly grant caps ?
    for (auto &p : in->caps) {
      if (&p.second == &cap)
 continue;
      if (p.second.implemented & ~p.second.issued & issued) {
 check_caps(in, CHECK_CAPS_NODELAY);
 break;
      }
    }
  }

  if (issued & ~old_caps) {
    ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
    client->signal_caps_inode(in);
  }
}

void ClientCaps::remove_cap(Cap *cap, bool queue_release)
{
  auto &in = cap->inode;
  ceph_assert(ceph_mutex_is_locked_by_me(in));
  MetaSession *session = cap->session;
  mds_rank_t mds = cap->session->mds_num;

  ldout(cct, 10) << __func__ << " mds." << mds << " on " << in << dendl;
  
  if (queue_release) {
    session->enqueue_cap_release(
      in.ino,
      cap->cap_id,
      cap->issue_seq,
      cap->mseq,
      cap_epoch_barrier);
  } else {
    dec_pinned_icaps();
  }


  if (in.auth_cap == cap) {
    if (in.flushing_cap_item.is_on_list()) {
      ldout(cct, 10) << " removing myself from flushing_cap list" << dendl;
      session->with_flushing_caps([&in](auto& flushing_caps) {
        in.flushing_cap_item.remove_myself();
      });
    }
    in.auth_cap = NULL;
  }
  size_t n = in.caps.erase(mds);
  ceph_assert(n == 1);
  cap = nullptr;

  if (!in.is_any_caps()) {
    ldout(cct, 15) << __func__ << " last one, closing snaprealm " << in.snaprealm << dendl;
    in.snaprealm_item.remove_myself();
    client->put_snap_realm(in.snaprealm);
    in.snaprealm = 0;
  }
}

void ClientCaps::remove_all_caps(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  while (!in->caps.empty())
    remove_cap(&in->caps.begin()->second, true);
}

void ClientCaps::remove_session_caps(MetaSession *s, int err)
{
  ldout(cct, 10) << __func__ << " mds." << s->mds_num << dendl;

  while (s->with_caps_list([](auto& caps) { return caps.size(); })) {
    Cap *cap = s->with_caps_list([](auto& caps) { return *caps.begin(); });
    InodeRef in(&cap->inode);
    std::unique_lock in_lock(*in);
    bool dirty_caps = false;
    if (in->auth_cap == cap) {
      dirty_caps = in->dirty_caps | in->flushing_caps;
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
      if (in->has_any_filelocks())
	in->flags |= I_ERROR_FILELOCK;
    }
    auto caps = cap->implemented;
    if (cap->wanted | cap->issued)
      in->flags |= I_CAP_DROPPED;
    remove_cap(cap, false);
    in->cap_snaps.clear();
    if (dirty_caps) {
      lderr(cct) << __func__ << " still has dirty|flushing caps on " << *in << dendl;
      if (in->flushing_caps) {
	dec_num_flushing_caps();
	in->flushing_cap_tids.clear();
      }
      in->flushing_caps = 0;
      in->mark_caps_clean();
      client->put_inode(in.get());
    }
    caps &= CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER;
    if (caps && !in->caps_issued_mask(caps, true)) {
      if (err == -EBLOCKLISTED) {
	if (in->oset.dirty_or_tx) {
	  lderr(cct) << __func__ << " still has dirty data on " << *in << dendl;
	  in->set_async_err(err);
	}
	client->objectcacher->purge_set(&in->oset);
      } else {
	client->objectcacher->release_set(&in->oset);
      }
      client->_schedule_invalidate_callback(in.get(), 0, 0);
    }

    ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
    client->signal_caps_inode(in.get());
  }
  s->with_flushing_caps_tids([](auto& tids) {
    tids.clear();
  });
  client->sync_cond.notify_all();
}

void ClientCaps::trim_caps(MetaSession *s, uint64_t max)
{
  mds_rank_t mds = s->mds_num;
  size_t caps_size = s->with_caps_list([](auto& caps) { return caps.size(); });
  ldout(cct, 10) << __func__ << " mds." << mds << " max " << max 
    << " caps " << caps_size << dendl;

  uint64_t trimmed = 0;
  auto p = s->with_caps_list([](auto& caps) { return caps.begin(); });
  std::set<class Dentry *> to_trim; /* this avoids caps other than the one we're
                               * looking at from getting deleted during traversal. */
  while ((caps_size - trimmed) > max && !p.end()) {
    Cap *cap = *p;
    InodeRef in(&cap->inode);
    std::unique_lock in_lock(*in);

    // Increment p early because it will be invalidated if cap
    // is deleted inside remove_cap
    ++p;

    if (in->dirty_caps || in->cap_snaps.size())
      cap_delay_requeue(in.get());

    if (in->caps.size() > 1 && cap != in->auth_cap) {
      int mine = cap->issued | cap->implemented;
      int oissued = in->auth_cap ? in->auth_cap->issued : 0;
      // disposable non-auth cap
      if (!(get_caps_used(in.get()) & ~oissued & mine)) {
        ldout(cct, 20) << " removing unused, unneeded non-auth cap on " 
                       << *in << dendl;
        cap = (remove_cap(cap, true), nullptr);
        trimmed++;
      }
    } else {
      ldout(cct, 20) << " trying to trim dentries for " << *in << dendl;
      bool all = true;
      auto q = in->dentries.begin();
      while (q != in->dentries.end()) {
        class Dentry *dn = *q;
        ++q;
        if (dn->inode.get() == in.get() && dn->lru_is_expireable()) {
          to_trim.insert(dn);
        } else {
          ldout(cct, 20) << "  not expirable: " << dn->name << dendl;
          all = false;
        }
      }
      if (all && in->ino != client->get_root_ino()) {
        ldout(cct, 20) << __func__ << " counting as trimmed: " << *in << dendl;
        trimmed++;
      }
    }
  }
  ldout(cct, 20) << " trimming dentries for " << to_trim.size() 
                 << " inodes" << dendl;
  std::scoped_lock<Client> cl(*client);
  for (auto dn : to_trim)
    client->trim_dentry(dn);
}

int ClientCaps::mark_caps_flushing(Inode *in, ceph_tid_t* ptid)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  ceph_assert(flushing);

  ceph_tid_t flush_tid = allocate_flush_tid();
  in->flushing_cap_tids[flush_tid] = flushing;

  if (!in->flushing_caps) {
    ldout(cct, 10) << __func__ << " " << ccap_string(flushing) 
                   << " " << *in << dendl;
    inc_num_flushing_caps();
  } else {
    ldout(cct, 10) << __func__ << " (more) " << ccap_string(flushing) 
                   << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->mark_caps_clean();
 
  if (!in->flushing_cap_item.is_on_list()) {
    session->with_flushing_caps([in](auto& flushing_caps) {
      flushing_caps.push_back(&in->flushing_cap_item);
    });
  }
  session->with_flushing_caps_tids([flush_tid](auto& tids) {
    tids.insert(flush_tid);
  });

  *ptid = flush_tid;
  return flushing;
}

void ClientCaps::adjust_session_flushing_caps(Inode *in, MetaSession *old_s,  
                                              MetaSession *new_s)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    if (capsnap.flush_tid > 0) {
      old_s->with_flushing_caps_tids([&capsnap](auto& tids) {
        tids.erase(capsnap.flush_tid);
      });
      new_s->with_flushing_caps_tids([&capsnap](auto& tids) {
        tids.insert(capsnap.flush_tid);
      });
    }
  }
  for (auto &p : in->flushing_cap_tids) {
    old_s->with_flushing_caps_tids([&p](auto& tids) {
      tids.erase(p.first);
    });
    new_s->with_flushing_caps_tids([&p](auto& tids) {
      tids.insert(p.first);
    });
  }
  new_s->with_flushing_caps([in](auto& flushing_caps) {
    flushing_caps.push_back(&in->flushing_cap_item);
  });
}

void ClientCaps::cap_delay_requeue(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  ldout(cct, 10) << __func__ << " on " << *in << dendl;

  in->hold_caps_until = ceph::coarse_mono_clock::now() + caps_release_delay;
  std::unique_lock in_lock(caps_lock);
  delayed_list.push_back(&in->delay_cap_item);
}

void ClientCaps::prepare_inode_unmount(Inode *in)
{
  // inode_lock before client_lock (same order as _put_inode) — callers must
  // not hold client_lock across this function.
  ceph_assert(ceph_mutex_is_not_locked_by_me(client->m_client_lock));

  std::unique_lock in_lock(*in);

  client->objectcacher->purge_set(&in->oset);

  std::vector<std::pair<int, int>> refs(in->cap_refs.begin(),
					in->cap_refs.end());
  for (auto &[cap, cnt] : refs) {
    for (int i = 0; i < cnt; ++i)
      put_cap_ref(in, cap);
  }

  // Wake threads blocked in get_caps so unmount can proceed.
  signal_caps_inode(in);
  signal_context_list(in->waitfor_commit);

  {
    std::unique_lock caps_lock_guard(caps_lock);
    in->delay_cap_item.remove_myself();
  }
  in->hold_caps_until = ceph::coarse_mono_clock::time_point::min();
}

void ClientCaps::flush_cap_releases()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->m_client_lock));
  
  uint64_t nr_caps = 0;

  // send any cap releases
  for (auto &p : client->mds_sessions) {
    auto session = p.second;
    if (session->release && client->mdsmap->is_clientreplay_or_active_or_stopping(
            p.first)) {
      nr_caps += session->release->caps.size();
      for (const auto &cap: session->release->caps) {
        ldout(cct, 10) << __func__ << " removing " 
                       << static_cast<inodeno_t>(cap.ino) <<
        " from subvolume tracker" << dendl;
        client->subvolume_tracker->remove_inode(static_cast<inodeno_t>(cap.ino));
      }
      if (cct->_conf->client_inject_release_failure) {
        ldout(cct, 20) << __func__ 
                       << " injecting failure to send cap release message" 
                       << dendl;
      } else {
        session->con->send_message2(std::move(session->release));
      }
      session->release.reset();
    }
  }

  if (nr_caps > 0) {
    dec_pinned_icaps(nr_caps);
  }
}

void ClientCaps::renew_and_flush_cap_releases()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->m_client_lock));

  if (!client->mount_aborted && client->mdsmap->get_epoch()) {
    // renew caps?
    auto el = ceph::coarse_mono_clock::now() - last_cap_renew;
    if (unlikely(utime_t(el) > client->mdsmap->get_session_timeout() / 3.0))
      renew_caps();

    flush_cap_releases();
  }
}

void ClientCaps::renew_caps()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->m_client_lock));
  
  ldout(cct, 10) << "renew_caps()" << dendl;
  last_cap_renew = ceph::coarse_mono_clock::now();

  for (auto &p : client->mds_sessions) {
    ldout(cct, 15) << "renew_caps requesting from mds." << p.first << dendl;
    if (client->mdsmap->get_state(p.first) >= MDSMap::STATE_REJOIN)
      renew_caps(p.second.get());
  }
}

void ClientCaps::renew_caps(MetaSession *session)
{
  ldout(cct, 10) << "renew_caps mds." << session->mds_num << dendl;
  std::scoped_lock s_lock(session->session_lock);
  session->last_cap_renew_request = ceph_clock_now();
  uint64_t seq = ++session->cap_renew_seq;
  auto m = make_message<MClientSession>(CEPH_SESSION_REQUEST_RENEWCAPS, seq);
  session->con->send_message2(std::move(m));
}

int ClientCaps::get_caps_issued(int fd)
{
  Fh *f = client->get_filehandle(fd);
  if (!f)
    return -EBADF;

  std::unique_lock in_lock(*(f->inode));
  return f->inode->caps_issued();
}

int ClientCaps::get_caps_issued(const char *path, const UserPerm& perms)
{
  InodeRef in;
  filepath fp(path);
  if (int rc = client->path_walk(client->cwd, fp, &in, perms, {}); rc < 0) {
    return rc;
  }
  std::unique_lock in_lock(*in);
  return in->caps_issued();
}


void ClientCaps::wait_on_context_list(std::vector<Context*>& ls)
{
  reentrant_condition_variable cond;
  bool done = false;
  int r;
  ls.push_back(new C_ReentrantCond(cond, &done, &r));
  wait_on_context_cond(cond, done);
}

void ClientCaps::wait_on_context_cond(
  reentrant_condition_variable& cond, bool& done)
{
  // Do not wait while holding caps_lock: signal_caps_inode takes caps_lock to
  // finish waitfor_caps contexts, which would deadlock get_caps here.
  //
  // Use a private wait_lock paired with cond.  C_ReentrantCond::finish() wakes
  // via notify_all_sloppy() without holding wait_lock; that can race with an
  // unbounded wait() and hang even after *done is set (seen in gdb with
  // done==true).  Timed waits re-check *done so a missed notify cannot stall.
  ReentrantLock wait_lock = make_reentrant("ClientCaps::wait_cond", false);
  std::unique_lock l{wait_lock};
  while (!done) {
    cond.wait_for(l, std::chrono::milliseconds(200),
                  [&done] { return done; });
  }
}

void ClientCaps::signal_caps_inode(Inode *in)
{
  std::unique_lock l(caps_lock);
  signal_context_list(in->waitfor_caps);
  std::swap(in->waitfor_caps, in->waitfor_caps_pending);
  signal_context_list(in->waitfor_caps);
}

// Stub implementations for methods that need more complex refactoring
void ClientCaps::get_cap_ref(Inode *in, int cap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  if ((cap & CEPH_CAP_FILE_BUFFER) &&
      in->cap_refs[CEPH_CAP_FILE_BUFFER] == 0) {
    ldout(cct, 5) << __func__ << " got first FILE_BUFFER ref on " << *in << dendl;
    in->iget();
      }
  if ((cap & CEPH_CAP_FILE_CACHE) &&
      in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    ldout(cct, 5) << __func__ << " got first FILE_CACHE ref on " << *in << dendl;
    in->iget();
      }
  in->get_cap_ref(cap);
}

void ClientCaps::put_cap_ref(Inode *in, int cap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  int last = in->put_cap_ref(cap);
  if (last) {
    int put_nref = 0;
    int drop = last & ~in->caps_issued();
    if (in->snapid == CEPH_NOSNAP) {
      if ((last & CEPH_CAP_FILE_WR) && !in->cap_snaps.empty() &&
          in->cap_snaps.rbegin()->second.writing) {
        ldout(cct, 10) << __func__ << " finishing pending cap_snap on " << *in
                       << dendl;
        in->cap_snaps.rbegin()->second.writing = 0;
        finish_cap_snap(in, in->cap_snaps.rbegin()->second, get_caps_used(in));
        ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
        signal_caps_inode(in); // wake up blocked sync writers
      }
      if (last & CEPH_CAP_FILE_BUFFER) {
        for (auto& p : in->cap_snaps)
          p.second.dirty_data = 0;
        signal_context_list(in->waitfor_commit);
        ldout(cct, 5) << __func__ << " dropped last FILE_BUFFER ref on " << *in
                      << dendl;
        if (!in->is_write_delegated()) {
          ++put_nref;
        }

        if (!in->cap_snaps.empty()) {
          flush_snaps(in);
        }
      }
    }
    if (last & CEPH_CAP_FILE_CACHE) {
      ldout(cct, 5) << __func__ << " dropped last FILE_CACHE ref on " << *in
                    << dendl;
      ++put_nref;

      ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
      signal_caps_inode(in);
    }
    if (drop)
      check_caps(in, 0);
    if (put_nref)
      client->put_inode(in, put_nref);
  }
}

void ClientCaps::send_cap(Inode *in, MetaSession *session, Cap *cap,
                          int flags, int used, int want, int retain,
                          int flush, ceph_tid_t flush_tid)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  client->send_cap(in, session, cap, flags, used, want, retain, flush, flush_tid);
}

bool ClientCaps::is_max_size_approaching(Inode *in)
{
  /* mds will adjust max size according to the reported size */
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  if (in->flushing_caps & CEPH_CAP_FILE_WR)
    return false;
  if (in->size >= in->max_size)
    return true;
  /* half of previous max_size increment has been used */
  if (in->max_size > in->reported_size &&
      (in->size << 1) >= in->max_size + in->reported_size)
    return true;
  return false;
}

int ClientCaps::adjust_caps_used_for_lazyio(int used, int issued, int implemented)
{
  if (!(used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER)))
    return used;
  if (!(implemented & CEPH_CAP_FILE_LAZYIO))
    return used;

  if (issued & CEPH_CAP_FILE_LAZYIO) {
    if (!(issued & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(issued & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  } else {
    if (!(implemented & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(implemented & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  }
  return used;
}

/**
 * check_caps
 *
 * Examine currently used and wanted versus held caps. Release, flush or ack
 * revoked caps to the MDS as appropriate.
 *
 * @param in the inode to check
 * @param flags flags to apply to cap check
 */
void ClientCaps::check_caps(const InodeRef& in, unsigned flags)
{
  std::unique_lock in_lock(*in);
  unsigned wanted;
  unsigned used;
  if (client->is_unmounting()) {
    wanted = 0;
    used = 0;
  } else {
    wanted = in->caps_wanted();
    used = get_caps_used(in.get());
  }
  unsigned cap_used;

  int implemented;
  int issued = in->caps_issued(&implemented);
  int revoking = implemented & ~issued;

  int orig_used = used;
  used = adjust_caps_used_for_lazyio(used, issued, implemented);

  int retain = wanted | used | CEPH_CAP_PIN;
  if (!client->is_unmounting() && in->nlink > 0) {
    if (wanted) {
      retain |= CEPH_CAP_ANY;
    } else if (in->is_dir() &&
	       (issued & CEPH_CAP_FILE_SHARED) &&
	       (in->flags & I_COMPLETE)) {
      // we do this here because we don't want to drop to Fs (and then
      // drop the Fs if we do a create!) if that alone makes us send lookups
      // to the MDS. Doing it in in->caps_wanted() has knock-on effects elsewhere
      wanted = CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_EXCL;
      retain |= wanted;
    } else {
      retain |= CEPH_CAP_ANY_SHARED;
      // keep RD only if we didn't have the file open RW,
      // because then the mds would revoke it anyway to
      // journal max_size=0.
      if (in->max_size == 0)
	retain |= CEPH_CAP_ANY_RD;
    }
  }

  ldout(cct, 10) << __func__ << " on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " issued " << ccap_string(issued)
	   << " revoking " << ccap_string(revoking)
	   << " flags=" << flags
	   << dendl;

  if (in->snapid != CEPH_NOSNAP)
    return; //snap caps last forever, can't write

  if (in->caps.empty())
    return;   // guard if at end of func

  if (!(orig_used & CEPH_CAP_FILE_BUFFER) &&
      (revoking & used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO))) {
    if (client->_release(in.get()))
      used &= ~(CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO);
  }

  for (auto &[mds, cap] : in->caps) {
    // Use cap.session; do not take client_lock here.  check_caps runs under
    // inode_lock (e.g. from _close) and nesting inode_lock -> client_lock
    // deadlocks with flush_set_callback (client_lock -> inode_lock).
    MetaSession *session = cap.session;
    ceph_assert(session->mds_num == mds);

    cap_used = used;
    if (in->auth_cap && &cap != in->auth_cap)
      cap_used &= ~in->auth_cap->issued;

    revoking = cap.implemented & ~cap.issued;

    ldout(cct, 10) << " cap mds." << mds
	     << " issued " << ccap_string(cap.issued)
	     << " implemented " << ccap_string(cap.implemented)
	     << " revoking " << ccap_string(revoking) << dendl;

    if (in->wanted_max_size > in->max_size &&
	in->wanted_max_size > in->requested_max_size &&
	&cap == in->auth_cap)
      goto ack;

    /* approaching file_max? */
    if ((cap.issued & CEPH_CAP_FILE_WR) &&
	&cap == in->auth_cap &&
	is_max_size_approaching(in.get())) {
      ldout(cct, 10) << "size " << in->size << " approaching max_size " << in->max_size
		     << ", reported " << in->reported_size << dendl;
      goto ack;
    }

    /* completed revocation? */
    if (revoking && (revoking & cap_used) == 0) {
      ldout(cct, 10) << "completed revocation of " << ccap_string(cap.implemented & ~cap.issued) << dendl;
      goto ack;
    }

    /* want more caps from mds? */
    if (wanted & ~(cap.wanted | cap.issued))
      goto ack;

    if (!revoking && client->is_unmounting() && (cap_used == 0))
      goto ack;

    if ((cap.issued & ~retain) == 0 && // and we don't have anything we wouldn't like
	!in->dirty_caps)               // and we have no dirty caps
      continue;

    if (!(flags & CHECK_CAPS_NODELAY)) {
      ldout(cct, 10) << "delaying cap release" << dendl;
      cap_delay_requeue(in.get());
      continue;
    }

  ack:
    if (&cap == in->auth_cap) {
      if (in->flags & I_KICK_FLUSH) {
	ldout(cct, 20) << " reflushing caps (check_caps) on " << *in
		       << " to mds." << mds << dendl;
	kick_flushing_caps(in.get(), session);
      }
      if (!in->cap_snaps.empty() &&
	  in->cap_snaps.rbegin()->second.flush_tid == 0)
	flush_snaps(in.get());
    }

    int flushing;
    int msg_flags = 0;
    ceph_tid_t flush_tid;
    if (in->auth_cap == &cap && in->dirty_caps) {
      flushing = mark_caps_flushing(in.get(), &flush_tid);
      if (flags & CHECK_CAPS_SYNCHRONOUS)
	msg_flags |= MClientCaps::FLAG_SYNC;
    } else {
      flushing = 0;
      flush_tid = 0;
    }

    {
      std::unique_lock caps_lock_guard(caps_lock);
      in->delay_cap_item.remove_myself();
    }
    send_cap(in.get(), session, &cap, msg_flags, cap_used, wanted, retain,
      flushing, flush_tid);
  }
}

int ClientCaps::get_caps(Fh *fh, int need, int want, int *phave, loff_t endoff)
{
  Inode *in = fh->inode.get();
  ceph_assert(ceph_mutex_is_locked_by_me(*in));

  int r = 0;
  {
    std::unique_lock<Client> lock(*client);
    r = client->check_pool_perm(in, need);
    if (r < 0)
      return r;
  }

  while (1) {
    if (client->is_unmounting())
      return -ENOTCONN;

    int file_wanted = in->caps_file_wanted();
    if ((file_wanted & need) != need) {
      ldout(cct, 10) << "get_caps " << *in << " need " << ccap_string(need)
		     << " file_wanted " << ccap_string(file_wanted) << ", EBADF "
		     << dendl;
      return -EBADF;
    }

    if ((fh->mode & CEPH_FILE_MODE_WR) && fh->gen != client->fd_gen)
      return -EBADF;

    if ((in->flags & I_ERROR_FILELOCK) && fh->has_any_filelocks())
      return -EIO;

    int implemented;
    int have = in->caps_issued(&implemented);

    bool waitfor_caps = false;
    bool waitfor_commit = false;

    if (have & need & CEPH_CAP_FILE_WR) {
      if (endoff > 0) {
	 if ((endoff >= (loff_t)in->max_size ||
	      endoff > (loff_t)(in->size << 1)) &&
	     endoff > (loff_t)in->wanted_max_size) {
           ldout(cct, 10) << "wanted_max_size " << in->wanted_max_size << " -> " << endoff << dendl;
           uint64_t want = endoff;
#if defined(__linux__)
           if (in->fscrypt_auth.size()) {
             want = fscrypt_block_start(endoff + FSCRYPT_BLOCK_SIZE - 1);
	   }
#endif
	   in->wanted_max_size = want;
	 }
	 if (in->wanted_max_size > in->max_size &&
	     in->wanted_max_size > in->requested_max_size)
	   check_caps(in, Client::CHECK_CAPS_NODELAY);
      }

      if (endoff >= 0 && endoff > (loff_t)in->max_size) {
	ldout(cct, 10) << "waiting on max_size, endoff " << endoff << " max_size " << in->max_size << " on " << *in << dendl;
	waitfor_caps = true;
      }
      if (!in->cap_snaps.empty()) {
	if (in->cap_snaps.rbegin()->second.writing) {
	  ldout(cct, 10) << "waiting on cap_snap write to complete" << dendl;
	  waitfor_caps = true;
	}
	for (auto &p : in->cap_snaps) {
	  if (p.second.dirty_data) {
	    waitfor_commit = true;
	    break;
	  }
        }
	if (waitfor_commit) {
	  client->_flush_inode_async(in);
	  ldout(cct, 10) << "waiting for WRBUFFER to get dropped" << dendl;
	}
      }
    }

    if (!waitfor_caps && !waitfor_commit) {
      if ((have & need) == need) {
	int revoking = implemented & ~have;
	ldout(cct, 10) << "get_caps " << *in << " have " << ccap_string(have)
		 << " need " << ccap_string(need) << " want " << ccap_string(want)
		 << " revoking " << ccap_string(revoking)
		 << dendl;
	if ((revoking & want) == 0) {
	  *phave = need | (have & want);
	  in->get_cap_ref(need);
	  client->cap_hit();
	  return 0;
	}
      }
      ldout(cct, 10) << "waiting for caps " << *in << " need " << ccap_string(need) << " want " << ccap_string(want) << dendl;
      waitfor_caps = true;
    }

    if ((need & CEPH_CAP_FILE_WR) &&
        ((in->auth_cap && in->auth_cap->session->readonly)
        // (is locked)
#if defined(__linux__)
        || (in->is_fscrypt_enabled() && client->is_inode_locked(in) && client->fscrypt_as)
#endif
       ))
      return -EROFS;

    if (in->flags & I_CAP_DROPPED) {
      int mds_wanted = in->caps_mds_wanted();
      if ((mds_wanted & need) != need) {
	int ret = client->_renew_caps(in);
	if (ret < 0)
	  return ret;
	continue;
      }
      if (!(file_wanted & ~mds_wanted))
	in->flags &= ~I_CAP_DROPPED;
    }

    if (waitfor_caps || waitfor_commit) {
      auto& wait_list = waitfor_caps ? in->waitfor_caps : in->waitfor_commit;
      reentrant_condition_variable cond;
      bool done = false;
      int r = 0;
      // Register before check_caps / ms_dispatch: a grant can arrive as soon as
      // the MDS sees our cap update; signaling an empty waitfor_caps leaves the
      // waiter stuck with done=false even after max_size is granted.
      wait_list.push_back(new C_ReentrantCond(cond, &done, &r));
      if (waitfor_caps) {
	// We may have already sent a max_size request (requested_max_size ==
	// wanted_max_size) but the MDS grant was still too small.  Allow
	// check_caps to re-send instead of sleeping with no in-flight request.
	if (in->wanted_max_size > in->max_size &&
	    in->wanted_max_size <= in->requested_max_size) {
	  in->requested_max_size = 0;
	}
	check_caps(in, Client::CHECK_CAPS_NODELAY);
	// Grant may have been applied before we registered; don't sleep if so.
	if (endoff >= 0 && endoff <= (loff_t)in->max_size) {
	  bool snap_writing = false;
	  if (!in->cap_snaps.empty()) {
	    snap_writing = in->cap_snaps.rbegin()->second.writing;
	  }
	  if (!snap_writing) {
	    waitfor_caps = false;
	  }
	}
      }
      if (!waitfor_caps && !waitfor_commit) {
	// ms_dispatch may have already finish()ed our context after push_back.
	if (!wait_list.empty()) {
	  signal_context_list(wait_list);
	}
	continue;
      }
      // Drop client_lock when held so ms_dispatch can take scoped_lock<Client>
      // in handle_caps.  ll_write/ll_read usually hold only inode_lock (see
      // inode_lock.h: client_lock is not taken unless already held).
      const bool had_client = client->is_locked_by_me();
      ceph::unique_unlock<Client> cl_drop(*client, std::defer_lock);
      if (had_client) {
	cl_drop.release();
      }
      const bool had_inode = in->is_locked_by_me();
      if (had_inode) {
	{
	  ceph::unique_unlock<Inode> in_unlock(*in, std::defer_lock);
	  in_unlock.release();
	  wait_on_context_cond(cond, done);
	  in_unlock._abandon();
	}
	if (!in->is_locked_by_me()) {
	  in->m_inode_lock.lock();
	}
      } else {
	wait_on_context_cond(cond, done);
      }
      if (had_client) {
	ceph::client_lock::reacquire_after_drop(*client);
      }
    }
  }
}

void ClientCaps::kick_flushing_caps(Inode *in, MetaSession *session)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  
  in->flags &= ~I_KICK_FLUSH;

  Cap *cap = in->auth_cap;
  ceph_assert(cap->session == session);

  ceph_tid_t last_snap_flush = 0;
  for (auto p = in->flushing_cap_tids.rbegin();
       p != in->flushing_cap_tids.rend();
       ++p) {
    if (!p->second) {
      last_snap_flush = p->first;
      break;
    }
  }

  int wanted = in->caps_wanted();
  int used = get_caps_used(in) | in->caps_dirty();
  auto it = in->cap_snaps.begin();
  for (auto& p : in->flushing_cap_tids) {
    if (p.second) {
      int msg_flags = p.first < last_snap_flush ? MClientCaps::FLAG_PENDING_CAPSNAP : 0;
      send_cap(in, session, cap, msg_flags, used, wanted, (cap->issued | cap->implemented),
	       p.second, p.first);
    } else {
      ceph_assert(it != in->cap_snaps.end());
      ceph_assert(it->second.flush_tid == p.first);
      send_flush_snap(in, session, it->first, it->second);
      ++it;
    }
  }
}

void ClientCaps::kick_flushing_caps(MetaSession *session)
{
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " mds." << mds << dendl;

  session->with_flushing_caps([&](auto& flushing_caps) {
    for (xlist<Inode*>::iterator p = flushing_caps.begin(); !p.end(); ++p) {
      Inode *in = *p;
      std::scoped_lock in_lock(*in);
      if (in->flags & I_KICK_FLUSH) {
        ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
        kick_flushing_caps(in, session);
      }
    }
  });
}

void ClientCaps::early_kick_flushing_caps(MetaSession *session)
{
  session->with_flushing_caps([&](auto& flushing_caps) {
    for (xlist<Inode*>::iterator p = flushing_caps.begin(); !p.end(); ++p) {
      Inode *in = *p;
      std::scoped_lock in_lock(*in);
      Cap *cap = in->auth_cap;
      ceph_assert(cap);

      // if flushing caps were revoked, we re-send the cap flush in client reconnect
      // stage. This guarantees that MDS processes the cap flush message before issuing
      // the flushing caps to other client.
      if ((in->flushing_caps & in->auth_cap->issued) == in->flushing_caps) {
        in->flags |= I_KICK_FLUSH;
        continue;
      }

      ldout(cct, 20) << " reflushing caps (early_kick) on " << *in
       << " to mds." << session->mds_num << dendl;
      // send_reconnect() also will reset these sequence numbers. make sure
      // sequence numbers in cap flush message match later reconnect message.
      cap->seq = 0;
      cap->issue_seq = 0;
      cap->mseq = 0;
      cap->issued = cap->implemented;

      kick_flushing_caps(in, session);
    }
  });
}

void ClientCaps::flush_caps_sync()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->m_client_lock));
  
  ldout(cct, 10) << __func__ << dendl;
  for (auto &q : client->mds_sessions) {
    auto s = q.second;
    s->with_dirty_list([&](auto& dirty_list) {
      xlist<Inode*>::iterator p = dirty_list.begin();
      while (!p.end()) {
        unsigned flags = CHECK_CAPS_NODELAY;
        Inode *in = *p;
        ++p;
        if (p.end())
          flags |= CHECK_CAPS_SYNCHRONOUS;
        std::scoped_lock in_lock(*in);
        check_caps(in, flags);
      }
    });
  }
}

void ClientCaps::queue_cap_snap(Inode *in, const SnapContext &old_snapc)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  int used = get_caps_used(in);
  int dirty = in->caps_dirty();
  ldout(cct, 10) << __func__ << " " << *in << " snapc " << old_snapc << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second.writing) {
    ldout(cct, 10) << __func__ << " already have pending cap_snap on " << *in << dendl;
    return;
      } else if (dirty || (used & CEPH_CAP_FILE_WR)) {
        const auto &capsnapem = in->cap_snaps.emplace(std::piecewise_construct, std::make_tuple(old_snapc.seq), std::make_tuple(in));
        ceph_assert(capsnapem.second); /* element inserted */
        CapSnap &capsnap = capsnapem.first->second;
        capsnap.context = old_snapc;
        capsnap.issued = in->caps_issued();
        capsnap.dirty = dirty;

        capsnap.dirty_data = (used & CEPH_CAP_FILE_BUFFER);

        capsnap.uid = in->uid;
        capsnap.gid = in->gid;
        capsnap.mode = in->mode;
        capsnap.btime = in->btime;
        capsnap.xattrs = in->xattrs;
        capsnap.xattr_version = in->xattr_version;

        if (used & CEPH_CAP_FILE_WR) {
          ldout(cct, 10) << __func__ << " WR used on " << *in << dendl;
          capsnap.writing = 1;
        } else {
          finish_cap_snap(in, capsnap, used);
        }
      } else {
        ldout(cct, 10) << __func__ << " not dirty|writing on " << *in << dendl;
      }
}

void ClientCaps::finish_cap_snap(Inode *in, CapSnap &capsnap, int used)
{
  client->finish_cap_snap(in, capsnap, used);
}
void ClientCaps::send_flush_snap(Inode *in, MetaSession *session, 
                                 snapid_t follows, CapSnap& capsnap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  auto m = make_message<MClientCaps>(CEPH_CAP_OP_FLUSHSNAP,
                       in->ino, in->snaprealm->ino, 0,
                       in->auth_cap->mseq, get_cap_epoch_barrier());
  /*
   * Since the setattr will check the cephx mds auth access before
   * buffering the changes, so it makes no sense any more to let
   * the cap update to check the access in MDS again.
   */
  m->caller_uid = -1;
  m->caller_gid = -1;

  m->set_client_tid(capsnap.flush_tid);
  m->head.snap_follows = follows;

  m->head.caps = capsnap.issued;
  m->head.dirty = capsnap.dirty;

  m->head.uid = capsnap.uid;
  m->head.gid = capsnap.gid;
  m->head.mode = capsnap.mode;
  m->btime = capsnap.btime;

  m->size = capsnap.size;

  m->head.xattr_version = capsnap.xattr_version;
  encode(capsnap.xattrs, m->xattrbl);

  m->fscrypt_file = capsnap.fscrypt_auth;
  m->fscrypt_file = capsnap.fscrypt_file;
  m->ctime = capsnap.ctime;
  m->btime = capsnap.btime;
  m->mtime = capsnap.mtime;
  m->atime = capsnap.atime;
  m->time_warp_seq = capsnap.time_warp_seq;
  m->change_attr = capsnap.change_attr;

  if (capsnap.dirty & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  session->with_flushing_caps_tids([&](auto& tids) {
    ceph_assert(!tids.empty());
    m->set_oldest_flush_tid(*tids.begin());
  });

  session->con->send_message2(std::move(m));
}

void ClientCaps::flush_snaps(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(*in));
  ldout(cct, 10) << "flush_snaps on " << *in << dendl;
  ceph_assert(in->cap_snaps.size());

  // pick auth mds
  ceph_assert(in->auth_cap);
  MetaSession *session = in->auth_cap->session;

  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    // only do new flush
    if (capsnap.flush_tid > 0)
      continue;

    ldout(cct, 10) << "flush_snaps mds." << session->mds_num
             << " follows " << p.first
             << " size " << capsnap.size
             << " mtime " << capsnap.mtime
             << " dirty_data=" << capsnap.dirty_data
             << " writing=" << capsnap.writing
             << " on " << *in << dendl;
    if (capsnap.dirty_data || capsnap.writing)
      break;

    capsnap.flush_tid = allocate_flush_tid();
    session->with_flushing_caps_tids([&capsnap](auto& tids) {
      tids.insert(capsnap.flush_tid);
    });
    in->flushing_cap_tids[capsnap.flush_tid] = 0;
    if (!in->flushing_cap_item.is_on_list()) {
      session->with_flushing_caps([in](auto& flushing_caps) {
        flushing_caps.push_back(&in->flushing_cap_item);
      });
    }

    send_flush_snap(in, session, p.first, capsnap);
  }
}

void ClientCaps::submit_sync_caps(Inode *in, ceph_tid_t want, Context *onfinish)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client->m_client_lock));
  // TODO: Implement sync caps submission
  // For now, just complete the context immediately
  if (onfinish)
    onfinish->complete(0);
}

void ClientCaps::wait_sync_caps(Inode *in, ceph_tid_t want)
{
  client->wait_sync_caps(in, want);
}

void ClientCaps::wait_sync_caps(ceph_tid_t want)
{
  client->wait_sync_caps(want);
}

// Made with Bob
