// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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

ClientCaps::ClientCaps(Client *client, CephContext *cct, 
                       ceph::mutex &client_lock,
                       ceph::mutex &cache_lock,
                       ObjectCacher *objectcacher)
  : client(client),
    cct(cct),
    client_lock(client_lock),
    cache_lock(cache_lock),
    objectcacher(objectcacher),
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

int ClientCaps::get_caps_used(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  unsigned used = in->caps_used();
  if (!(used & CEPH_CAP_FILE_CACHE)) {
    bool is_empty;
    {
      ceph::unique_unlock u(client_lock);
      std::scoped_lock l(cache_lock);
      is_empty = objectcacher->set_is_empty(&in->oset);
    }
    if (!is_empty)
      used |= CEPH_CAP_FILE_CACHE;
  }
  return used;
}

void ClientCaps::check_cap_issue(Inode *in, unsigned issued)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
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
  if (!capem.second) {
    if (cap.gen < mds_session->cap_gen)
      cap.issued = cap.implemented = CEPH_CAP_PIN;

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
          mds_session->get_dirty_list().push_back(&in->dirty_cap_item);
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
  cap.gen = mds_session->cap_gen;
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  auto &in = cap->inode;
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
      in.flushing_cap_item.remove_myself();
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  while (!in->caps.empty())
    remove_cap(&in->caps.begin()->second, true);
}

void ClientCaps::remove_session_caps(MetaSession *s, int err)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  ldout(cct, 10) << __func__ << " mds." << s->mds_num << dendl;

  while (s->caps.size()) {
    Cap *cap = *s->caps.begin();
    InodeRef in(&cap->inode);
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
	num_flushing_caps--;
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
	objectcacher->purge_set(&in->oset);
      } else {
	objectcacher->release_set(&in->oset);
      }
      client->_schedule_invalidate_callback(in.get(), 0, 0);
    }

    ldout(cct, 10) << __func__ << " calling signal_caps_inode" << dendl;
    client->signal_caps_inode(in.get());
  }
  s->flushing_caps_tids.clear();
  client->sync_cond.notify_all();
}

void ClientCaps::trim_caps(MetaSession *s, uint64_t max)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  mds_rank_t mds = s->mds_num;
  size_t caps_size = s->caps.size();
  ldout(cct, 10) << __func__ << " mds." << mds << " max " << max 
    << " caps " << caps_size << dendl;

  uint64_t trimmed = 0;
  auto p = s->caps.begin();
  std::set<class Dentry *> to_trim; /* this avoids caps other than the one we're
                               * looking at from getting deleted during traversal. */
  while ((caps_size - trimmed) > max && !p.end()) {
    Cap *cap = *p;
    InodeRef in(&cap->inode);

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
  for (auto dn : to_trim)
    client->trim_dentry(dn);
}

int ClientCaps::mark_caps_flushing(Inode *in, ceph_tid_t* ptid)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  ceph_assert(flushing);

  ceph_tid_t flush_tid = ++last_flush_tid;
  in->flushing_cap_tids[flush_tid] = flushing;

  if (!in->flushing_caps) {
    ldout(cct, 10) << __func__ << " " << ccap_string(flushing) 
                   << " " << *in << dendl;
    num_flushing_caps++;
  } else {
    ldout(cct, 10) << __func__ << " (more) " << ccap_string(flushing) 
                   << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->mark_caps_clean();
 
  if (!in->flushing_cap_item.is_on_list())
    session->flushing_caps.push_back(&in->flushing_cap_item);
  session->flushing_caps_tids.insert(flush_tid);

  *ptid = flush_tid;
  return flushing;
}

void ClientCaps::adjust_session_flushing_caps(Inode *in, MetaSession *old_s,  
                                              MetaSession *new_s)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    if (capsnap.flush_tid > 0) {
      old_s->flushing_caps_tids.erase(capsnap.flush_tid);
      new_s->flushing_caps_tids.insert(capsnap.flush_tid);
    }
  }
  for (auto &p : in->flushing_cap_tids) {
    old_s->flushing_caps_tids.erase(p.first);
    new_s->flushing_caps_tids.insert(p.first);
  }
  new_s->flushing_caps.push_back(&in->flushing_cap_item);
}

void ClientCaps::cap_delay_requeue(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  ldout(cct, 10) << __func__ << " on " << *in << dendl;

  in->hold_caps_until = ceph::coarse_mono_clock::now() + caps_release_delay;
  delayed_list.push_back(&in->delay_cap_item);
}

void ClientCaps::flush_cap_releases()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));

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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  ldout(cct, 10) << "renew_caps mds." << session->mds_num << dendl;
  session->last_cap_renew_request = ceph_clock_now();
  uint64_t seq = ++session->cap_renew_seq;
  auto m = make_message<MClientSession>(CEPH_SESSION_REQUEST_RENEWCAPS, seq);
  session->con->send_message2(std::move(m));
}

int ClientCaps::get_caps_issued(int fd)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  Fh *f = client->get_filehandle(fd);
  if (!f)
    return -EBADF;

  return f->inode->caps_issued();
}

int ClientCaps::get_caps_issued(const char *path, const UserPerm& perms)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  InodeRef in;
  filepath fp(path);
  if (int rc = client->path_walk(client->cwd, fp, &in, perms, {}); rc < 0) {
    return rc;
  }
  return in->caps_issued();
}

// Stub implementations for methods that need more complex refactoring
void ClientCaps::get_cap_ref(Inode *in, int cap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->get_cap_ref(in, cap);
}

void ClientCaps::put_cap_ref(Inode *in, int cap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->put_cap_ref(in, cap);
}

void ClientCaps::send_cap(Inode *in, MetaSession *session, Cap *cap,
                          int flags, int used, int want, int retain,
                          int flush, ceph_tid_t flush_tid)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->send_cap(in, session, cap, flags, used, want, retain, flush, flush_tid);
}

void ClientCaps::check_caps(const InodeRef& in, unsigned flags)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->check_caps(in, flags);
}

int ClientCaps::get_caps(Fh *fh, int need, int want, int *phave, loff_t endoff)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  Inode *in = fh->inode.get();

  int r = client->check_pool_perm(in, need);
  if (r < 0)
    return r;

  while (1) {
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
	   check_caps(in, 0);
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

    if (waitfor_caps)
      client->wait_on_context_list(in->waitfor_caps);
    else if (waitfor_commit)
      client->wait_on_context_list(in->waitfor_commit);
  }
}

void ClientCaps::kick_flushing_caps(Inode *in, MetaSession *session)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " mds." << mds << dendl;

  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    if (in->flags & I_KICK_FLUSH) {
      ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
      kick_flushing_caps(in, session);
    }
  }
}

void ClientCaps::early_kick_flushing_caps(MetaSession *session)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
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
}

void ClientCaps::flush_caps_sync()
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  
  ldout(cct, 10) << __func__ << dendl;
  for (auto &q : client->mds_sessions) {
    auto s = q.second;
    xlist<Inode*>::iterator p = s->dirty_list.begin();
    while (!p.end()) {
      unsigned flags = CHECK_CAPS_NODELAY;
      Inode *in = *p;

      ++p;
      if (p.end())
        flags |= CHECK_CAPS_SYNCHRONOUS;
      check_caps(in, flags);
    }
  }
}

void ClientCaps::queue_cap_snap(Inode *in, const SnapContext &old_snapc)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->queue_cap_snap(in, old_snapc);
}

void ClientCaps::send_flush_snap(Inode *in, MetaSession *session, 
                                 snapid_t follows, CapSnap& capsnap)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->send_flush_snap(in, session, follows, capsnap);
}

void ClientCaps::flush_snaps(Inode *in)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->flush_snaps(in);
}

void ClientCaps::submit_sync_caps(Inode *in, ceph_tid_t want, Context *onfinish)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  // TODO: Implement sync caps submission
  // For now, just complete the context immediately
  if (onfinish)
    onfinish->complete(0);
}

void ClientCaps::wait_sync_caps(Inode *in, ceph_tid_t want)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->wait_sync_caps(in, want);
}

void ClientCaps::wait_sync_caps(ceph_tid_t want)
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
  client->wait_sync_caps(want);
}

// Made with Bob
