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

#ifndef CEPH_CLIENT_CAPS_H
#define CEPH_CLIENT_CAPS_H

#include "include/types.h"
#include "include/xlist.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "mds/mdstypes.h"
#include "InodeRef.h"
#include "include/Context.h"
#include "common/reentrant_lock.h"
#include <chrono>

class Client;
class Inode;
class SnapRealm;
class Cap;
class MetaSession;
class Fh;
class UserPerm;
class SnapContext;
class CapSnap;
class Context;

/**
 * ClientCaps - capability operations extracted from Client.
 *
 * Callers hold client_lock; caps_lock protects caps-specific bookkeeping
 * (flush tids, delayed release list, epoch barrier, etc.).
 */
class ClientCaps {
public:
  ClientCaps(Client *client, CephContext *cct);
  ~ClientCaps();

  void check_cap_issue(Inode *in, unsigned issued);
  int get_caps_used(Inode *in);

  void add_update_cap(Inode *in, MetaSession *session, uint64_t cap_id,
                      unsigned issued, unsigned wanted, unsigned seq,
                      unsigned mseq, inodeno_t realm, int flags,
                      const UserPerm& perms);
  void remove_cap(Cap *cap, bool queue_release);
  void remove_all_caps(Inode *in, bool queue_release);
  void remove_session_caps(MetaSession *session, int err);

  int mark_caps_flushing(Inode *in, ceph_tid_t *ptid);
  void adjust_session_flushing_caps(Inode *in, MetaSession *old_s,
                                    MetaSession *new_s);
  void flush_caps_sync();
  void kick_flushing_caps(Inode *in, MetaSession *session);
  void kick_flushing_caps(MetaSession *session);
  void early_kick_flushing_caps(MetaSession *session);

  int get_caps(Fh *fh, int need, int want, int *have, loff_t endoff);
  // Non-blocking cap check for async I/O submit.  Returns 0 when @need caps
  // are issued and none of @want are revoking; -EAGAIN otherwise.
  int try_get_caps(Fh *fh, int need, int want, int *have);

  void enqueue_cap_waiter(Inode *in, Context *c);

  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);

  void queue_cap_snap(Inode *in, const SnapContext &old_snapc);
  void finish_cap_snap(Inode *in, CapSnap &capsnap, int used);

  void wait_sync_caps(Inode *in, ceph_tid_t want);
  void wait_sync_caps(ceph_tid_t want);

  void trim_caps(MetaSession *s, uint64_t max);
  void renew_caps();
  void renew_caps(MetaSession *session);
  void flush_cap_releases();
  void renew_and_flush_cap_releases();

  void set_cap_epoch_barrier(epoch_t e);
  epoch_t get_cap_epoch_barrier() const;

  void cap_delay_requeue(Inode *in);
  template<typename Func>
  void process_delayed_caps(ceph::coarse_mono_time now, bool mount_aborted, Func&& func);

  void send_cap(Inode *in, MetaSession *session, Cap *cap, int flags,
                int used, int want, int retain, int flush,
                ceph_tid_t flush_tid);
  void send_flush_snap(Inode *in, MetaSession *session, snapid_t follows,
                       CapSnap& capsnap);
  void flush_snaps(Inode *in);

  static constexpr unsigned CHECK_CAPS_NODELAY = 0x1;
  static constexpr unsigned CHECK_CAPS_SYNCHRONOUS = 0x2;
  void check_caps(const InodeRef& in, unsigned flags);

  int get_num_flushing_caps() const;
  void inc_num_flushing_caps();
  void dec_num_flushing_caps();
  ceph_tid_t get_last_flush_tid() const;
  ceph_tid_t allocate_flush_tid();

  std::chrono::seconds get_caps_release_delay() const { return caps_release_delay; }
  void set_caps_release_delay(std::chrono::seconds delay) { caps_release_delay = delay; }

  void inc_pinned_icaps();
  void dec_pinned_icaps(uint64_t nr = 1);

  void purge_delayed_list();
  void unlink_delay_cap_item(Inode *in);
  void prepare_inode_unmount(Inode *in);

  static bool is_max_size_approaching(Inode *in);
  static int adjust_caps_used_for_lazyio(int used, int issued, int implemented);

  void wait_on_context_list(std::vector<Context*>& ls);
  void wait_on_context_cond(ceph::reentrant_condition_variable& cond,
			    std::atomic<bool>& done,
			    std::atomic<bool>& wake_complete);
  void signal_context_list(std::vector<Context*>& ls) {
    finish_contexts(cct, ls, 0);
  }
  void signal_caps_inode(Inode *in);
  void signal_caps_inode_sync(Inode *in);

private:
  Client *client;
  CephContext *cct;

  mutable ReentrantLock caps_lock =
    make_reentrant("ClientCaps::caps_lock", false);

  ceph::coarse_mono_time last_cap_renew;
  epoch_t cap_epoch_barrier = 0;
  ceph_tid_t last_flush_tid = 1;
  xlist<Inode*> delayed_list;
  int num_flushing_caps = 0;
  std::chrono::seconds caps_release_delay;

  SnapRealm *_get_snap_realm_unlocked(Inode *in, inodeno_t realm);
  void _put_snap_realm_unlocked(Inode *in, SnapRealm *realm);
  void _snaprealm_link_inode(Inode *in, SnapRealm *realm);
  void _snaprealm_unlink_inode(Inode *in);
};

#endif // CEPH_CLIENT_CAPS_H