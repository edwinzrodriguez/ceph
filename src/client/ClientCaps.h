// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * CCopyright (C) 2026 IBM, Inc.
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
#include <chrono>
#include "common/reentrant_lock.h"

class Client;
class Inode;
class SnapRealm;
class Cap;
class MetaSession;
class Fh;
class UserPerm;
class ObjectCacher;
class SnapContext;
class CapSnap;
class Context;

namespace ceph {
  class Formatter;
}

/**
 * ClientCaps - Manages capability (caps) operations for the CephFS client
 * 
 * This class encapsulates all capability-related functionality that was
 * previously part of the Client class. It provides thread-safe operations
 * for managing file capabilities with its own lock, independent of the
 * client_lock.
 */
class ClientCaps {
public:
  ClientCaps(Client *client, CephContext *cct);
  ~ClientCaps();

  // Capability checking and issuing
  void check_cap_issue(Inode *in, unsigned issued);
  int get_caps_issued(int fd);
  int get_caps_issued(const char *path, const UserPerm& perms);
  int get_caps_used(Inode *in);
  
  // Capability management
  void add_update_cap(Inode *in, MetaSession *session, uint64_t cap_id,
                      unsigned issued, unsigned wanted, unsigned seq, 
                      unsigned mseq, inodeno_t realm, int flags, 
                      const UserPerm& perms);
  void remove_cap(Cap *cap, bool queue_release);
  void remove_all_caps(Inode *in);
  void remove_session_caps(MetaSession *session, int err);
  
  // Capability flushing
  int mark_caps_flushing(Inode *in, ceph_tid_t *ptid);
  void adjust_session_flushing_caps(Inode *in, MetaSession *old_s, 
                                    MetaSession *new_s);
  void flush_caps_sync();
  void kick_flushing_caps(Inode *in, MetaSession *session);
  void kick_flushing_caps(MetaSession *session);
  void early_kick_flushing_caps(MetaSession *session);
  
  // Capability acquisition
  int get_caps(Fh *fh, int need, int want, int *have, loff_t endoff);
  
  // Capability reference counting
  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);
  
  // Capability snapshots
  void queue_cap_snap(Inode *in, const class SnapContext &old_snapc);
  void finish_cap_snap(Inode *in, CapSnap &capsnap, int used);

  // Capability synchronization
  void submit_sync_caps(Inode *in, ceph_tid_t want, class Context *onfinish);
  void wait_sync_caps(Inode *in, ceph_tid_t want);
  void wait_sync_caps(ceph_tid_t want);
  
  // Capability trimming and renewal
  void trim_caps(MetaSession *s, uint64_t max);
  void renew_caps();
  void renew_caps(MetaSession *session);
  void flush_cap_releases();
  void renew_and_flush_cap_releases();
  
  // Capability epoch barrier
  void set_cap_epoch_barrier(epoch_t e);
  epoch_t get_cap_epoch_barrier() const;
  
  // Capability delay management
  void cap_delay_requeue(Inode *in);
  void prepare_inode_unmount(Inode *in);
  template<typename Func>
  void process_delayed_caps(ceph::coarse_mono_time now, bool mount_aborted, Func&& func);
  
  // Capability sending
  void send_cap(Inode *in, MetaSession *session, Cap *cap, int flags,
                int used, int want, int retain, int flush, 
                ceph_tid_t flush_tid);
  
  // Capability snapshot flushing
  void send_flush_snap(Inode *in, MetaSession *session, snapid_t follows, 
                       class CapSnap& capsnap);
  void flush_snaps(Inode *in);
  
  // Check caps with flags
  static constexpr unsigned CHECK_CAPS_NODELAY = 0x1;
  static constexpr unsigned CHECK_CAPS_SYNCHRONOUS = 0x2;
  void check_caps(const InodeRef& in, unsigned flags);
  
  // Statistics (protected by caps_lock)
  int get_num_flushing_caps() const;
  void inc_num_flushing_caps();
  void dec_num_flushing_caps();
  ceph_tid_t get_last_flush_tid() const;
  ceph_tid_t allocate_flush_tid();
  
  // Caps release delay management
  std::chrono::seconds get_caps_release_delay() const { return caps_release_delay; }
  void set_caps_release_delay(std::chrono::seconds delay) { caps_release_delay = delay; }
  
  // Increment/decrement pinned icaps
  void inc_pinned_icaps();
  void dec_pinned_icaps(uint64_t nr = 1);

  static bool is_max_size_approaching(Inode *in);
  static int adjust_caps_used_for_lazyio(int used, int issued, int implemented);
  void wait_on_context_list(std::vector<Context*>& ls);
  // Wait on a cond after ls already contains a C_ReentrantCond (registered
  // under inode_lock so signal_caps_inode cannot run on an empty list).
  void wait_on_context_cond(ceph::reentrant_condition_variable& cond,
			    std::atomic<bool>& done,
			    std::atomic<bool>& wake_complete);
  void signal_context_list(std::vector<Context*>& ls) {
    finish_contexts(cct, ls, 0);
  }
  void signal_caps_inode(Inode *in);

private:
  Client *client;
  CephContext *cct;

  // Caps-specific lock - protects all caps state
  // mutable ceph::mutex caps_lock = ceph::make_mutex("ClientCaps::caps_lock");
  mutable ReentrantLock caps_lock = make_reentrant("ClientCaps::caps_lock", false);

  // Capability state
  ceph::coarse_mono_time last_cap_renew;
  epoch_t cap_epoch_barrier = 0;
  
  // Capability flushing state
  ceph_tid_t last_flush_tid = 1;
  xlist<Inode*> delayed_list;
  int num_flushing_caps = 0;
  
  // Capability release delay
  std::chrono::seconds caps_release_delay;
  
  // Helper methods (drop inode_lock briefly for client_lock-only snaprealm ops)
  SnapRealm *_get_snap_realm_unlocked(Inode *in, inodeno_t realm);
  void _put_snap_realm_unlocked(Inode *in, SnapRealm *realm);

  void _check_cap_issue(Inode *in, unsigned issued);
  int _get_caps_used(Inode *in);
  void _remove_cap(Cap *cap, bool queue_release);
  int _mark_caps_flushing(Inode *in, ceph_tid_t *ptid);
  void _trim_caps(MetaSession *s, uint64_t max);
  void _renew_caps();
  void _renew_caps(MetaSession *session);
  void _flush_cap_releases();
  void _check_caps(const InodeRef& in, unsigned flags);
};

#endif // CEPH_CLIENT_CAPS_H
