// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_LOCK_ORDER_H
#define CEPH_CLIENT_LOCK_ORDER_H

#include "include/ceph_assert.h"
#include "common/reentrant_lock.h"

namespace ceph::client_lock::order {

#ifdef CEPH_DEBUG_MUTEX

inline thread_local int inode_lock_depth = 0;

inline void on_inode_locked()
{
  ++inode_lock_depth;
}

inline void on_inode_unlocked()
{
  ceph_assert(inode_lock_depth > 0);
  --inode_lock_depth;
}

inline void assert_no_inode_lock()
{
  ceph_assert(inode_lock_depth == 0);
}

inline void assert_no_client_lock(const ceph::ReentrantLock& client)
{
  ceph_assert(!client.is_locked_by_me());
}

// PR1: detect overlap after the old stash/reacquire bridge (removed in PR2).
inline void report_overlap_if_any(const ceph::ReentrantLock& client)
{
  if (inode_lock_depth > 0 && client.is_locked_by_me()) {
    ceph_abort_msg(
      "client_lock and inode_lock overlap on the same thread");
  }
}

#else // !CEPH_DEBUG_MUTEX

inline void on_inode_locked() {}
inline void on_inode_unlocked() {}
inline void assert_no_inode_lock() {}
inline void assert_no_client_lock(const ceph::ReentrantLock&) {}
inline void report_overlap_if_any(const ceph::ReentrantLock&) {}

#endif // CEPH_DEBUG_MUTEX

} // namespace ceph::client_lock::order

#endif // CEPH_CLIENT_LOCK_ORDER_H