// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_LOCK_ORDER_H
#define CEPH_CLIENT_LOCK_ORDER_H

#include "include/ceph_assert.h"
#include "common/reentrant_lock.h"

/**
 * Client lock-order rules and RAII helpers
 * ========================================
 *
 * Core invariant (debug-enforced)
 * --------------------------------
 * A thread must **never** hold Client::m_client_lock ("client_lock") and any
 * Inode::m_inode_lock ("inode_lock") at the same time.
 *
 * Both locks are ceph::ReentrantLock instances, but they use different RAII
 * wrappers (see client_lock.h and inode_lock.h).  Always use those wrappers —
 * do not call m_client_lock.lock() / m_inode_lock.lock() directly except inside
 * the wrappers themselves.
 *
 * Tiered locks (do not nest across tiers)
 * ---------------------------------------
 *   Tier 1 — client_lock     inode_map, sessions, requests, dispatch
 *   Tier 2 — session_lock    per-MDS dirty_list, cap_gen, session lists
 *   Tier 3 — inode_lock      per-inode caps, size, wait lists, cap_refs
 *
 * session_lock must not be held across paths that may need client_lock (e.g.
 * check_caps / cap_is_valid).  Snapshot session state under session_lock, then
 * process under inode_lock only.
 *
 * Typical usage by path
 * ---------------------
 *   * VFS hot paths (read/write): inode_lock only for the whole syscall.
 *   * ll_sync_inode / ll_fsync / fsync: call _fsync without an outer
 *     inode_lock; _fsync acquires inode_lock and must drop it across
 *     ObjectCacher and caps_lock cond waits so parallel writers can finish.
 *   * Metadata / ms_dispatch: client_lock only; drop it before taking
 *     inode_lock when inode state is required.
 *   * Cross-tier work (_put_inode, cap flush, send_request): finish under one
 *     lock, explicitly switch to the other — never overlap.
 *   * make_request: never hold any inode_lock across the call.  The MDS reply
 *     is handled on ms_dispatch (client_lock) and insert_trace needs the same
 *     inode_locks that ll_* entry points often hold via std::scoped_lock.
 *
 *
 * std::unique_lock specializations
 * --------------------------------
 * Declared in client_lock.h (Client) and inode_lock.h (Inode).  Each guard
 * adds or removes **one** reentrant recursion level, matching
 * std::unique_lock<ceph::ReentrantLock>.
 *
 *   std::unique_lock<Client> cl(*this);
 *     Acquires one client_lock level.  lock() and try_lock() call
 *     assert_no_inode_lock() first (debug builds abort if inode_lock_depth > 0).
 *
 *   std::unique_lock<Inode> in_lock(*in);
 *     Acquires one inode_lock level.  lock() and try_lock() call
 *     assert_no_client_lock() first.  Updates thread-local inode_lock_depth.
 *
 *   std::scoped_lock<Client> / std::scoped_lock<Inode>
 *     Thin aliases over the unique_lock specializations above.
 *
 * Construction options:
 *   defer_lock — create guard without acquiring; call lock() later.
 *   adopt_lock — caller already holds the lock; guard assumes ownership of one
 *                level (Inode adopt_lock also bumps inode_lock_depth).
 *
 *
 * ceph::unique_unlock specializations
 * -----------------------------------
 * Temporarily drop the **full** reentrant depth of a lock and restore it when
 * the unlock object is destroyed.  Built on ceph::unique_unlock<ReentrantLock>,
 * which saves recursion via release_for_wait() / restore_after_wait().
 *
 *   ceph::unique_unlock<Client> cl_drop(*this);
 *     Immediately releases every client_lock level held by this thread.
 *     Destructor reacquires the saved depth.  Pair with an outer
 *     std::unique_lock<Client> that remains in scope (its _owns flag stays
 *     true while the mutex is physically released).
 *
 *   ceph::unique_unlock<Inode> in_drop(*in);
 *     Same for inode_lock; decrements inode_lock_depth on release and bumps it
 *     again on reacquire.
 *
 *   defer_lock — construct without releasing; call release() manually (used by
 *     ceph::client_lock::scoped_drop).
 *
 *   _abandon() — mark the unlock as done without restoring (rare; used when a
 *     partner guard already reacquired the lock).
 *
 * Partner pattern (switch client_lock → inode_lock):
 *
 *   std::unique_lock<Client> cl(*this);          // outer guard, one level
 *   {
 *     ceph::unique_unlock<Client> drop(*this);   // drop all client levels
 *     std::unique_lock<Inode> in_lock(*in);     // safe: no client_lock held
 *     // ... inode-only work ...
 *   }                                            // drop restores client_lock
 *
 * Partner pattern (brief drop for blocking I/O, keep outer inode guard):
 *
 *   std::unique_lock<Inode> in_lock(*in);
 *   {
 *     ceph::unique_unlock<Inode> in_unlock(*in);
 *     objectcacher->file_write(...);             // must not hold inode_lock
 *   }                                            // full inode depth restored
 *
 * Reverse switch (inode_lock → client_lock):
 *
 *   std::unique_lock<Inode> in_lock(*in);
 *   {
 *     ceph::unique_unlock<Inode> drop(*in);
 *     std::unique_lock<Client> cl(*this);
 *     // ... client-only work ...
 *   }
 *
 * Re-entering without holding either lock (e.g. put_inode):
 *
 *   if (in->is_locked_by_me()) {
 *     ceph::unique_unlock<Inode> u(*in);
 *     put_inode(in, n);                          // recursive call, no inode_lock
 *     return;
 *   }
 *
 *
 * ceph::client_lock helpers (client_lock.h)
 * -----------------------------------------
 *   scoped_drop   — if client_lock is held, release full depth for the scope
 *                   (callbacks that must run inode-only, e.g. flush_set_callback).
 *   wait_on / wait_mount / wait_mount_for
 *                 — wait on a reentrant_condition_variable while coordinating
 *                   with a std::unique_lock<Client> partner guard.
 *   reacquire_after_drop
 *                 — lock client_lock when no partner unique_lock exists after
 *                   a ceph::unique_unlock<Client>.
 *
 *
 * Assertions vs control logic
 * ---------------------------
 * ceph_mutex_is_locked_by_me(m) and ceph_mutex_is_not_locked_by_me(m) are
 * compile-time stubs that evaluate to **true** in release builds (and in
 * CEPH_LOCKSTAT builds).  They exist only for documentation and for
 * ceph_assert — never use them in if/while/switch or any other control path.
 *
 * For runtime lock checks, call the real methods instead:
 *   Inode::is_locked_by_me()              — inode_lock held by this thread
 *   Client::is_locked_by_me()             — client_lock held by this thread
 *   in->m_inode_lock.is_locked_by_me()    — same, when you have the mutex
 *   client.get_client_lock().is_locked_by_me()
 *
 * Many helpers document preconditions with asserts (debug builds only):
 *   ceph_assert(ceph_mutex_is_locked_by_me(*in));   // caller holds inode_lock
 *   ceph_assert(ceph_mutex_is_locked_by_me(*this)); // caller holds client_lock
 *
 * Client::get_cap_ref requires the caller to hold inode_lock.
 * Client::put_cap_ref acquires inode_lock internally (do not call it while
 * holding client_lock).
 *
 *
 * Debug enforcement (CEPH_DEBUG_MUTEX)
 * ------------------------------------
 *   inode_lock_depth   — thread_local count maintained by unique_lock<Inode>
 *                        and unique_unlock<Inode>.
 *   assert_no_inode_lock()
 *                        — client_lock acquire must see depth == 0.
 *   assert_no_client_lock()
 *                        — inode_lock acquire must see client_lock not held.
 *   report_overlap_if_any()
 *                        — optional spot check; aborts if both are held.
 *
 * GDB:  p ceph::client_lock::order::inode_lock_depth
 *
 * See also: doc/dev/client-lock-order-plan.md
 */

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

// Detect overlap after the old stash/reacquire bridge was removed from inode_lock.h.
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