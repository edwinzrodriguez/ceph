// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * mds_lock_debug.h
 *
 * Debug helpers for MDSDaemon::mds_lock ownership in reactor dispatch mode.
 *
 * ## Background
 *
 * Most MDS rank state is serialized by mds_lock (a ceph::fair_mutex on
 * MDSDaemon).  Classic mode allows several threads to hold that lock in turn
 * (messenger dispatch, finisher, progress thread, etc.).  Reactor mode
 * (mds_dispatch_engine=reactor) moves rank-critical work onto a single op
 * thread (mds-rank-op) so client I/O sees less lock contention.
 *
 * ## Purpose
 *
 * While migrating to reactor mode, many call sites still assert
 * ceph_mutex_is_locked_by_me(mds_lock).  When one of those asserts fires it
 * is hard to tell *which* code path last acquired the lock.  These helpers:
 *
 *   1. Attach a short-lived debug "owner token" (a string label) to each
 *      mds_lock acquisition so assert failures log the active token.
 *   2. In reactor mode, abort if a token prefixed with "reactor:" is taken
 *      on a thread other than the registered op thread.
 *
 * Owner tokens are debug-only metadata stored on fair_mutex under
 * CEPH_DEBUG_MUTEX.  They do not affect lock ordering, lockdep, or release
 * behaviour.
 *
 * ## Token naming
 *
 * Tokens are stable string literals, not allocated memory.  Convention:
 *
 *   reactor:<work-kind>   rank work executed on the op thread
 *                         (see work_kind_owner_token())
 *   classic:<path>        classic dispatch engine entry points
 *                         (see dispatch_lane_owner_token())
 *   classic:progress      ProgressThread::_advance_queues()
 *
 * Daemon lifecycle paths (beacon, shutdown, admin socket) may hold mds_lock
 * without a token; that is expected and does not trigger reactor thread
 * checks.
 *
 * ## Usage
 *
 * Prefer MdsLockGuard when acquiring mds_lock:
 *
 *   mds::MdsLockGuard g{daemon.mds_lock, "classic:inbound"};
 *   ...
 *
 * Use MdsLockToken when the lock is already held (e.g. unique_lock around a
 * condition variable) and you only want to label a nested scope:
 *
 *   std::unique_lock l(mds->mds_lock);
 *   ...
 *   mds::MdsLockToken t{mds->mds_lock, "classic:progress"};
 *   mds->_advance_queues();
 *
 * Replace bare ceph_assert(ceph_mutex_is_locked_by_me(mds_lock)) with
 * MDS_ASSERT_MDS_LOCK(mds_lock) for richer diagnostics on failure.
 *
 * ReactorDispatchEngine registers the op thread via reactor_register_op_thread()
 * at thread start and deregisters on shutdown.  Dispatch engines pass
 * work_kind_owner_token() / dispatch_lane_owner_token() into MdsLockGuard.
 *
 * In non-debug builds (without CEPH_DEBUG_MUTEX), MdsLockGuard is a thin
 * lock_guard wrapper and MDS_ASSERT_MDS_LOCK falls back to the standard macro.
 */

#pragma once

#include <string_view>

#include "common/fair_mutex.h"
#include "dispatch/OpWorkItem.h"

namespace mds {

/// Owner token for reactor-mode OpWorkItem execution on the op thread.
const char* work_kind_owner_token(WorkKind kind);

/// Owner token for classic-mode dispatch engine entry points.
const char* dispatch_lane_owner_token(DispatchLane lane);

#ifdef CEPH_DEBUG_MUTEX

/// Acquire @p lock, set @p token, and verify reactor thread rules.
class MdsLockGuard {
  ceph::fair_mutex& lock;
  const char* prev_token;

public:
  MdsLockGuard(ceph::fair_mutex& lock_, const char* token);
  ~MdsLockGuard();

  MdsLockGuard(const MdsLockGuard&) = delete;
  MdsLockGuard& operator=(const MdsLockGuard&) = delete;
};

/// Tag an already-held mds_lock with a debug owner token for a nested scope.
class MdsLockToken {
  ceph::fair_mutex& lock;
  const char* prev_token;

public:
  MdsLockToken(ceph::fair_mutex& lock_, const char* token);
  ~MdsLockToken();

  MdsLockToken(const MdsLockToken&) = delete;
  MdsLockToken& operator=(const MdsLockToken&) = delete;
};

/// Called once from ReactorDispatchEngine::op_thread_main().
void reactor_register_op_thread();
/// Called when the reactor op thread exits.
void reactor_deregister_op_thread();
/// Abort if a reactor:* token is set on a non-op thread.
void reactor_assert_op_thread(std::string_view token);
/// Assert current thread holds @p lock; log @p site and owner token on failure.
void assert_mds_lock_held_by_me(ceph::fair_mutex& lock, const char* site);

#define MDS_ASSERT_MDS_LOCK(lock) \
  mds::assert_mds_lock_held_by_me((lock), __func__)

#else

class MdsLockGuard {
  std::lock_guard<ceph::fair_mutex> guard;

public:
  MdsLockGuard(ceph::fair_mutex& lock_, const char*) :
    guard(lock_)
  {}

  MdsLockGuard(const MdsLockGuard&) = delete;
  MdsLockGuard& operator=(const MdsLockGuard&) = delete;
};

#define MDS_ASSERT_MDS_LOCK(lock) ceph_assert(ceph_mutex_is_locked_by_me(lock))

#endif

} // namespace mds
