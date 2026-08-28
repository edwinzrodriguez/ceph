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

#include "mds_lock_debug.h"

#include <atomic>
#include <thread>

#include "common/debug.h"

#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

namespace mds {

const char*
work_kind_owner_token(WorkKind kind)
{
  switch (kind) {
  case WorkKind::InboundMessage:
    return "reactor:inbound";
  case WorkKind::IOCompletion:
    return "reactor:io_complete";
  case WorkKind::AdvanceQueues:
    return "reactor:advance_queues";
  case WorkKind::TrimQuantum:
    return "reactor:trim_quantum";
  case WorkKind::Callable:
    return "reactor:callable";
  }
  return "reactor:unknown";
}

const char*
dispatch_lane_owner_token(DispatchLane lane)
{
  switch (lane) {
  case DispatchLane::Control:
    return "classic:callable:control";
  case DispatchLane::IOComplete:
    return "classic:io_complete";
  case DispatchLane::Client:
    return "classic:inbound";
  case DispatchLane::Maintenance:
    return "classic:callable:maintenance";
  case DispatchLane::Count:
    break;
  }
  return "classic:unknown";
}

#ifdef CEPH_DEBUG_MUTEX

namespace {

std::atomic<std::thread::id> reactor_op_thread{};
std::atomic<bool> reactor_op_thread_registered{false};

bool
is_reactor_rank_token(std::string_view token)
{
  static constexpr std::string_view prefix = "reactor:";
  return token.size() >= prefix.size() &&
         token.compare(0, prefix.size(), prefix) == 0;
}

} // namespace

void
reactor_register_op_thread()
{
  reactor_op_thread.store(std::this_thread::get_id(), std::memory_order_release);
  reactor_op_thread_registered.store(true, std::memory_order_release);
}

void
reactor_deregister_op_thread()
{
  reactor_op_thread_registered.store(false, std::memory_order_release);
  reactor_op_thread.store(std::thread::id{}, std::memory_order_release);
}

void
reactor_assert_op_thread(std::string_view token)
{
  if (!is_reactor_rank_token(token)) {
    return;
  }
  if (!reactor_op_thread_registered.load(std::memory_order_acquire)) {
    return;
  }

  const auto expected = reactor_op_thread.load(std::memory_order_acquire);
  const auto self = std::this_thread::get_id();
  if (expected == self) {
    return;
  }

  derr << "reactor rank lock token=" << token << " taken on unexpected thread"
       << dendl;
  ceph_abort_msg("reactor mds_lock owner thread mismatch");
}

MdsLockGuard::MdsLockGuard(ceph::fair_mutex& lock_, const char* token) :
  lock(lock_), prev_token(lock.debug_get_owner_token())
{
  lock.lock();
  lock.debug_set_owner_token(token);
  reactor_assert_op_thread(token);
}

MdsLockGuard::~MdsLockGuard()
{
  lock.debug_set_owner_token(prev_token);
  lock.unlock();
}

MdsLockToken::MdsLockToken(ceph::fair_mutex& lock_, const char* token) :
  lock(lock_), prev_token(lock.debug_get_owner_token())
{
  ceph_assert(lock.is_locked_by_me());
  lock.debug_set_owner_token(token);
  reactor_assert_op_thread(token);
}

MdsLockToken::~MdsLockToken() { lock.debug_set_owner_token(prev_token); }

void
assert_mds_lock_held_by_me(ceph::fair_mutex& lock, const char* site)
{
  if (lock.is_locked_by_me()) {
    return;
  }

  const char* token = lock.debug_get_owner_token();
  derr << "mds_lock not held by current thread at " << site
       << " owner_token=" << (token ? token : "(none)")
       << " locked=" << (lock.is_locked() ? "yes" : "no") << dendl;
  ceph_abort_msg("mds_lock not held by me");
}

#endif

} // namespace mds
