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
 * ClassicDispatchEngine.cc
 *
 * Inline implementation of MDSDispatchEngine for classic mode:
 *   submit_inbound  -> lock mds_lock, MDSDaemon::dispatch_inbound_locked()
 *   submit_callable -> lock mds_lock, run fn (for future timer/asok routing)
 *
 * IO completions remain in MDSIOContextBase::complete (finisher / inline lock).
 * submit_io_completion() mirrors the reactor execute path but is unused in classic.
 */

#include "ClassicDispatchEngine.h"

#include "common/debug.h"
#include "mds_lock_debug.h"

#include "MDSContext.h"
#include "MDSDaemon.h"
#include "MDSRank.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

void
ClassicDispatchEngine::start()
{}

void
ClassicDispatchEngine::shutdown()
{}

bool
ClassicDispatchEngine::is_reactor() const
{
  return false;
}

Dispatcher::dispatch_result_t
ClassicDispatchEngine::submit_inbound(const ref_t<Message>& m)
{
  ceph_assert(ctx.daemon != nullptr);
  ceph_assert(ctx.mds_lock != nullptr);

  mds::MdsLockGuard mds_lock_guard{
      *ctx.mds_lock, mds::dispatch_lane_owner_token(DispatchLane::Client)};
  if (ctx.daemon->stopping) {
    return false;
  }

  return ctx.daemon->dispatch_inbound_locked(m);
}

void
ClassicDispatchEngine::submit_io_completion(MDSIOContextBase* ioctx, int r)
{
  ceph_assert(ctx.rank != nullptr);
  ceph_assert(ctx.mds_lock != nullptr);

  MDSRank* mds = ctx.rank;

  dout(10) << "MDSIOContextBase::complete: " << typeid(*ioctx).name() << dendl;
  mds::MdsLockGuard mds_lock_guard{
      *ctx.mds_lock, mds::dispatch_lane_owner_token(DispatchLane::IOComplete)};

  if (mds->is_daemon_stopping()) {
    dout(4) << "MDSIOContextBase::complete: dropping for stopping "
            << typeid(*ioctx).name() << dendl;
    return;
  }

  if (r == -EBLOCKLISTED || r == -ETIMEDOUT) {
    derr << "MDSIOContextBase: failed with " << r << ", restarting..." << dendl;
    mds->respawn();
  } else {
    ioctx->MDSContext::complete(r);
  }
}

void
ClassicDispatchEngine::submit_advance_queues()
{}

void
ClassicDispatchEngine::submit_trim_tick()
{}

void
ClassicDispatchEngine::submit_log_trim_tick()
{}

void
ClassicDispatchEngine::submit_callable(
    DispatchLane lane,
    std::function<void()> fn)
{
  (void)lane;
  ceph_assert(ctx.mds_lock != nullptr);
  mds::MdsLockGuard mds_lock_guard{
      *ctx.mds_lock, mds::dispatch_lane_owner_token(lane)};
  fn();
}

void
ClassicDispatchEngine::note_finished_queued()
{}

ClassicDispatchEngine::ClassicDispatchEngine(const MDSDispatchContext& ctx_) :
  ctx(ctx_)
{}
