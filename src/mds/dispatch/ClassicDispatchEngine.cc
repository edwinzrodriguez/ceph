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
 *   submit_io_completion -> lock mds_lock, MDSContext::complete() path
 *   submit_callable -> lock mds_lock, run fn (for future timer/asok routing)
 *
 * No queuing or extra threads; intended to be behavior-identical to original MDS.
 */

#include "ClassicDispatchEngine.h"

#include "common/debug.h"

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

  std::lock_guard l(*ctx.mds_lock);
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
  std::lock_guard l(*ctx.mds_lock);

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
ClassicDispatchEngine::submit_callable(
    DispatchLane lane,
    std::function<void()> fn)
{
  (void)lane;
  ceph_assert(ctx.mds_lock != nullptr);
  std::lock_guard l(*ctx.mds_lock);
  fn();
}

void
ClassicDispatchEngine::note_finished_queued()
{}

ClassicDispatchEngine::ClassicDispatchEngine(const MDSDispatchContext& ctx_) :
  ctx(ctx_)
{}
