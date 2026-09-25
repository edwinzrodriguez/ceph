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
 */

/**
 * Helpers for off-thread MDSRank mutators under reactor dispatch.
 *
 * Finisher / quiesce-db / metrics / messenger threads must not take
 * mds_lock while the reactor op thread may run unlocked.  These helpers
 * hop onto DispatchLane::Control (or run inline / take the lock for
 * classic and shutdown).
 */

#pragma once

#include <future>
#include <utility>

#include "mds_lock_debug.h"

#include "dispatch/MDSDispatchEngine.h"
#include "dispatch/OpWorkItem.h"

#include "MDSRank.h"

namespace mds {

template <typename Fn>
void
run_rank_exclusive_async(MDSRank* mds, Fn&& fn)
{
  if (auto* engine = mds->get_dispatch_engine();
      engine && engine->is_reactor() && !reactor_is_op_thread() &&
      !mds->is_daemon_stopping()) {
    engine->submit_callable(
        DispatchLane::Control, [fn = std::forward<Fn>(fn)]() mutable { fn(); });
    return;
  }

  if (reactor_is_op_thread()) {
    fn();
    return;
  }

  std::lock_guard locker(mds->mds_lock);
  fn();
}

template <typename R, typename Fn>
R
run_rank_exclusive_sync(MDSRank* mds, Fn&& fn)
{
  if (auto* engine = mds->get_dispatch_engine();
      engine && engine->is_reactor() && !reactor_is_op_thread() &&
      !mds->is_daemon_stopping()) {
    std::promise<R> p;
    auto f = p.get_future();
    engine->submit_callable(
        DispatchLane::Control,
        [fn = std::forward<Fn>(fn), &p]() mutable { p.set_value(fn()); });
    return f.get();
  }

  if (reactor_is_op_thread()) {
    return fn();
  }

  std::lock_guard locker(mds->mds_lock);
  return fn();
}

} // namespace mds
