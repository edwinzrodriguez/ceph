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
 * MDSDispatchEngine.h
 *
 * Strategy interface for how work enters the existing MDS metadata stack.
 * Selected at rank startup via mds_dispatch_engine (classic | reactor).
 *
 * Producers call submit_* from any thread; the engine decides whether to run
 * inline (classic) or enqueue for a single op thread (reactor).
 * Everything below the submit boundary (Server, MDCache, MDRequest) is shared.
 *
 * Integration points (callers):
 *   - MDSDaemon::ms_dispatch2        -> submit_inbound()
 *   - MDSIOContextBase::complete     -> submit_io_completion() (reactor only)
 *   - C_IO_Wrapper::complete         -> finisher (classic) or submit_io (reactor)
 *   - MDCache upkeep                  -> submit_trim_tick() (reactor)
 *   - MDLog log_trim_upkeep           -> submit_log_trim_tick() (reactor)
 *   - MDSRank finished_queue           -> note_finished_queued() (reactor)
 *   - MDSDaemon timer / mutating asok -> submit_callable(Control) (reactor)
 *   - Messenger accept/reset           -> submit_callable(Control) (reactor)
 *   - Metrics get_path/rbytes          -> submit_callable(Control)+future
 *   - Write-error / purge / conf-change -> submit_callable(Control) (reactor)
 *   - Quiesce send_ack/listing/agent    -> submit_callable(Control)+future
 *
 * Phase 2 (reactor boot exclusivity): while creating/starting/replay,
 * Client and Maintenance lanes are not drained so MDLog replay/recovery
 * threads can keep taking mds_lock without racing unlocked execute_item.
 * Control + IOComplete still drain (boot gathers, maps, tick).
 *
 * Owned by MDSRank; started in MDSRankDispatcher::init(), stopped in shutdown().
 */

#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>

#include "include/common_fwd.h"
#include "msg/Dispatcher.h"

#include "OpWorkItem.h"

class MDSIOContextBase;
struct MDSDispatchContext;

class MDSDispatchEngine {
public:
  virtual ~MDSDispatchEngine() = default;

  virtual void start() = 0;
  virtual void shutdown() = 0;
  /// true when the engine uses a single op thread instead of contended mds_lock
  virtual bool is_reactor() const = 0;

  /// Messenger / ms_dispatch2 entry; may run inline or enqueue.
  virtual Dispatcher::dispatch_result_t submit_inbound(
      const ref_t<Message>& m) = 0;
  /// Objecter / finisher IO completion path.
  virtual void submit_io_completion(MDSIOContextBase* ctx, int r) = 0;
  /// Drain finished_queue / laggy deferred messages (reactor).
  virtual void submit_advance_queues() = 0;
  /// Bounded cache trim slice (reactor).
  virtual void submit_trim_tick() = 0;
  /// Journal segment trim slice (reactor).
  virtual void submit_log_trim_tick() = 0;
  /// Timer, asok, or other callbacks that need mds_lock (reactor).
  virtual void submit_callable(DispatchLane lane, std::function<void()> fn) = 0;
  /// Hint that finished_queue has new continuations (reactor).
  virtual void note_finished_queued() = 0;

  /// Phase 2 Option A: when true, reactor drains only Control + IOComplete
  /// (Client/Maintenance stay queued). Classic ignores.
  virtual void
  set_boot_exclusive(bool exclusive)
  {}

  /// Refresh cached runtime options used on enqueue/execute hot paths.
  virtual void
  handle_conf_change(const std::set<std::string>& changed)
  {}

  /// Factory: reads mds_dispatch_engine (classic | reactor).
  static std::unique_ptr<MDSDispatchEngine> create(
      const MDSDispatchContext& ctx);
};
