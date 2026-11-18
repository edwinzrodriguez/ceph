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
 *   - MDSIOContextBase::complete     -> submit_io_completion()
 *   - MDCache upkeep (future)        -> submit_trim_tick()
 *   - MDSRank finished_queue (future)-> note_finished_queued()
 *
 * Owned by MDSRank; started in MDSRankDispatcher::init(), stopped in shutdown().
 */

#pragma once

#include <functional>
#include <memory>

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
  /// Timer, asok, or other callbacks that need mds_lock (reactor).
  virtual void submit_callable(DispatchLane lane, std::function<void()> fn) = 0;
  /// Hint that finished_queue has new continuations (reactor).
  virtual void note_finished_queued() = 0;

  /// Factory: reads mds_dispatch_engine; currently always returns classic.
  static std::unique_ptr<MDSDispatchEngine> create(
      const MDSDispatchContext& ctx);
};
