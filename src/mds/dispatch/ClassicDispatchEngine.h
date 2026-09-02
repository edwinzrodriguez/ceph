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
 * ClassicDispatchEngine.h
 *
 * Default dispatch backend (mds_dispatch_engine=classic). Preserves legacy
 * behavior: submit_inbound acquires mds_lock and runs dispatch_inbound_locked().
 * IO completions still use the legacy MDSIOContextBase::complete inline path;
 * submit_io_completion() exists for interface symmetry but is not used in classic.
 *
 * Used as the next phase pluggability wrapper and as the operational fallback
 * when reactor mode is unavailable. start/shutdown and queue-related submit_*
 * methods are no-ops. ProgressThread runs in classic mode only; reactor mode
 * drains finished_queue via note_finished_queued() on the op thread. MDCache
 * upkeep runs trim_quantum() inline under mds_lock (reactor posts submit_trim_tick()).
 * submit_callable() runs inline under mds_lock in classic mode.
 */

#pragma once

#include "MDSDispatchContext.h"
#include "MDSDispatchEngine.h"

class ClassicDispatchEngine : public MDSDispatchEngine {
public:
  explicit ClassicDispatchEngine(const MDSDispatchContext& ctx);

  void start() override;
  void shutdown() override;
  bool is_reactor() const override;

  Dispatcher::dispatch_result_t submit_inbound(const ref_t<Message>& m) override;
  void submit_io_completion(MDSIOContextBase* ctx, int r) override;
  void submit_advance_queues() override;
  void submit_trim_tick() override;
  void submit_log_trim_tick() override;
  void submit_callable(DispatchLane lane, std::function<void()> fn) override;
  void note_finished_queued() override;

private:
  MDSDispatchContext ctx;
};
