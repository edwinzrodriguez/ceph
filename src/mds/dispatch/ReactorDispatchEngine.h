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
 * ReactorDispatchEngine.h
 *
 * Reactor-mode dispatch backend (mds_dispatch_engine=reactor). Producers enqueue
 * OpWorkItem instances; a single op thread drains MDSOpWorkQueue and executes
 * work under mds_lock without other threads contending for that lock.
 */

#pragma once

#include <atomic>
#include <thread>

#include "MDSDispatchContext.h"
#include "MDSDispatchEngine.h"
#include "MDSOpWorkQueue.h"

class ReactorDispatchEngine : public MDSDispatchEngine {
public:
  explicit ReactorDispatchEngine(const MDSDispatchContext& ctx);
  ~ReactorDispatchEngine() override;

  void start() override;
  void shutdown() override;

  bool
  is_reactor() const override
  {
    return true;
  }

  Dispatcher::dispatch_result_t submit_inbound(const ref_t<Message>& m) override;
  void submit_io_completion(MDSIOContextBase* ctx, int r) override;
  void submit_advance_queues() override;
  void submit_trim_tick() override;
  void submit_callable(DispatchLane lane, std::function<void()> fn) override;
  void note_finished_queued() override;

private:
  void op_thread_main();
  void execute_item(OpWorkItem* item);
  void execute_io_completion(MDSIOContextBase* ioctx, int r);

  MDSDispatchContext ctx;
  MDSOpWorkQueue queue;
  std::thread op_thread;
  std::atomic<bool> stop{false};
};
