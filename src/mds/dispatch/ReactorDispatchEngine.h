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
 * ReactorDispatchEngine.h
 *
 * Reactor-mode dispatch backend (mds_dispatch_engine=reactor). Producers enqueue
 * OpWorkItem instances; a single op thread drains MDSOpWorkQueue and executes.
 *
 * Lane order (high -> low): Control, IOComplete, Maintenance, Client.
 * Each scheduling round visits lanes in that order and runs each lane until
 * empty or its wall-clock budget expires. Base budgets come from
 * mds_reactor_lane_slice_*; Client/Maintenance may adapt from estimated Client
 * backlog (depth * rolling avg execute time) once backlog exceeds
 * mds_reactor_slice_backlog_target.
 * TrimQuantum/LogTrim are single-flight and cooperatively time-sliced.
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <set>
#include <string>
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
  void submit_log_trim_tick() override;
  void submit_callable(DispatchLane lane, std::function<void()> fn) override;
  void note_finished_queued() override;
  void handle_conf_change(const std::set<std::string>& changed) override;

private:
  /// Op-thread-only rolling window of recent execute times (µs) per lane.
  struct ExecWindow {
    static constexpr uint32_t CAP = 1024;
    std::array<uint32_t, CAP> samples{};
    uint64_t sum = 0;
    uint32_t filled = 0;
    uint32_t next = 0;
    uint32_t window_n = 256;

    void reset(uint32_t n);
    void record(uint32_t usec);
    uint64_t avg_us() const;

    uint32_t
    sample_count() const
    {
      return filled;
    }
  };

  void op_thread_main();
  void enqueue_item(OpWorkItem* item, DispatchLane lane);
  void execute_item(OpWorkItem* item);
  void execute_io_completion(MDSIOContextBase* ioctx, int r);
  void record_wait_metrics(const OpWorkItem& item);
  void record_execute_metrics(
      const OpWorkItem& item,
      ceph::coarse_mono_time exec_start);
  void note_enqueued();
  void maybe_abort_on_queue_depth(size_t depth);
  void publish_queue_depth_metrics();
  void finish_trim_quantum(bool more);
  void finish_log_trim(bool more);
  void refresh_cached_conf();
  int64_t lane_slice_ms(DispatchLane lane) const;
  void advance_lane_slice(
      size_t& lane_idx,
      ceph::coarse_mono_time& slice_deadline);
  uint64_t avg_exec_us(DispatchLane lane) const;
  uint64_t estimated_backlog_us(DispatchLane lane) const;
  uint64_t client_backlog_us() const;
  double client_adapt_t() const;

  MDSDispatchContext ctx;
  MDSOpWorkQueue queue;
  std::thread op_thread;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> queue_len_max{0};
  std::atomic<bool> queue_len_abort_armed{false};
  /// Cached conf (avoid string-keyed get_val on enqueue/execute).
  std::atomic<uint64_t> queue_len_abort_limit{0};
  std::atomic<int64_t> cache_trim_max_duration_ms{0};
  std::atomic<int64_t> log_trim_max_duration_ms{0};
  std::array<std::atomic<int64_t>, static_cast<size_t>(DispatchLane::Count)>
      lane_slice_ms_cached{};
  std::atomic<uint64_t> slice_backlog_target_us{500'000};
  std::atomic<int64_t> client_slice_max_ms{20};
  std::atomic<int64_t> maintenance_slice_min_ms{1};
  std::atomic<uint32_t> exec_window_n{256};
  /// At most one TrimQuantum / LogTrim outstanding (queued or running).
  std::atomic<bool> trim_quantum_queued{false};
  std::atomic<bool> log_trim_queued{false};

  /// Written only by the op thread.
  std::array<ExecWindow, static_cast<size_t>(DispatchLane::Count)> exec_windows{};
};
