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
 * Shared helpers for MDS dispatch PerfCounters.
 *
 * Classic: call note_* to update the logger immediately (caller may be any
 * thread under mds_lock).
 *
 * Reactor: accumulate into LocalDispatchMetrics on the op thread (plain ints),
 * then flush_to_logger() periodically — avoids per-item atomic PerfCounters
 * traffic on the single-threaded hot path.
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>

#include "common/perf_counters.h"
#include "common/perf_histogram.h"

#include "OpWorkItem.h"
#include "classify.h"

class PerfCounters;

namespace mds::dispatch_perf {

/// Must match MDSRank PerfCountersBuilder histogram axis config.
inline constexpr int HIST_LATENCY_BUCKETS = 32;
inline constexpr int HIST_LANE_BUCKETS = static_cast<int>(DispatchLane::Count);
inline constexpr int WC_COUNT = static_cast<int>(DispatchWorkClass::Count);

inline PerfHistogramCommon::axis_config_d
latency_axis()
{
  return {
      "Latency (usec)", PerfHistogramCommon::SCALE_LOG2, 0, 100,
      HIST_LATENCY_BUCKETS};
}

inline PerfHistogramCommon::axis_config_d
lane_axis()
{
  return {
      "Dispatch lane", PerfHistogramCommon::SCALE_LINEAR, 0, 1,
      HIST_LANE_BUCKETS};
}

struct TimeAvg {
  uint64_t sum_ns = 0;
  uint64_t count = 0;

  void
  add_us(int64_t usec)
  {
    if (usec < 0) {
      usec = 0;
    }
    sum_ns += static_cast<uint64_t>(usec) * 1000ull;
    ++count;
  }

  void
  reset()
  {
    sum_ns = 0;
    count = 0;
  }
};

/**
 * Op-thread-only accumulators. Not atomic — single writer.
 */
struct LocalDispatchMetrics {
  uint64_t inbound = 0;
  uint64_t io_completions = 0;
  TimeAvg enqueue_total;
  std::array<TimeAvg, HIST_LANE_BUCKETS> enqueue_lane{};
  TimeAvg execute_total;
  std::array<TimeAvg, HIST_LANE_BUCKETS> execute_lane{};
  std::array<TimeAvg, WC_COUNT> execute_wc{};
  std::array<uint64_t, HIST_LATENCY_BUCKETS * HIST_LANE_BUCKETS> enqueue_hist{};
  std::array<uint64_t, HIST_LATENCY_BUCKETS * HIST_LANE_BUCKETS> execute_hist{};

  static int
  hist_index(int64_t latency_usec, DispatchLane lane)
  {
    const int bx = static_cast<int>(
        PerfHistogramCommon::get_bucket_for_axis(latency_usec, latency_axis()));
    const int by = static_cast<int>(PerfHistogramCommon::get_bucket_for_axis(
        static_cast<int64_t>(lane), lane_axis()));
    return bx * HIST_LANE_BUCKETS + by;
  }

  void
  note_enqueue(int64_t wait_usec, DispatchLane lane)
  {
    enqueue_total.add_us(wait_usec);
    enqueue_lane[static_cast<size_t>(lane)].add_us(wait_usec);
    enqueue_hist[hist_index(wait_usec, lane)]++;
  }

  void
  note_execute(
      int64_t exec_usec,
      DispatchLane lane,
      std::optional<DispatchWorkClass> wc)
  {
    execute_total.add_us(exec_usec);
    execute_lane[static_cast<size_t>(lane)].add_us(exec_usec);
    if (wc) {
      execute_wc[static_cast<size_t>(*wc)].add_us(exec_usec);
    }
    execute_hist[hist_index(exec_usec, lane)]++;
  }
};

/// Classic / direct: one sample into PerfCounters (atomic).
void note_enqueue_logger(
    PerfCounters* logger,
    int64_t wait_usec,
    DispatchLane lane);
void note_execute_logger(
    PerfCounters* logger,
    int64_t exec_usec,
    DispatchLane lane,
    std::optional<DispatchWorkClass> wc);
void note_inbound_logger(PerfCounters* logger);
void note_io_completion_logger(PerfCounters* logger);

/// Reactor: push LocalDispatchMetrics into PerfCounters and clear.
void flush_to_logger(PerfCounters* logger, LocalDispatchMetrics& local);

} // namespace mds::dispatch_perf
