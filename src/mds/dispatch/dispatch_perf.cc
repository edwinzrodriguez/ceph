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

#include "dispatch_perf.h"

#include <chrono>

#include "MDSRank.h"

namespace mds::dispatch_perf {

namespace {

int
enqueue_lane_counter(DispatchLane lane)
{
  static const int counters[] = {
      l_mds_dispatch_enqueue_latency_control,
      l_mds_dispatch_enqueue_latency_io,
      l_mds_dispatch_enqueue_latency_maintenance,
      l_mds_dispatch_enqueue_latency_client,
  };
  return counters[static_cast<size_t>(lane)];
}

int
execute_lane_counter(DispatchLane lane)
{
  static const int counters[] = {
      l_mds_dispatch_execute_latency_control,
      l_mds_dispatch_execute_latency_io,
      l_mds_dispatch_execute_latency_maintenance,
      l_mds_dispatch_execute_latency_client,
  };
  return counters[static_cast<size_t>(lane)];
}

void
flush_time_avg(PerfCounters* logger, int idx, TimeAvg& avg)
{
  if (avg.count == 0) {
    return;
  }
  logger->tinc_n(idx, std::chrono::nanoseconds(avg.sum_ns), avg.count);
  avg.reset();
}

void
flush_hist(
    PerfCounters* logger,
    int idx,
    std::array<uint64_t, HIST_LATENCY_BUCKETS * HIST_LANE_BUCKETS>& hist)
{
  for (int bx = 0; bx < HIST_LATENCY_BUCKETS; ++bx) {
    for (int by = 0; by < HIST_LANE_BUCKETS; ++by) {
      uint64_t& n = hist[bx * HIST_LANE_BUCKETS + by];
      if (n == 0) {
        continue;
      }
      logger->hinc_bucket(idx, bx, by, n);
      n = 0;
    }
  }
}

} // namespace

void
note_enqueue_logger(PerfCounters* logger, int64_t wait_usec, DispatchLane lane)
{
  if (!logger) {
    return;
  }
  if (wait_usec < 0) {
    wait_usec = 0;
  }
  const auto wait = std::chrono::microseconds(wait_usec);
  logger->tinc(l_mds_dispatch_enqueue_latency, wait);
  logger->tinc(enqueue_lane_counter(lane), wait);
  logger->hinc(
      l_mds_dispatch_enqueue_hist, wait_usec, static_cast<int64_t>(lane));
}

void
note_execute_logger(
    PerfCounters* logger,
    int64_t exec_usec,
    DispatchLane lane,
    std::optional<DispatchWorkClass> wc)
{
  if (!logger) {
    return;
  }
  if (exec_usec < 0) {
    exec_usec = 0;
  }
  const auto exec = std::chrono::microseconds(exec_usec);
  logger->tinc(l_mds_dispatch_execute_latency, exec);
  logger->tinc(execute_lane_counter(lane), exec);
  if (wc) {
    logger->tinc(
        l_mds_dispatch_execute_latency_wc_first + static_cast<int>(*wc), exec);
  }
  logger->hinc(
      l_mds_dispatch_execute_hist, exec_usec, static_cast<int64_t>(lane));
}

void
note_inbound_logger(PerfCounters* logger)
{
  if (logger) {
    logger->inc(l_mds_dispatch_inbound);
  }
}

void
note_io_completion_logger(PerfCounters* logger)
{
  if (logger) {
    logger->inc(l_mds_dispatch_io_completions);
  }
}

void
flush_to_logger(PerfCounters* logger, LocalDispatchMetrics& local)
{
  if (!logger) {
    return;
  }

  if (local.inbound) {
    logger->inc(l_mds_dispatch_inbound, local.inbound);
    local.inbound = 0;
  }
  if (local.io_completions) {
    logger->inc(l_mds_dispatch_io_completions, local.io_completions);
    local.io_completions = 0;
  }

  flush_time_avg(logger, l_mds_dispatch_enqueue_latency, local.enqueue_total);
  for (size_t i = 0; i < local.enqueue_lane.size(); ++i) {
    flush_time_avg(
        logger, enqueue_lane_counter(static_cast<DispatchLane>(i)),
        local.enqueue_lane[i]);
  }

  flush_time_avg(logger, l_mds_dispatch_execute_latency, local.execute_total);
  for (size_t i = 0; i < local.execute_lane.size(); ++i) {
    flush_time_avg(
        logger, execute_lane_counter(static_cast<DispatchLane>(i)),
        local.execute_lane[i]);
  }
  for (size_t i = 0; i < local.execute_wc.size(); ++i) {
    flush_time_avg(
        logger, l_mds_dispatch_execute_latency_wc_first + static_cast<int>(i),
        local.execute_wc[i]);
  }

  flush_hist(logger, l_mds_dispatch_enqueue_hist, local.enqueue_hist);
  flush_hist(logger, l_mds_dispatch_execute_hist, local.execute_hist);
}

} // namespace mds::dispatch_perf
