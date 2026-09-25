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

#include "ReactorDispatchEngine.h"

#include <algorithm>
#include <cstdint>
#include <optional>

#include "common/debug.h"
#include "mds_lock_debug.h"

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "log/Log.h"

#include "MDCache.h"
#include "MDLog.h"
#include "MDSContext.h"
#include "MDSDaemon.h"
#include "MDSRank.h"
#include "OpWorkItem.h"
#include "classify.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

static int64_t
dispatch_usec_since(ceph::fast_mono_time t)
{
  if (t == ceph::fast_mono_time{}) {
    return 0;
  }
  return std::chrono::duration_cast<std::chrono::microseconds>(
             ceph::fast_mono_clock::now() - t)
      .count();
}

static int
dispatch_enqueue_latency_lane_counter(DispatchLane lane)
{
  static const int counters[] = {
      l_mds_dispatch_enqueue_latency_control,
      l_mds_dispatch_enqueue_latency_io,
      l_mds_dispatch_enqueue_latency_maintenance,
      l_mds_dispatch_enqueue_latency_client,
  };
  return counters[static_cast<size_t>(lane)];
}

static int
dispatch_execute_latency_lane_counter(DispatchLane lane)
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
ReactorDispatchEngine::ExecWindow::reset(uint32_t n)
{
  window_n = std::clamp(n, 1u, CAP);
  sum = 0;
  filled = 0;
  next = 0;
}

void
ReactorDispatchEngine::ExecWindow::record(uint32_t usec)
{
  // Caller may pass a desired window via reset() first; record assumes
  // window_n is already set.
  if (filled == window_n) {
    sum -= samples[next];
  } else {
    ++filled;
  }
  samples[next] = usec;
  sum += usec;
  next = (next + 1) % window_n;
}

uint64_t
ReactorDispatchEngine::ExecWindow::avg_us() const
{
  if (filled == 0) {
    return 0;
  }
  return sum / filled;
}

void
ReactorDispatchEngine::note_enqueued()
{
  const size_t depth = queue.count();
  uint64_t max = queue_len_max.load(std::memory_order_relaxed);
  while (depth > max && !queue_len_max.compare_exchange_weak(
                            max, depth, std::memory_order_relaxed)) {
  }
  maybe_abort_on_queue_depth(depth);
}

void
ReactorDispatchEngine::refresh_cached_conf()
{
  queue_len_abort_limit.store(
      g_conf().get_val<uint64_t>("mds_reactor_queue_len_abort"),
      std::memory_order_relaxed);
  cache_trim_max_duration_ms.store(
      g_conf()
          .get_val<std::chrono::milliseconds>("mds_cache_trim_max_duration")
          .count(),
      std::memory_order_relaxed);
  log_trim_max_duration_ms.store(
      g_conf()
          .get_val<std::chrono::milliseconds>("mds_log_trim_max_duration")
          .count(),
      std::memory_order_relaxed);
  lane_slice_ms_cached[static_cast<size_t>(DispatchLane::Control)].store(
      g_conf()
          .get_val<std::chrono::milliseconds>("mds_reactor_lane_slice_control")
          .count(),
      std::memory_order_relaxed);
  lane_slice_ms_cached[static_cast<size_t>(DispatchLane::IOComplete)].store(
      g_conf()
          .get_val<std::chrono::milliseconds>("mds_reactor_lane_slice_io")
          .count(),
      std::memory_order_relaxed);
  lane_slice_ms_cached[static_cast<size_t>(DispatchLane::Maintenance)].store(
      g_conf()
          .get_val<std::chrono::milliseconds>(
              "mds_reactor_lane_slice_maintenance")
          .count(),
      std::memory_order_relaxed);
  lane_slice_ms_cached[static_cast<size_t>(DispatchLane::Client)].store(
      g_conf()
          .get_val<std::chrono::milliseconds>("mds_reactor_lane_slice_client")
          .count(),
      std::memory_order_relaxed);
  slice_backlog_target_us.store(
      static_cast<uint64_t>(g_conf()
                                .get_val<std::chrono::milliseconds>(
                                    "mds_reactor_slice_backlog_target")
                                .count()) *
          1000,
      std::memory_order_relaxed);
  client_slice_max_ms.store(
      g_conf()
          .get_val<std::chrono::milliseconds>(
              "mds_reactor_lane_slice_client_max")
          .count(),
      std::memory_order_relaxed);
  maintenance_slice_min_ms.store(
      g_conf()
          .get_val<std::chrono::milliseconds>(
              "mds_reactor_lane_slice_maintenance_min")
          .count(),
      std::memory_order_relaxed);
  const uint32_t win = static_cast<uint32_t>(std::clamp<uint64_t>(
      g_conf().get_val<uint64_t>("mds_reactor_slice_exec_window"), 1,
      ExecWindow::CAP));
  exec_window_n.store(win, std::memory_order_relaxed);
  // ExecWindow::record() on the op thread picks up a new window_n lazily.
}

void
ReactorDispatchEngine::handle_conf_change(const std::set<std::string>& changed)
{
  if (changed.count("mds_reactor_queue_len_abort") ||
      changed.count("mds_cache_trim_max_duration") ||
      changed.count("mds_log_trim_max_duration") ||
      changed.count("mds_reactor_lane_slice_control") ||
      changed.count("mds_reactor_lane_slice_io") ||
      changed.count("mds_reactor_lane_slice_maintenance") ||
      changed.count("mds_reactor_lane_slice_client") ||
      changed.count("mds_reactor_slice_exec_window") ||
      changed.count("mds_reactor_slice_backlog_target") ||
      changed.count("mds_reactor_lane_slice_client_max") ||
      changed.count("mds_reactor_lane_slice_maintenance_min")) {
    refresh_cached_conf();
  }
}

uint64_t
ReactorDispatchEngine::avg_exec_us(DispatchLane lane) const
{
  const auto& w = exec_windows[static_cast<size_t>(lane)];
  const uint32_t warm = std::min(w.window_n, 32u);
  if (w.sample_count() < warm) {
    // Conservative default so depth alone can trip adaptation early.
    return 20;
  }
  const uint64_t avg = w.avg_us();
  return avg == 0 ? 1 : avg;
}

uint64_t
ReactorDispatchEngine::estimated_backlog_us(DispatchLane lane) const
{
  return queue.count(lane) * avg_exec_us(lane);
}

uint64_t
ReactorDispatchEngine::client_backlog_us() const
{
  return estimated_backlog_us(DispatchLane::Client);
}

double
ReactorDispatchEngine::client_adapt_t() const
{
  const uint64_t target =
      slice_backlog_target_us.load(std::memory_order_relaxed);
  if (target == 0) {
    return 0.0;
  }
  const uint64_t backlog = client_backlog_us();
  constexpr double max_ratio = 4.0;
  const double ratio = static_cast<double>(backlog) /
                       static_cast<double>(target);
  if (ratio < 1.0) {
    return 0.0;
  }
  return std::min(1.0, (std::min(ratio, max_ratio) - 1.0) / (max_ratio - 1.0));
}

int64_t
ReactorDispatchEngine::lane_slice_ms(DispatchLane lane) const
{
  const int64_t base = lane_slice_ms_cached[static_cast<size_t>(lane)].load(
      std::memory_order_relaxed);
  // 0 means drain until empty — do not adapt.
  if (base <= 0) {
    return base;
  }
  if (lane != DispatchLane::Client && lane != DispatchLane::Maintenance) {
    return base;
  }

  const double t = client_adapt_t();
  if (t <= 0.0) {
    return base;
  }

  if (lane == DispatchLane::Client) {
    int64_t max_ms = client_slice_max_ms.load(std::memory_order_relaxed);
    if (max_ms < base) {
      max_ms = base;
    }
    return base + static_cast<int64_t>((max_ms - base) * t);
  }

  // Maintenance: cut toward floor under Client backlog pressure.
  int64_t min_ms = maintenance_slice_min_ms.load(std::memory_order_relaxed);
  if (min_ms < 0) {
    min_ms = 0;
  }
  if (min_ms > base) {
    min_ms = base;
  }
  return base - static_cast<int64_t>((base - min_ms) * t);
}

void
ReactorDispatchEngine::advance_lane_slice(
    size_t& lane_idx,
    ceph::fast_mono_time& slice_deadline)
{
  lane_idx = (lane_idx + 1) % static_cast<size_t>(DispatchLane::Count);
  const auto budget_ms = lane_slice_ms(static_cast<DispatchLane>(lane_idx));
  if (budget_ms <= 0) {
    // Zero means drain until empty this visit (no wall deadline).
    slice_deadline = ceph::fast_mono_time::max();
  } else {
    slice_deadline = ceph::fast_mono_clock::now() +
                     std::chrono::milliseconds(budget_ms);
  }
}

void
ReactorDispatchEngine::maybe_abort_on_queue_depth(size_t depth)
{
  const uint64_t limit = queue_len_abort_limit.load(std::memory_order_relaxed);
  if (limit == 0 || depth < limit) {
    return;
  }
  // Only one producer should dump+abort if many race past the threshold.
  if (queue_len_abort_armed.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  derr << "mds_reactor_queue_len_abort: dispatch queue depth " << depth
       << " >= limit " << limit
       << " (queue_len_max=" << queue_len_max.load(std::memory_order_relaxed)
       << ")" << dendl;
  publish_queue_depth_metrics();
  if (g_ceph_context && g_ceph_context->_log) {
    g_ceph_context->_log->dump_recent();
  }
  ceph_abort_msg(
      "mds_reactor_queue_len_abort: reactor dispatch queue depth exceeded "
      "mds_reactor_queue_len_abort");
}

void
ReactorDispatchEngine::publish_queue_depth_metrics()
{
  if (!ctx.rank || !ctx.rank->logger) {
    return;
  }

  PerfCounters* logger = ctx.rank->logger;
  logger->set(l_mds_reactor_dispatch_queue_len, queue.count());
  logger->set(
      l_mds_dispatch_queue_len_max,
      queue_len_max.load(std::memory_order_relaxed));
  logger->set(
      l_mds_reactor_dispatch_queue_len_client,
      queue.count(DispatchLane::Client));
  logger->set(
      l_mds_reactor_client_avg_exec_us, avg_exec_us(DispatchLane::Client));
  logger->set(l_mds_reactor_client_backlog_us, client_backlog_us());
  logger->set(
      l_mds_reactor_slice_client_effective_ms,
      lane_slice_ms(DispatchLane::Client));
  logger->set(
      l_mds_reactor_slice_maintenance_effective_ms,
      lane_slice_ms(DispatchLane::Maintenance));
}

void
ReactorDispatchEngine::enqueue_item(OpWorkItem* item, DispatchLane lane)
{
  queue.enqueue(item, lane);
  note_enqueued();
}

void
ReactorDispatchEngine::record_wait_metrics(const OpWorkItem& item)
{
  if (!ctx.rank || !ctx.rank->logger) {
    return;
  }

  PerfCounters* logger = ctx.rank->logger;
  const int64_t wait_usec = dispatch_usec_since(item.enqueued_at);
  const auto wait = std::chrono::microseconds(wait_usec);

  logger->tinc(l_mds_dispatch_enqueue_latency, wait);
  logger->tinc(dispatch_enqueue_latency_lane_counter(item.lane), wait);
  logger->hinc(
      l_mds_dispatch_enqueue_hist, wait_usec, static_cast<int64_t>(item.lane));
}

void
ReactorDispatchEngine::record_execute_metrics(
    const OpWorkItem& item,
    ceph::fast_mono_time exec_start)
{
  const int64_t exec_usec = dispatch_usec_since(exec_start);
  const uint32_t usec = exec_usec < 0
                            ? 0
                            : (exec_usec > UINT32_MAX
                                   ? UINT32_MAX
                                   : static_cast<uint32_t>(exec_usec));

  // Op-thread-only rolling window (update even if logger is absent).
  {
    auto& w = exec_windows[static_cast<size_t>(item.lane)];
    const uint32_t want = exec_window_n.load(std::memory_order_relaxed);
    if (want != w.window_n) {
      w.reset(want);
    }
    w.record(usec);
  }

  if (!ctx.rank || !ctx.rank->logger) {
    return;
  }

  PerfCounters* logger = ctx.rank->logger;
  const auto exec = std::chrono::microseconds(exec_usec);

  logger->tinc(l_mds_dispatch_execute_latency, exec);
  logger->tinc(dispatch_execute_latency_lane_counter(item.lane), exec);
  if (auto wc = classify_dispatch_work_class(item); wc) {
    logger->tinc(
        l_mds_dispatch_execute_latency_wc_first + static_cast<int>(*wc), exec);
  }
  logger->hinc(
      l_mds_dispatch_execute_hist, exec_usec, static_cast<int64_t>(item.lane));
}

ReactorDispatchEngine::ReactorDispatchEngine(const MDSDispatchContext& ctx_) :
  ctx(ctx_)
{
  refresh_cached_conf();
  const uint32_t win = exec_window_n.load(std::memory_order_relaxed);
  for (auto& w : exec_windows) {
    w.reset(win);
  }
}

ReactorDispatchEngine::~ReactorDispatchEngine() { shutdown(); }

void
ReactorDispatchEngine::start()
{
  stop.store(false);
  op_thread = std::thread(&ReactorDispatchEngine::op_thread_main, this);
}

void
ReactorDispatchEngine::shutdown()
{
  if (stop.exchange(true)) {
    return;
  }

  queue.shutdown();

  if (op_thread.joinable()) {
    op_thread.join();
  }

  queue.flush_and_clear();

  queue_len_max.store(0, std::memory_order_relaxed);
  trim_quantum_queued.store(false, std::memory_order_relaxed);
  log_trim_queued.store(false, std::memory_order_relaxed);
  if (ctx.rank && ctx.rank->logger) {
    ctx.rank->logger->set(l_mds_reactor_dispatch_queue_len, 0);
    ctx.rank->logger->set(l_mds_dispatch_queue_len_max, 0);
  }
}

Dispatcher::dispatch_result_t
ReactorDispatchEngine::submit_inbound(const ref_t<Message>& m)
{
  ceph_assert(ctx.daemon != nullptr);

  if (ctx.daemon->stopping) {
    return false;
  }

  const DispatchLane lane = classify_inbound_message(*m);
  OpWorkItem* item = OpWorkItem::create_inbound(m, lane);
  enqueue_item(item, lane);
  return true;
}

void
ReactorDispatchEngine::submit_io_completion(MDSIOContextBase* ioctx, int r)
{
  OpWorkItem* item = OpWorkItem::create_io(ioctx, r);
  enqueue_item(item, DispatchLane::IOComplete);
}

void
ReactorDispatchEngine::submit_advance_queues()
{
  OpWorkItem* item = OpWorkItem::create_advance();
  enqueue_item(item, DispatchLane::Control);
}

void
ReactorDispatchEngine::submit_trim_tick()
{
  // Single-flight: at most one TrimQuantum queued or running.
  if (trim_quantum_queued.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  OpWorkItem* item = OpWorkItem::create_trim();
  enqueue_item(item, DispatchLane::Maintenance);
}

void
ReactorDispatchEngine::submit_log_trim_tick()
{
  // Single-flight: at most one LogTrim queued or running.
  if (log_trim_queued.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  OpWorkItem* item = OpWorkItem::create_log_trim();
  enqueue_item(item, DispatchLane::Maintenance);
}

void
ReactorDispatchEngine::finish_trim_quantum(bool more)
{
  if (more && !stop.load(std::memory_order_relaxed)) {
    // Keep trim_quantum_queued set; continuation is the outstanding item.
    enqueue_item(OpWorkItem::create_trim(), DispatchLane::Maintenance);
    return;
  }
  trim_quantum_queued.store(false, std::memory_order_release);
}

void
ReactorDispatchEngine::finish_log_trim(bool more)
{
  if (more && !stop.load(std::memory_order_relaxed)) {
    enqueue_item(OpWorkItem::create_log_trim(), DispatchLane::Maintenance);
    return;
  }
  log_trim_queued.store(false, std::memory_order_release);
}

void
ReactorDispatchEngine::submit_callable(
    DispatchLane lane,
    std::function<void()> fn)
{
  OpWorkItem* item = OpWorkItem::create_callable(lane, std::move(fn));
  enqueue_item(item, lane);
}

void
ReactorDispatchEngine::note_finished_queued()
{
  submit_advance_queues();
}

void
ReactorDispatchEngine::set_boot_exclusive(bool exclusive)
{
  const bool was = boot_exclusive.exchange(exclusive, std::memory_order_acq_rel);
  if (was && !exclusive) {
    // Leaving boot exclusivity: wake so queued Client/Maintenance can drain.
    queue.wake();
  }
  dout(5) << __func__ << " exclusive=" << exclusive << " (was " << was << ")"
          << dendl;
}

bool
ReactorDispatchEngine::lane_drain_allowed(DispatchLane lane) const
{
  if (!boot_exclusive.load(std::memory_order_acquire)) {
    return true;
  }
  // Boot/replay: Control (maps, tick, Phase-1 callbacks) and IOComplete
  // (boot gathers / journal wrappers) only. Client and Maintenance stay
  // queued until set_boot_exclusive(false).
  return lane == DispatchLane::Control || lane == DispatchLane::IOComplete;
}

bool
ReactorDispatchEngine::has_drainable_work() const
{
  for (size_t i = 0; i < static_cast<size_t>(DispatchLane::Count); ++i) {
    const auto lane = static_cast<DispatchLane>(i);
    if (lane_drain_allowed(lane) && queue.count(lane) > 0) {
      return true;
    }
  }
  return false;
}

void
ReactorDispatchEngine::execute_io_completion(MDSIOContextBase* ioctx, int r)
{
  ceph_assert(ctx.rank != nullptr);
  ceph_assert(ctx.mds_lock != nullptr);

  MDSRank* mds = ctx.rank;

  dout(10) << "MDSIOContextBase::complete: " << typeid(*ioctx).name() << dendl;
  MDS_ASSERT_RANK_EXCLUSIVE(*ctx.mds_lock);

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
ReactorDispatchEngine::execute_item(OpWorkItem* item)
{
  ceph_assert(item != nullptr);
  ceph_assert(ctx.mds_lock != nullptr);

  record_wait_metrics(*item);

  const auto exec_start = ceph::fast_mono_clock::now();
  const char* token = mds::work_kind_owner_token(item->kind);

  // Boot/replay: MDLog replay/recovery still take mds_lock on dedicated
  // threads. Keep a real MdsLockGuard so Control/IOComplete serialize with
  // them. After boot, exclusivity is the registered op thread alone.
  std::optional<mds::MdsLockGuard> boot_lock;
  std::optional<mds::ReactorOpMdsLockHold> boot_hold;
  std::optional<mds::ReactorOwnerToken> owner_token;
  if (boot_exclusive.load(std::memory_order_acquire)) {
    boot_lock.emplace(*ctx.mds_lock, token);
    boot_hold.emplace();
  } else {
    owner_token.emplace(*ctx.mds_lock, token);
  }

  switch (item->kind) {
  case WorkKind::InboundMessage:
    if (ctx.rank && ctx.rank->logger) {
      ctx.rank->logger->inc(l_mds_dispatch_inbound);
    }
    if (ctx.daemon && !ctx.daemon->stopping) {
      (void)ctx.daemon->dispatch_inbound_locked(item->msg);
    }
    break;

  case WorkKind::IOCompletion:
    if (ctx.rank && ctx.rank->logger) {
      ctx.rank->logger->inc(l_mds_dispatch_io_completions);
    }
    execute_io_completion(item->io_ctx, item->rval);
    break;

  case WorkKind::AdvanceQueues:
    if (ctx.rank) {
      ctx.rank->_advance_queues();
    }
    break;

  case WorkKind::Callable:
    if (item->callable && *item->callable) {
      (*item->callable)();
    }
    break;

  case WorkKind::TrimQuantum:
    if (ctx.rank && ctx.rank->mdcache) {
      const auto budget = std::chrono::milliseconds(
          cache_trim_max_duration_ms.load(std::memory_order_relaxed));
      const auto more = ctx.rank->mdcache->trim_quantum(budget);
      finish_trim_quantum(more && *more);
    } else {
      finish_trim_quantum(false);
    }
    break;

  case WorkKind::LogTrim:
    if (ctx.rank && ctx.rank->mdlog) {
      const auto budget = std::chrono::milliseconds(
          log_trim_max_duration_ms.load(std::memory_order_relaxed));
      finish_log_trim(ctx.rank->mdlog->trim_tick(budget));
    } else {
      finish_log_trim(false);
    }
    break;
  }

  record_execute_metrics(*item, exec_start);
  item->destroy();
}

void
ReactorDispatchEngine::op_thread_main()
{
  ceph_pthread_setname("mds-rank-op");
#ifdef CEPH_LOCKSTAT
  lockstat_detail::LockStat::set_thread_iopath(true);
#endif
  mds::reactor_register_op_thread();

  size_t lane_idx = 0;
  ceph::fast_mono_time slice_deadline;
  {
    const auto budget_ms = lane_slice_ms(DispatchLane::Control);
    if (budget_ms <= 0) {
      slice_deadline = ceph::fast_mono_time::max();
    } else {
      slice_deadline = ceph::fast_mono_clock::now() +
                       std::chrono::milliseconds(budget_ms);
    }
  }

  while (!stop.load()) {
    unsigned processed = 0;
    // Bound work per wake so we still publish queue-depth metrics.
    constexpr unsigned max_items_per_wake = 256;
    while (processed < max_items_per_wake) {
      OpWorkItem* item = nullptr;
      size_t lanes_visited = 0;
      while (lanes_visited < static_cast<size_t>(DispatchLane::Count)) {
        const auto lane = static_cast<DispatchLane>(lane_idx);
        if (!lane_drain_allowed(lane)) {
          // Boot exclusivity: leave Client/Maintenance queued.
          advance_lane_slice(lane_idx, slice_deadline);
          ++lanes_visited;
          continue;
        }
        if (ceph::fast_mono_clock::now() >= slice_deadline) {
          advance_lane_slice(lane_idx, slice_deadline);
          ++lanes_visited;
          continue;
        }
        item = queue.dequeue_lane(lane);
        if (item) {
          break;
        }
        // Lane empty before budget expired: yield to next priority.
        advance_lane_slice(lane_idx, slice_deadline);
        ++lanes_visited;
      }
      if (!item) {
        break; // all drainable lanes empty
      }
      execute_item(item);
      ++processed;
    }

    if (processed > 0) {
      publish_queue_depth_metrics();
      continue;
    }

    std::unique_lock lock(queue.wait_lock());
    queue.wait_cond().wait_for(lock, std::chrono::milliseconds(10), [this] {
      return stop.load() || has_drainable_work();
    });
  }

  mds::reactor_deregister_op_thread();
}
