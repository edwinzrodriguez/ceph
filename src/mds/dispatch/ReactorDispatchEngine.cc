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

#include "common/debug.h"

#include "common/perf_counters.h"
#include "include/compat.h"

#include "MDCache.h"
#include "MDSContext.h"
#include "MDSDaemon.h"
#include "MDSRank.h"
#include "OpWorkItem.h"
#include "classify.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

static int64_t
dispatch_usec_since(ceph::coarse_mono_time t)
{
  if (t == ceph::coarse_mono_time{}) {
    return 0;
  }
  return std::chrono::duration_cast<std::chrono::microseconds>(
             ceph::coarse_mono_clock::now() - t)
      .count();
}

static int
dispatch_enqueue_usec_lane_counter(DispatchLane lane)
{
  static const int counters[] = {
      l_mds_dispatch_enqueue_usec_control,
      l_mds_dispatch_enqueue_usec_io,
      l_mds_dispatch_enqueue_usec_client,
      l_mds_dispatch_enqueue_usec_maintenance,
  };
  return counters[static_cast<size_t>(lane)];
}

static int
dispatch_execute_usec_lane_counter(DispatchLane lane)
{
  static const int counters[] = {
      l_mds_dispatch_execute_usec_control,
      l_mds_dispatch_execute_usec_io,
      l_mds_dispatch_execute_usec_client,
      l_mds_dispatch_execute_usec_maintenance,
  };
  return counters[static_cast<size_t>(lane)];
}

void
ReactorDispatchEngine::update_queue_depth_metrics()
{
  if (!ctx.rank || !ctx.rank->logger) {
    return;
  }

  const size_t depth = queue.count();
  PerfCounters* logger = ctx.rank->logger;

  logger->set(l_mds_reactor_dispatch_queue_len, depth);

  uint64_t max = queue_len_max.load(std::memory_order_relaxed);
  while (depth > max && !queue_len_max.compare_exchange_weak(
                            max, depth, std::memory_order_relaxed)) {
  }
  logger->set(l_mds_dispatch_queue_len_max, queue_len_max.load());
}

void
ReactorDispatchEngine::enqueue_item(OpWorkItem* item, DispatchLane lane)
{
  queue.enqueue(item, lane);
  update_queue_depth_metrics();
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

  logger->tinc(l_mds_dispatch_enqueue_usec, wait);
  logger->tinc(dispatch_enqueue_usec_lane_counter(item.lane), wait);
  logger->hinc(
      l_mds_dispatch_enqueue_hist, wait_usec, static_cast<int64_t>(item.lane));
}

void
ReactorDispatchEngine::record_execute_metrics(
    DispatchLane lane,
    ceph::coarse_mono_time exec_start)
{
  if (!ctx.rank || !ctx.rank->logger) {
    return;
  }

  PerfCounters* logger = ctx.rank->logger;
  const int64_t exec_usec = dispatch_usec_since(exec_start);
  const auto exec = std::chrono::microseconds(exec_usec);

  logger->tinc(l_mds_dispatch_execute_usec, exec);
  logger->tinc(dispatch_execute_usec_lane_counter(lane), exec);
  logger->hinc(
      l_mds_dispatch_execute_hist, exec_usec, static_cast<int64_t>(lane));
}

ReactorDispatchEngine::ReactorDispatchEngine(const MDSDispatchContext& ctx_) :
  ctx(ctx_)
{}

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
  OpWorkItem* item = OpWorkItem::create_trim();
  enqueue_item(item, DispatchLane::Maintenance);
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
ReactorDispatchEngine::execute_io_completion(MDSIOContextBase* ioctx, int r)
{
  ceph_assert(ctx.rank != nullptr);
  ceph_assert(ctx.mds_lock != nullptr);

  MDSRank* mds = ctx.rank;

  dout(10) << "MDSIOContextBase::complete: " << typeid(*ioctx).name() << dendl;

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

  const auto exec_start = ceph::coarse_mono_clock::now();
  const DispatchLane lane = item->lane;

  {
    std::lock_guard lock(*ctx.mds_lock);

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
        ctx.rank->mdcache->trim_quantum();
      }
      break;
    }
  }

  record_execute_metrics(lane, exec_start);
  item->destroy();
}

void
ReactorDispatchEngine::op_thread_main()
{
  ceph_pthread_setname("mds-rank-op");
#ifdef CEPH_LOCKSTAT
  lockstat_detail::LockStat::set_thread_iopath(true);
#endif

  while (!stop.load()) {
    if (OpWorkItem* item = queue.dequeue()) {
      update_queue_depth_metrics();
      execute_item(item);
      continue;
    }

    std::unique_lock lock(queue.wait_lock());
    queue.wait_cond().wait_for(lock, std::chrono::milliseconds(10), [this] {
      return stop.load() || queue.has_work_for_consumer();
    });
  }
}
