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

#include "include/compat.h"

#include "MDSContext.h"
#include "MDSDaemon.h"
#include "MDSRank.h"
#include "OpWorkItem.h"
#include "classify.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

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
  queue.enqueue(item, lane);
  return true;
}

void
ReactorDispatchEngine::submit_io_completion(MDSIOContextBase* ioctx, int r)
{
  OpWorkItem* item = OpWorkItem::create_io(ioctx, r);
  queue.enqueue(item, DispatchLane::IOComplete);
}

void
ReactorDispatchEngine::submit_advance_queues()
{
  queue.wake();
}

void
ReactorDispatchEngine::submit_trim_tick()
{
  queue.wake();
}

void
ReactorDispatchEngine::submit_callable(
    DispatchLane lane,
    std::function<void()> fn)
{
  OpWorkItem* item = OpWorkItem::create_callable(lane, std::move(fn));
  queue.enqueue(item, lane);
}

void
ReactorDispatchEngine::note_finished_queued()
{
  queue.wake();
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

  std::lock_guard lock(*ctx.mds_lock);

  switch (item->kind) {
  case WorkKind::InboundMessage:
    if (ctx.daemon && !ctx.daemon->stopping) {
      (void)ctx.daemon->dispatch_inbound_locked(item->msg);
    }
    break;

  case WorkKind::IOCompletion:
    execute_io_completion(item->io_ctx, item->rval);
    break;

  case WorkKind::Callable:
    if (item->callable && *item->callable) {
      (*item->callable)();
    }
    break;

  case WorkKind::TrimQuantum:
    break;
  }

  item->destroy();
}

void
ReactorDispatchEngine::op_thread_main()
{
  ceph_pthread_setname("mds-rank-op");

  while (!stop.load()) {
    if (OpWorkItem* item = queue.dequeue()) {
      execute_item(item);
      continue;
    }

    std::unique_lock lock(queue.wait_lock());
    queue.wait_cond().wait_for(lock, std::chrono::milliseconds(10), [this] {
      return stop.load() || queue.has_work_for_consumer();
    });
  }
}
