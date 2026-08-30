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

#include "MDSOpWorkQueue.h"

#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

MDSOpWorkQueue::LaneQueue::LaneQueue() :
  lock(ceph::make_mutex("MDSOpWorkQueue::lane"))
{}

MDSOpWorkQueue::MDSOpWorkQueue() :
  queue_lock(ceph::make_mutex("MDSOpWorkQueue::queue_lock"))
{}

MDSOpWorkQueue::~MDSOpWorkQueue() { flush_and_clear(); }

MDSOpWorkQueue::LaneList&
MDSOpWorkQueue::consumer_list(LaneQueue& lane)
{
  const uint8_t producer = lane.producer_bank.load(std::memory_order_acquire);
  return lane.lists[producer ^ 1];
}

void
MDSOpWorkQueue::clear_lane(LaneQueue& lane)
{
  for (auto& list : lane.lists) {
    while (!list.empty()) {
      OpWorkItem& item = list.front();
      list.pop_front();
      depth.fetch_sub(1, std::memory_order_relaxed);
      item.destroy();
    }
  }
  lane.producer_bank.store(0, std::memory_order_release);
}

void
MDSOpWorkQueue::enqueue(OpWorkItem* item, DispatchLane lane)
{
  ceph_assert(item != nullptr);
  ceph_assert(lane < DispatchLane::Count);
  item->lane = lane;

  LaneQueue& q = lanes[static_cast<size_t>(lane)];
  {
    std::lock_guard lock(q.lock);
    if (stopping.load()) {
      item->destroy();
      return;
    }
    const uint8_t producer = q.producer_bank.load(std::memory_order_relaxed);
    item->note_enqueued();
    q.lists[producer].push_back(*item);
    depth.fetch_add(1, std::memory_order_relaxed);
  }

  {
    std::lock_guard lock(queue_lock);
    queue_cond.notify_one();
  }
}

OpWorkItem*
MDSOpWorkQueue::dequeue_lane(LaneQueue& lane)
{
  LaneList& list = consumer_list(lane);
  if (list.empty()) {
    return nullptr;
  }
  OpWorkItem& item = list.front();
  list.pop_front();
  depth.fetch_sub(1, std::memory_order_relaxed);
  return &item;
}

void
MDSOpWorkQueue::maybe_swap_lane(LaneQueue& lane)
{
  if (!consumer_list(lane).empty()) {
    return;
  }

  std::lock_guard lock(lane.lock);
  const uint8_t producer = lane.producer_bank.load(std::memory_order_relaxed);
  if (!lane.lists[producer ^ 1].empty()) {
    return;
  }
  if (lane.lists[producer].empty()) {
    return;
  }
  lane.producer_bank.store(producer ^ 1, std::memory_order_release);
}

OpWorkItem*
MDSOpWorkQueue::dequeue()
{
  for (size_t li = 0; li < static_cast<size_t>(DispatchLane::Count); ++li) {
    LaneQueue& q = lanes[li];

    if (OpWorkItem* item = dequeue_lane(q)) {
      return item;
    }

    maybe_swap_lane(q);

    if (OpWorkItem* item = dequeue_lane(q)) {
      return item;
    }
  }
  return nullptr;
}

bool
MDSOpWorkQueue::has_work_for_consumer()
{
  for (auto& q : lanes) {
    if (!consumer_list(q).empty()) {
      return true;
    }
  }

  for (auto& q : lanes) {
    std::lock_guard lock(q.lock);
    const uint8_t producer = q.producer_bank.load(std::memory_order_relaxed);
    if (!q.lists[producer].empty()) {
      return true;
    }
  }
  return false;
}

size_t
MDSOpWorkQueue::count() const
{
  return depth.load(std::memory_order_relaxed);
}

void
MDSOpWorkQueue::wake()
{
  std::lock_guard lock(queue_lock);
  queue_cond.notify_one();
}

void
MDSOpWorkQueue::shutdown()
{
  stopping.store(true);
  wake();
}

void
MDSOpWorkQueue::flush_and_clear()
{
  for (auto& q : lanes) {
    std::lock_guard lock(q.lock);
    clear_lane(q);
  }
}
