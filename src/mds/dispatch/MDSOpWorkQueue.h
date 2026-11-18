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
 * MDSOpWorkQueue.h
 *
 * Per-lane double-buffered work queues for ReactorDispatchEngine.
 *
 * Each DispatchLane owns a LaneQueue: two boost::intrusive::list banks plus a
 * lane-local lock. Producers append to lists[producer_bank] under the lane
 * lock. The op thread drains lists[producer_bank ^ 1] without the lane lock.
 * When a lane's consumer list is empty, the op thread takes the lane lock and,
 * if the producer list for that lane has work, flips producer_bank (^ 1).
 *
 * This avoids a global bank swap, which would let low-priority items in the
 * consumer bank run while higher-priority work sat on the producer side of
 * other lanes waiting for all lanes to drain.
 *
 * Usage:
 *   queue.enqueue(item, lane);   // any producer thread
 *   item = queue.dequeue();      // op thread; walks lanes by priority
 */

#pragma once

#include <array>
#include <atomic>

#include <boost/intrusive/list.hpp>

#include "common/ceph_mutex.h"

#include "OpWorkItem.h"

class MDSOpWorkQueue {
public:
  MDSOpWorkQueue();
  ~MDSOpWorkQueue();

  void enqueue(OpWorkItem* item, DispatchLane lane);

  /// Pop highest-priority item across lanes. Op thread only.
  OpWorkItem* dequeue();

  bool has_work_for_consumer();

  /// Items waiting in all lane banks (O(1); maintained by depth counter).
  size_t count() const;

  void wake();
  void shutdown();

  ceph::condition_variable&
  wait_cond()
  {
    return queue_cond;
  }

  ceph::mutex&
  wait_lock()
  {
    return queue_lock;
  }

  void flush_and_clear();

private:
  using LaneList = boost::intrusive::
      list<OpWorkItem, boost::intrusive::constant_time_size<false>>;

  static constexpr size_t NUM_BANKS = 2;

  struct LaneQueue {
    std::array<LaneList, NUM_BANKS> lists;
    /// Producer bank index; consumer bank is always producer_bank ^ 1.
    std::atomic<uint8_t> producer_bank{0};
    ceph::mutex lock;

    LaneQueue();
  };

  std::array<LaneQueue, static_cast<size_t>(DispatchLane::Count)> lanes;

  ceph::mutex queue_lock;
  ceph::condition_variable queue_cond;
  std::atomic<bool> stopping{false};
  std::atomic<size_t> depth{0};

  static LaneList& consumer_list(LaneQueue& lane);
  OpWorkItem* dequeue_lane(LaneQueue& lane);
  void maybe_swap_lane(LaneQueue& lane);
  void clear_lane(LaneQueue& lane);
};
