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
 * OpWorkItem.h
 *
 * Shared vocabulary for the MDS dispatch subsystem (classic/reactor).
 *
 * DispatchLane and WorkKind classify work posted to the dispatch engine.
 * ClassicDispatchEngine does not queue work yet; these types are
 * defined here so ReactorDispatchEngine can use a single envelope
 * without introducing a parallel MDS op hierarchy.
 *
 * Usage (future reactor mode):
 *   - Producers enqueue OpWorkItem instances into per-lane dequeues.
 *   - lane is set at construction and cached on the item for metrics/requeue;
 *     dequeue priority is determined by which queue holds the item.
 *   - Server, MDCache, and MDRequest code below the drain point stay unchanged.
 */

#pragma once

#include <cstdint>

/// Priority lane for reactor-mode scheduling (high -> low).
enum class DispatchLane : uint8_t {
  Control = 0,
  IOComplete = 1,
  Client = 2,
  Maintenance = 3,

  // Keep this last
  Count
};

/// Kind of unit work handed to the dispatch engine.
enum class WorkKind : uint8_t {
  InboundMessage,
  IOCompletion,
  TrimQuantum,
  Callable,
};
