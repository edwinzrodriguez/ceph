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

#pragma once

#include <optional>

#include "OpWorkItem.h"

class Message;

/// Assign a dispatch lane for an inbound message at enqueue time.
/// Every type that can reach MDSRank dispatch must have an explicit case;
/// there is intentionally no default branch.
DispatchLane classify_inbound_message(const Message& m);

/**
 * Dense work-class ids for reactor execute-time breakdown.
 *
 * Covers Control/Maintenance inbound types plus synthetic WorkKinds.
 * Client-lane inbound messages are intentionally omitted (lane averages
 * already cover that path; per-type Client cardinality is high).
 *
 * Order must stay in sync with dispatch_work_class_info() and the
 * l_mds_dispatch_execute_latency_wc_* counter block in MDSRank.h.
 */
enum class DispatchWorkClass : uint8_t {
  MonMap = 0,
  MdsMap,
  OsdMap,
  RemoveSnaps,
  Command,
  MonCommand,
  ClientSession,
  ClientReconnect,
  ClientReclaim,
  QuiesceDbListing,
  QuiesceDbAck,
  Resolve,
  ResolveAck,
  CacheRejoin,
  Heartbeat,
  TableRequest,
  Scrub,
  ScrubStats,
  CacheExpire,
  IOCompletion,
  AdvanceQueues,
  TrimQuantum,
  LogTrim,
  Callable,
  Count
};

struct DispatchWorkClassInfo {
  const char* counter_name;
  const char* description;
  const char* nick;
};

const DispatchWorkClassInfo& dispatch_work_class_info(DispatchWorkClass wc);

/// Work class for per-type execute latency, if one is tracked for this item.
std::optional<DispatchWorkClass> classify_dispatch_work_class(
    const OpWorkItem& item);
