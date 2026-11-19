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
 * Work envelope for the MDS dispatch subsystem. Each item lives on the heap and
 * is linked into per-lane boost::intrusive::list queues via list_hook.
 */

#pragma once

#include <cstdint>
#include <functional>

#include <boost/intrusive/list.hpp>

#include "common/ceph_time.h"
#include "msg/Message.h"

class MDSIOContextBase;

/// Priority lane for reactor-mode scheduling (high -> low).
enum class DispatchLane : uint8_t {
  Control = 0,
  IOComplete = 1,
  Client = 2,
  Maintenance = 3,

  Count
};

/// Kind of unit work handed to the dispatch engine.
enum class WorkKind : uint8_t {
  InboundMessage,
  IOCompletion,
  AdvanceQueues,
  TrimQuantum,
  Callable,
};

struct OpWorkItem
  : boost::intrusive::list_base_hook<
        boost::intrusive::link_mode<boost::intrusive::normal_link>> {
  WorkKind kind = WorkKind::InboundMessage;
  DispatchLane lane = DispatchLane::Client;

  ref_t<Message> msg;
  MDSIOContextBase* io_ctx = nullptr;
  int rval = 0;
  std::function<void()>* callable = nullptr;

  /// Set when the item enters MDSOpWorkQueue (reactor perf counters).
  ceph::coarse_mono_time enqueued_at{};

  static OpWorkItem* create_inbound(const ref_t<Message>& m, DispatchLane lane);
  static OpWorkItem* create_io(MDSIOContextBase* ctx, int r);
  static OpWorkItem* create_advance();
  static OpWorkItem* create_trim();
  static OpWorkItem* create_callable(DispatchLane lane, std::function<void()> fn);
  void note_enqueued();
  void destroy();
};
