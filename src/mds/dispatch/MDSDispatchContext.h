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
 * MDSDispatchContext.h
 *
 * Non-owning handles wired once when an MDSRank constructs its dispatch engine.
 * Passed to MDSDispatchEngine::create() and retained by the engine implementation.
 *
 * Usage:
 *   MDSDispatchContext dispatch_ctx{daemon, this, &mds_lock, cct};
 *   dispatch_engine = MDSDispatchEngine::create(dispatch_ctx);
 *
 * The engine reads daemon/rank/lock/cct but does not own any of them; lifetime
 * is guaranteed by MDSRank (and its owning MDSDaemon).
 */

#pragma once

#include "common/ceph_context.h"
#include "common/fair_mutex.h"

class MDSDaemon;
class MDSRank;

struct MDSDispatchContext {
  MDSDaemon* daemon = nullptr; ///< outer daemon (stopping, core dispatch)
  MDSRank* rank = nullptr; ///< active rank (ms_dispatch, IO contexts)
  ceph::fair_mutex* mds_lock =
      nullptr; ///< global rank/daemon serialization lock
  CephContext* cct = nullptr; ///< config and logging
};
