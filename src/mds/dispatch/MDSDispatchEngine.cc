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
 * MDSDispatchEngine.cc
 *
 * Factory for rank-local dispatch engines. Reads mds_dispatch_engine once at
 * rank construction; selects ClassicDispatchEngine or ReactorDispatchEngine.
 */

#include "MDSDispatchEngine.h"

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"

#include "ClassicDispatchEngine.h"
#include "MDSDispatchContext.h"
#include "ReactorDispatchEngine.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

std::unique_ptr<MDSDispatchEngine>
MDSDispatchEngine::create(const MDSDispatchContext& ctx)
{
  ceph_assert(ctx.cct != nullptr);

  const std::string engine =
      ctx.cct->_conf.get_val<std::string>("mds_dispatch_engine");
  if (engine == "reactor") {
    return std::make_unique<ReactorDispatchEngine>(ctx);
  }
  if (engine != "classic") {
    derr << "unknown mds_dispatch_engine '" << engine << "'; using classic"
         << dendl;
  }

  return std::make_unique<ClassicDispatchEngine>(ctx);
}
