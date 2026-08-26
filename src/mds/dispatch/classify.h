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

#pragma once

#include "OpWorkItem.h"

class Message;

/// Assign a dispatch lane for an inbound message at enqueue time.
/// Every type that can reach MDSRank dispatch must have an explicit case;
/// there is intentionally no default branch.
DispatchLane classify_inbound_message(const Message& m);
