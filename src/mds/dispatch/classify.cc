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

#include "classify.h"

#include "common/debug.h"

#include "include/ceph_fs.h"
#include "msg/Message.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

DispatchLane
classify_inbound_message(const Message& m)
{
  switch (m.get_type()) {
  // daemon / rank control plane
  case CEPH_MSG_MON_MAP:
  case CEPH_MSG_MDS_MAP:
  case CEPH_MSG_OSD_MAP:
  case MSG_REMOVE_SNAPS:
  case MSG_COMMAND:
  case MSG_MON_COMMAND:
  case CEPH_MSG_CLIENT_SESSION:
  case CEPH_MSG_CLIENT_RECONNECT:
  case CEPH_MSG_CLIENT_RECLAIM:
  case MSG_MDS_QUIESCE_DB_LISTING:
  case MSG_MDS_QUIESCE_DB_ACK:
  case MSG_MDS_RESOLVE:
  case MSG_MDS_RESOLVEACK:
  case MSG_MDS_CACHEREJOIN:
  case MSG_MDS_HEARTBEAT:
  case MSG_MDS_TABLE_REQUEST:
    return DispatchLane::Control;

  // background / scrub
  case MSG_MDS_SCRUB:
  case MSG_MDS_SCRUB_STATS:
  case MSG_MDS_CACHEEXPIRE:
    return DispatchLane::Maintenance;

  // client and steady-state metadata path
  case CEPH_MSG_CLIENT_REQUEST:
  case CEPH_MSG_CLIENT_REPLY:
  case CEPH_MSG_CLIENT_CAPS:
  case CEPH_MSG_CLIENT_CAPRELEASE:
  case CEPH_MSG_CLIENT_LEASE:
  case MSG_MDS_PEER_REQUEST:
  case MSG_MDS_LOCK:
  case MSG_MDS_INODEFILECAPS:
  case MSG_MDS_DISCOVER:
  case MSG_MDS_DISCOVERREPLY:
  case MSG_MDS_DIRUPDATE:
  case MSG_MDS_DENTRYLINK:
  case MSG_MDS_DENTRYUNLINK:
  case MSG_MDS_FINDINO:
  case MSG_MDS_FINDINOREPLY:
  case MSG_MDS_OPENINO:
  case MSG_MDS_OPENINOREPLY:
  case MSG_MDS_SNAPUPDATE:
  case MSG_MDS_FRAGMENTNOTIFY:
  case MSG_MDS_FRAGMENTNOTIFYACK:
  case MSG_MDS_EXPORTDIRDISCOVER:
  case MSG_MDS_EXPORTDIRDISCOVERACK:
  case MSG_MDS_EXPORTDIRCANCEL:
  case MSG_MDS_EXPORTDIRPREP:
  case MSG_MDS_EXPORTDIRPREPACK:
  case MSG_MDS_EXPORTDIR:
  case MSG_MDS_EXPORTDIRACK:
  case MSG_MDS_EXPORTDIRNOTIFY:
  case MSG_MDS_EXPORTDIRNOTIFYACK:
  case MSG_MDS_EXPORTDIRFINISH:
  case MSG_MDS_EXPORTCAPS:
  case MSG_MDS_EXPORTCAPSACK:
  case MSG_MDS_GATHERCAPS:
    return DispatchLane::Client;
  }

  // No default: new message types should get an explicit case here.
  // MSG_* values are preprocessor constants (not an enum), so the compiler
  // cannot exhaustiveness-check this switch when Message.h grows.
  derr << __func__ << ": unclassified message type " << m.get_type() << " " << m
       << dendl;
  ceph_abort();
}
