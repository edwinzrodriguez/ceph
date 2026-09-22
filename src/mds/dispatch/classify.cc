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

#include "classify.h"

#include <iterator>

#include "common/debug.h"

#include "include/ceph_assert.h"
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
  case CEPH_MSG_CLIENT_METRICS:
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

namespace {

const DispatchWorkClassInfo work_class_info[] = {
    {"dispatch_execute_latency_mon_map",
     "Reactor execute time (CEPH_MSG_MON_MAP, seconds)", "dxmm"},
    {"dispatch_execute_latency_mds_map",
     "Reactor execute time (CEPH_MSG_MDS_MAP, seconds)", "dxmd"},
    {"dispatch_execute_latency_osd_map",
     "Reactor execute time (CEPH_MSG_OSD_MAP, seconds)", "dxom"},
    {"dispatch_execute_latency_remove_snaps",
     "Reactor execute time (MSG_REMOVE_SNAPS, seconds)", "dxrs"},
    {"dispatch_execute_latency_command",
     "Reactor execute time (MSG_COMMAND, seconds)", "dxcm"},
    {"dispatch_execute_latency_mon_command",
     "Reactor execute time (MSG_MON_COMMAND, seconds)", "dxmc"},
    {"dispatch_execute_latency_client_session",
     "Reactor execute time (CEPH_MSG_CLIENT_SESSION, seconds)", "dxcs"},
    {"dispatch_execute_latency_client_reconnect",
     "Reactor execute time (CEPH_MSG_CLIENT_RECONNECT, seconds)", "dxcr"},
    {"dispatch_execute_latency_client_reclaim",
     "Reactor execute time (CEPH_MSG_CLIENT_RECLAIM, seconds)", "dxcl"},
    {"dispatch_execute_latency_quiesce_db_listing",
     "Reactor execute time (MSG_MDS_QUIESCE_DB_LISTING, seconds)", "dxql"},
    {"dispatch_execute_latency_quiesce_db_ack",
     "Reactor execute time (MSG_MDS_QUIESCE_DB_ACK, seconds)", "dxqa"},
    {"dispatch_execute_latency_resolve",
     "Reactor execute time (MSG_MDS_RESOLVE, seconds)", "dxre"},
    {"dispatch_execute_latency_resolve_ack",
     "Reactor execute time (MSG_MDS_RESOLVEACK, seconds)", "dxra"},
    {"dispatch_execute_latency_cache_rejoin",
     "Reactor execute time (MSG_MDS_CACHEREJOIN, seconds)", "dxrj"},
    {"dispatch_execute_latency_heartbeat",
     "Reactor execute time (MSG_MDS_HEARTBEAT, seconds)", "dxhb"},
    {"dispatch_execute_latency_table_request",
     "Reactor execute time (MSG_MDS_TABLE_REQUEST, seconds)", "dxtr"},
    {"dispatch_execute_latency_scrub",
     "Reactor execute time (MSG_MDS_SCRUB, seconds)", "dxsc"},
    {"dispatch_execute_latency_scrub_stats",
     "Reactor execute time (MSG_MDS_SCRUB_STATS, seconds)", "dxss"},
    {"dispatch_execute_latency_cache_expire",
     "Reactor execute time (MSG_MDS_CACHEEXPIRE, seconds)", "dxce"},
    {"dispatch_execute_latency_io_completion",
     "Reactor execute time (IOCompletion work, seconds)", "dxio"},
    {"dispatch_execute_latency_advance_queues",
     "Reactor execute time (AdvanceQueues work, seconds)", "dxaq"},
    {"dispatch_execute_latency_trim_quantum",
     "Reactor execute time (TrimQuantum work, seconds)", "dxtq"},
    {"dispatch_execute_latency_log_trim",
     "Reactor execute time (LogTrim work, seconds)", "dxlt"},
    {"dispatch_execute_latency_callable",
     "Reactor execute time (Callable work, seconds)", "dxcb"},
};

static_assert(
    std::size(work_class_info) == static_cast<size_t>(DispatchWorkClass::Count),
    "work_class_info must match DispatchWorkClass::Count");

std::optional<DispatchWorkClass>
classify_inbound_work_class(int type)
{
  switch (type) {
  case CEPH_MSG_MON_MAP:
    return DispatchWorkClass::MonMap;
  case CEPH_MSG_MDS_MAP:
    return DispatchWorkClass::MdsMap;
  case CEPH_MSG_OSD_MAP:
    return DispatchWorkClass::OsdMap;
  case MSG_REMOVE_SNAPS:
    return DispatchWorkClass::RemoveSnaps;
  case MSG_COMMAND:
    return DispatchWorkClass::Command;
  case MSG_MON_COMMAND:
    return DispatchWorkClass::MonCommand;
  case CEPH_MSG_CLIENT_SESSION:
    return DispatchWorkClass::ClientSession;
  case CEPH_MSG_CLIENT_RECONNECT:
    return DispatchWorkClass::ClientReconnect;
  case CEPH_MSG_CLIENT_RECLAIM:
    return DispatchWorkClass::ClientReclaim;
  case MSG_MDS_QUIESCE_DB_LISTING:
    return DispatchWorkClass::QuiesceDbListing;
  case MSG_MDS_QUIESCE_DB_ACK:
    return DispatchWorkClass::QuiesceDbAck;
  case MSG_MDS_RESOLVE:
    return DispatchWorkClass::Resolve;
  case MSG_MDS_RESOLVEACK:
    return DispatchWorkClass::ResolveAck;
  case MSG_MDS_CACHEREJOIN:
    return DispatchWorkClass::CacheRejoin;
  case MSG_MDS_HEARTBEAT:
    return DispatchWorkClass::Heartbeat;
  case MSG_MDS_TABLE_REQUEST:
    return DispatchWorkClass::TableRequest;
  case MSG_MDS_SCRUB:
    return DispatchWorkClass::Scrub;
  case MSG_MDS_SCRUB_STATS:
    return DispatchWorkClass::ScrubStats;
  case MSG_MDS_CACHEEXPIRE:
    return DispatchWorkClass::CacheExpire;
  default:
    // Client-lane (and any future untracked) inbound types.
    return std::nullopt;
  }
}

} // namespace

const DispatchWorkClassInfo&
dispatch_work_class_info(DispatchWorkClass wc)
{
  const auto idx = static_cast<size_t>(wc);
  ceph_assert(idx < static_cast<size_t>(DispatchWorkClass::Count));
  return work_class_info[idx];
}

std::optional<DispatchWorkClass>
classify_dispatch_work_class(const OpWorkItem& item)
{
  switch (item.kind) {
  case WorkKind::InboundMessage:
    ceph_assert(item.msg);
    return classify_inbound_work_class(item.msg->get_type());
  case WorkKind::IOCompletion:
    return DispatchWorkClass::IOCompletion;
  case WorkKind::AdvanceQueues:
    return DispatchWorkClass::AdvanceQueues;
  case WorkKind::TrimQuantum:
    return DispatchWorkClass::TrimQuantum;
  case WorkKind::LogTrim:
    return DispatchWorkClass::LogTrim;
  case WorkKind::Callable:
    return DispatchWorkClass::Callable;
  }
  ceph_abort_msg("unknown WorkKind");
}
