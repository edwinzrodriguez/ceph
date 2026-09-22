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

#include "gtest/gtest.h"
#include "include/ceph_fs.h"
#include "mds/dispatch/classify.h"
#include "messages/MGenericMessage.h"
#include "msg/Message.h"

TEST(MDSDispatchClassify, ControlMessages)
{
  const std::vector<int> types = {
      CEPH_MSG_MDS_MAP,
      CEPH_MSG_OSD_MAP,
      MSG_MDS_TABLE_REQUEST,
      MSG_MDS_RESOLVE,
  };

  for (int type : types) {
    auto m = ceph::make_ref<MGenericMessage>(type);
    EXPECT_EQ(classify_inbound_message(*m), DispatchLane::Control)
        << "type 0x" << std::hex << type;
  }
}

TEST(MDSDispatchClassify, ClientMessages)
{
  const std::vector<int> types = {
      CEPH_MSG_CLIENT_REQUEST, CEPH_MSG_CLIENT_CAPS, MSG_MDS_PEER_REQUEST,
      MSG_MDS_DISCOVER,        MSG_MDS_EXPORTDIR,
  };

  for (int type : types) {
    auto m = ceph::make_ref<MGenericMessage>(type);
    EXPECT_EQ(classify_inbound_message(*m), DispatchLane::Client)
        << "type 0x" << std::hex << type;
  }
}

TEST(MDSDispatchClassify, MaintenanceMessages)
{
  const std::vector<int> types = {
      MSG_MDS_SCRUB,
      MSG_MDS_SCRUB_STATS,
      MSG_MDS_CACHEEXPIRE,
  };

  for (int type : types) {
    auto m = ceph::make_ref<MGenericMessage>(type);
    EXPECT_EQ(classify_inbound_message(*m), DispatchLane::Maintenance)
        << "type 0x" << std::hex << type;
  }
}

TEST(MDSDispatchClassify, ClientMetricsIsClient)
{
  auto m = ceph::make_ref<MGenericMessage>(CEPH_MSG_CLIENT_METRICS);
  EXPECT_EQ(classify_inbound_message(*m), DispatchLane::Client);
}

TEST(MDSDispatchClassify, WorkClassControlAndMaintenance)
{
  auto mds_map = ceph::make_ref<MGenericMessage>(CEPH_MSG_MDS_MAP);
  OpWorkItem* control =
      OpWorkItem::create_inbound(mds_map, DispatchLane::Control);
  EXPECT_EQ(classify_dispatch_work_class(*control), DispatchWorkClass::MdsMap);
  control->destroy();

  auto scrub = ceph::make_ref<MGenericMessage>(MSG_MDS_SCRUB);
  OpWorkItem* maint =
      OpWorkItem::create_inbound(scrub, DispatchLane::Maintenance);
  EXPECT_EQ(classify_dispatch_work_class(*maint), DispatchWorkClass::Scrub);
  maint->destroy();

  auto caps = ceph::make_ref<MGenericMessage>(CEPH_MSG_CLIENT_CAPS);
  OpWorkItem* client = OpWorkItem::create_inbound(caps, DispatchLane::Client);
  EXPECT_FALSE(classify_dispatch_work_class(*client).has_value());
  client->destroy();
}

TEST(MDSDispatchClassify, WorkClassSynthetic)
{
  OpWorkItem* io = OpWorkItem::create_io(nullptr, 0);
  EXPECT_EQ(classify_dispatch_work_class(*io), DispatchWorkClass::IOCompletion);
  io->destroy();

  OpWorkItem* advance = OpWorkItem::create_advance();
  EXPECT_EQ(
      classify_dispatch_work_class(*advance), DispatchWorkClass::AdvanceQueues);
  advance->destroy();

  OpWorkItem* trim = OpWorkItem::create_trim();
  EXPECT_EQ(classify_dispatch_work_class(*trim), DispatchWorkClass::TrimQuantum);
  trim->destroy();

  OpWorkItem* log_trim = OpWorkItem::create_log_trim();
  EXPECT_EQ(classify_dispatch_work_class(*log_trim), DispatchWorkClass::LogTrim);
  log_trim->destroy();

  OpWorkItem* callable = OpWorkItem::create_callable(DispatchLane::Control, [] {
  });
  EXPECT_EQ(
      classify_dispatch_work_class(*callable), DispatchWorkClass::Callable);
  callable->destroy();
}

TEST(MDSDispatchClassify, UnclassifiedAborts)
{
  auto m = ceph::make_ref<MGenericMessage>(MSG_NOP);
  ASSERT_DEATH(classify_inbound_message(*m), "unclassified message type");
}
