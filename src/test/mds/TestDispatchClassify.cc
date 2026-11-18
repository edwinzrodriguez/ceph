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

TEST(MDSDispatchClassify, UnclassifiedAborts)
{
  auto m = ceph::make_ref<MGenericMessage>(MSG_NOP);
  ASSERT_DEATH(classify_inbound_message(*m), "unclassified message type");
}
