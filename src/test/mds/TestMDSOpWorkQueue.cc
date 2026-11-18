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
#include "mds/dispatch/MDSOpWorkQueue.h"
#include "mds/dispatch/OpWorkItem.h"
#include "messages/MGenericMessage.h"

static ref_t<Message>
make_message(int type)
{
  return ceph::make_ref<MGenericMessage>(type);
}

TEST(MDSOpWorkQueue, DequeueLanePriority)
{
  MDSOpWorkQueue queue;

  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_CLIENT_REQUEST), DispatchLane::Client),
      DispatchLane::Client);
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_MDS_MAP), DispatchLane::Control),
      DispatchLane::Control);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->lane, DispatchLane::Control);
  item->destroy();

  item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->lane, DispatchLane::Client);
  item->destroy();

  EXPECT_EQ(queue.dequeue(), nullptr);
}

TEST(MDSOpWorkQueue, FifoWithinLane)
{
  MDSOpWorkQueue queue;

  auto m1 = make_message(CEPH_MSG_CLIENT_REQUEST);
  auto m2 = make_message(CEPH_MSG_CLIENT_REPLY);
  queue.enqueue(
      OpWorkItem::create_inbound(m1, DispatchLane::Client),
      DispatchLane::Client);
  queue.enqueue(
      OpWorkItem::create_inbound(m2, DispatchLane::Client),
      DispatchLane::Client);

  OpWorkItem* first = queue.dequeue();
  OpWorkItem* second = queue.dequeue();
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->msg.get(), m1.get());
  EXPECT_EQ(second->msg.get(), m2.get());
  first->destroy();
  second->destroy();
}

TEST(MDSOpWorkQueue, IOCompletionLane)
{
  MDSOpWorkQueue queue;

  queue.enqueue(
      OpWorkItem::create_io(nullptr, -ENOENT), DispatchLane::IOComplete);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->kind, WorkKind::IOCompletion);
  EXPECT_EQ(item->lane, DispatchLane::IOComplete);
  EXPECT_EQ(item->rval, -ENOENT);
  item->destroy();
}

TEST(MDSOpWorkQueue, AdvanceQueuesLane)
{
  MDSOpWorkQueue queue;

  queue.enqueue(OpWorkItem::create_advance(), DispatchLane::Control);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->kind, WorkKind::AdvanceQueues);
  EXPECT_EQ(item->lane, DispatchLane::Control);
  item->destroy();
}

TEST(MDSOpWorkQueue, TrimQuantumLane)
{
  MDSOpWorkQueue queue;

  queue.enqueue(OpWorkItem::create_trim(), DispatchLane::Maintenance);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->kind, WorkKind::TrimQuantum);
  EXPECT_EQ(item->lane, DispatchLane::Maintenance);
  item->destroy();
}

TEST(MDSOpWorkQueue, PerLaneBankSwap)
{
  MDSOpWorkQueue queue;

  // Drain the initially empty consumer side for Control, then enqueue while
  // producers fill the other bank for that lane.
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_MDS_MAP), DispatchLane::Control),
      DispatchLane::Control);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->lane, DispatchLane::Control);
  item->destroy();

  // Consumer bank for Control is empty again; next enqueue lands on producer
  // bank and should become visible after maybe_swap on a later dequeue pass.
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_OSD_MAP), DispatchLane::Control),
      DispatchLane::Control);

  item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(item->lane, DispatchLane::Control);
  item->destroy();
}

TEST(MDSOpWorkQueue, ControlPreemptsClientOnProducerBank)
{
  MDSOpWorkQueue queue;

  // Drain client lane so the next client item lands only on the producer bank.
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_CLIENT_REQUEST), DispatchLane::Client),
      DispatchLane::Client);
  OpWorkItem* first_client = queue.dequeue();
  ASSERT_NE(first_client, nullptr);
  first_client->destroy();

  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_CLIENT_REPLY), DispatchLane::Client),
      DispatchLane::Client);
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_MDS_MAP), DispatchLane::Control),
      DispatchLane::Control);

  OpWorkItem* control = queue.dequeue();
  ASSERT_NE(control, nullptr);
  EXPECT_EQ(control->lane, DispatchLane::Control);
  control->destroy();

  OpWorkItem* second_client = queue.dequeue();
  ASSERT_NE(second_client, nullptr);
  EXPECT_EQ(second_client->lane, DispatchLane::Client);
  second_client->destroy();
}

TEST(MDSOpWorkQueue, ShutdownDropsNewWork)
{
  MDSOpWorkQueue queue;
  queue.shutdown();

  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_CLIENT_REQUEST), DispatchLane::Client),
      DispatchLane::Client);

  EXPECT_EQ(queue.dequeue(), nullptr);
  EXPECT_FALSE(queue.has_work_for_consumer());
  EXPECT_EQ(queue.count(), 0u);
}

TEST(MDSOpWorkQueue, DepthCounterTracksEnqueueDequeue)
{
  MDSOpWorkQueue queue;

  EXPECT_EQ(queue.count(), 0u);

  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_CLIENT_REQUEST), DispatchLane::Client),
      DispatchLane::Client);
  queue.enqueue(
      OpWorkItem::create_inbound(
          make_message(CEPH_MSG_MDS_MAP), DispatchLane::Control),
      DispatchLane::Control);
  EXPECT_EQ(queue.count(), 2u);

  OpWorkItem* item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(queue.count(), 1u);
  item->destroy();

  item = queue.dequeue();
  ASSERT_NE(item, nullptr);
  EXPECT_EQ(queue.count(), 0u);
  item->destroy();

  queue.enqueue(OpWorkItem::create_trim(), DispatchLane::Maintenance);
  EXPECT_EQ(queue.count(), 1u);
  queue.flush_and_clear();
  EXPECT_EQ(queue.count(), 0u);
}
