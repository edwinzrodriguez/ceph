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

#include "OpWorkItem.h"

OpWorkItem*
OpWorkItem::create_inbound(const ref_t<Message>& m, DispatchLane lane)
{
  auto* item = new OpWorkItem;
  item->kind = WorkKind::InboundMessage;
  item->lane = lane;
  item->msg = m;
  return item;
}

OpWorkItem*
OpWorkItem::create_io(MDSIOContextBase* ctx, int r)
{
  auto* item = new OpWorkItem;
  item->kind = WorkKind::IOCompletion;
  item->lane = DispatchLane::IOComplete;
  item->io_ctx = ctx;
  item->rval = r;
  return item;
}

OpWorkItem*
OpWorkItem::create_callable(DispatchLane lane, std::function<void()> fn)
{
  auto* item = new OpWorkItem;
  item->kind = WorkKind::Callable;
  item->lane = lane;
  item->callable = new std::function<void()>(std::move(fn));
  return item;
}

void
OpWorkItem::note_enqueued()
{
  enqueued_at = ceph::coarse_mono_clock::now();
}

void
OpWorkItem::destroy()
{
  delete callable;
  delete this;
}
