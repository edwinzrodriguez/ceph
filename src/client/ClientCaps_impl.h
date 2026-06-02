// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CLIENT_CAPS_IMPL_H
#define CEPH_CLIENT_CAPS_IMPL_H

#include "ClientCaps.h"
#include "Inode.h"

template<typename Func>
void ClientCaps::process_delayed_caps(ceph::coarse_mono_time now, bool mount_aborted, Func&& func)
{
  std::vector<Inode*> to_process;
  
  {
    std::unique_lock lock(caps_lock);
    xlist<Inode*>::iterator p = delayed_list.begin();
    while (!p.end()) {
      Inode *in = *p;
      ++p;
      if (!mount_aborted && !client->is_unmounting() &&
	  in->hold_caps_until > now)
        break;
      delayed_list.pop_front();
      to_process.push_back(in);
    }
  }
  
  // Process inodes without holding caps_lock to avoid deadlock
  for (Inode *in : to_process) {
    func(in);
  }
}

#endif // CEPH_CLIENT_CAPS_IMPL_H
