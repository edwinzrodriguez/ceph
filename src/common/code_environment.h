// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_CODE_ENVIRONMENT_H
#define CEPH_COMMON_CODE_ENVIRONMENT_H

enum code_environment_t {
  CODE_ENVIRONMENT_UTILITY = 0,
  CODE_ENVIRONMENT_DAEMON = 1,
  CODE_ENVIRONMENT_LIBRARY = 2,
  CODE_ENVIRONMENT_UTILITY_NODOUT = 3,
};

#ifdef __cplusplus
#include <atomic>
#include <iosfwd>
#include <string>

extern std::atomic<code_environment_t> g_code_env;

inline code_environment_t get_code_env()
{
  return g_code_env.load(std::memory_order_relaxed);
}

inline void set_code_env(code_environment_t env)
{
  g_code_env.store(env, std::memory_order_relaxed);
}

extern "C" const char *code_environment_to_str(enum code_environment_t e);
std::ostream &operator<<(std::ostream &oss, const enum code_environment_t e);
extern "C" int get_process_name(char *buf, int len);
std::string get_process_name_cpp();

#else

const char *code_environment_to_str(const enum code_environment_t e);
extern int get_process_name(char *buf, int len);

#endif

#endif
