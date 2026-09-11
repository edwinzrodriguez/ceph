// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression test for stale used_prealloc_ino journal metadata when
 * prepare_new_inode() retries after test_and_clear_taken_inos().
 *
 * Uses mds_inject_prealloc_taken_retry to force the retry path, then
 * respawns the MDS to verify journal replay does not abort.
 */

#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/compat.h"
#include "include/stat.h"

namespace {

static int
mds_command(ceph_mount_info* cmount, const char* json)
{
  const char* cmd[1] = {json};
  char* outbuf = nullptr;
  char* outs = nullptr;
  size_t outbuf_len = 0;
  size_t outs_len = 0;
  int ret = ceph_mds_command(
      cmount, "0", cmd, 1, nullptr, 0, &outbuf, &outbuf_len, &outs, &outs_len);
  if (outbuf) {
    ceph_buffer_free(outbuf);
  }
  if (outs) {
    ceph_buffer_free(outs);
  }
  return ret;
}

static int
mds_config_set(ceph_mount_info* cmount, const char* var, const char* val)
{
  char json[256];
  snprintf(
      json, sizeof(json),
      "{\"prefix\": \"config set\", \"var\": \"%s\", \"val\": [\"%s\"]}", var,
      val);
  return mds_command(cmount, json);
}

static int
mds_config_unset(ceph_mount_info* cmount, const char* var)
{
  char json[256];
  snprintf(
      json, sizeof(json), "{\"prefix\": \"config unset\", \"var\": \"%s\"}",
      var);
  return mds_command(cmount, json);
}

static ceph_mount_info*
mount_client()
{
  ceph_mount_info* cmount = nullptr;
  if (ceph_create(&cmount, nullptr) != 0) {
    return nullptr;
  }
  if (ceph_conf_read_file(cmount, nullptr) != 0) {
    ceph_release(cmount);
    return nullptr;
  }
  if (ceph_conf_parse_env(cmount, nullptr) != 0) {
    ceph_release(cmount);
    return nullptr;
  }
  if (ceph_mount(cmount, "/") != 0) {
    ceph_release(cmount);
    return nullptr;
  }
  return cmount;
}

static void
wait_for_mds_respawn()
{
  /* ReclaimResetAfterMDSFailover uses 60s; allow less for vstart clusters. */
  sleep(30);
}

} // namespace

TEST(LibCephFS, PreallocTakenRetryJournalReplay)
{
  ceph_mount_info* admin = mount_client();
  ASSERT_NE(admin, nullptr);

  ASSERT_EQ(0, mds_config_set(admin, "mds_inject_prealloc_taken_retry", "true"));

  ceph_mount_info* writer = mount_client();
  ASSERT_NE(writer, nullptr);

  const std::string dir = std::string("prealloc_replay_") +
                          std::to_string(getpid());
  const std::string file = dir + "/created_file";

  ASSERT_EQ(0, ceph_mkdir(writer, dir.c_str(), 0755));

  int fd = ceph_open(writer, file.c_str(), O_CREAT | O_WRONLY, 0644);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(5, ceph_write(writer, fd, "hello", 5, 0));
  ASSERT_EQ(0, ceph_fsync(writer, fd, 0));
  ASSERT_EQ(0, ceph_close(writer, fd));

  ceph_unmount(writer);
  ceph_release(writer);

  const char* respawn_cmd[1] = {"{\"prefix\": \"respawn\"}"};
  char* outbuf = nullptr;
  char* outs = nullptr;
  size_t outbuf_len = 0;
  size_t outs_len = 0;
  int ret = ceph_mds_command(
      admin, "0", respawn_cmd, 1, nullptr, 0, &outbuf, &outbuf_len, &outs,
      &outs_len);
  ASSERT_EQ(0, ret);
  if (outbuf) {
    ceph_buffer_free(outbuf);
  }
  if (outs) {
    ceph_buffer_free(outs);
  }

  wait_for_mds_respawn();

  ceph_mount_info* reader = mount_client();
  ASSERT_NE(reader, nullptr);

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(reader, file.c_str(), &stx, CEPH_STATX_SIZE, 0));
  ASSERT_EQ(5u, stx.stx_size);

  ceph_unmount(reader);
  ceph_release(reader);

  ASSERT_EQ(0, mds_config_unset(admin, "mds_inject_prealloc_taken_retry"));

  ceph_unmount(admin);
  ceph_release(admin);
}
