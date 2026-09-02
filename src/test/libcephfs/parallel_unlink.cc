// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression tests for parallel unlinks/creates from a single client under
 * one parent directory.  SWBUILD-style workloads hit MDS stalls when the
 * client holds directory caps (Fs) on the parent while many creates/unlinks
 * run concurrently: wrlocking parent filelock after quiescelock triggers an
 * Fs revoke the client cannot process while blocked on the create/unlink
 * reply.  Also covers mds_dirstat_min_interval stop-path scatter nudges.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "include/ceph_fs.h"
#include "include/cephfs/libcephfs.h"
#include "include/compat.h"

namespace {

constexpr int k_parallel_unlinks = 64;
constexpr int k_stress_waves = 4;
constexpr int k_create_unlink_workers = 4;
constexpr int k_create_unlink_iterations = 32;
constexpr int k_scatter_replace_workers = 8;
constexpr int k_scatter_replace_iterations = 48;
constexpr int k_scatter_create_workers = 4;
constexpr int k_scatter_create_iterations = 24;
constexpr int k_create_storm_workers = 12;
constexpr int k_create_storm_iterations = 64;
constexpr auto k_unlink_timeout = std::chrono::seconds(120);
constexpr auto k_create_unlink_timeout = std::chrono::seconds(120);
constexpr auto k_scatter_timeout = std::chrono::seconds(60);
constexpr auto k_create_storm_timeout = std::chrono::seconds(90);

using WorkerFinish = std::function<void()>;

static void
worker_finish(
    std::atomic<int>* done,
    int expected,
    std::mutex* cv_lock,
    std::condition_variable* cv)
{
  if (done->fetch_add(1) + 1 == expected) {
    std::lock_guard g(*cv_lock);
    cv->notify_one();
  }
}

struct Mount {
  ceph_mount_info* cmount = nullptr;

  ~Mount()
  {
    if (cmount) {
      ceph_shutdown(cmount);
    }
  }
};

static ceph_mount_info*
mount_client()
{
  ceph_mount_info* cmount = nullptr;
  EXPECT_EQ(0, ceph_create(&cmount, nullptr));
  if (!cmount) {
    return nullptr;
  }
  EXPECT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  EXPECT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  EXPECT_EQ(0, ceph_mount(cmount, "/"));
  return cmount;
}

static std::string
test_dir(int mypid)
{
  char buf[128];
  snprintf(buf, sizeof(buf), "parallel_unlink_%d", mypid);
  return buf;
}

static void
create_files(
    ceph_mount_info* cmount,
    const std::string& dir,
    int count,
    bool mkdir_dir = true)
{
  if (mkdir_dir) {
    ASSERT_EQ(0, ceph_mkdir(cmount, dir.c_str(), 0755));
  }
  for (int i = 0; i < count; ++i) {
    char path[256];
    snprintf(path, sizeof(path), "%s/file_%d", dir.c_str(), i);
    int fd = ceph_open(cmount, path, O_CREAT | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, ceph_close(cmount, fd));
  }
}

static void
warm_parent_dir_caps(ceph_mount_info* cmount, const std::string& dir)
{
  struct ceph_dir_result* dirp = nullptr;
  ASSERT_EQ(0, ceph_opendir(cmount, dir.c_str(), &dirp));

  struct dirent de;
  struct ceph_statx stx;
  while (true) {
    int r =
        ceph_readdirplus_r(cmount, dirp, &de, &stx, CEPH_STATX_INO, 0, nullptr);
    if (r == 0) {
      break;
    }
    ASSERT_GT(r, 0);
    if (strcmp(de.d_name, ".") == 0 || strcmp(de.d_name, "..") == 0) {
      continue;
    }
  }
  ASSERT_EQ(0, ceph_closedir(cmount, dirp));

  int caps = ceph_debug_get_file_caps(cmount, dir.c_str());
  ASSERT_GT(caps, 0);
  ASSERT_NE(
      0, caps & (CEPH_CAP_AUTH_SHARED | CEPH_CAP_LINK_SHARED |
                 CEPH_CAP_XATTR_SHARED));
}

static void
parallel_unlink_files(ceph_mount_info* cmount, const std::string& dir, int count)
{
  std::vector<std::thread> threads;
  threads.reserve(count);
  std::vector<int> results(count, -999);
  std::mutex result_lock;

  std::atomic<int> done{0};
  std::mutex cv_lock;
  std::condition_variable cv;

  for (int i = 0; i < count; ++i) {
    threads.emplace_back([&, i]() {
      char path[256];
      snprintf(path, sizeof(path), "%s/file_%d", dir.c_str(), i);
      int r = ceph_unlink(cmount, path);
      {
        std::lock_guard g(result_lock);
        results[i] = r;
      }
      if (done.fetch_add(1) + 1 == count) {
        std::lock_guard g(cv_lock);
        cv.notify_one();
      }
    });
  }

  {
    std::unique_lock lk(cv_lock);
    ASSERT_TRUE(cv.wait_for(
        lk, k_unlink_timeout, [&done, count]() { return done.load() == count; }))
        << "parallel unlink timed out after " << k_unlink_timeout.count()
        << "s";
  }

  for (auto& t : threads) {
    t.join();
  }

  for (int i = 0; i < count; ++i) {
    ASSERT_EQ(0, results[i]) << "unlink file_" << i;
  }
}

static void
assert_dir_empty(ceph_mount_info* cmount, const std::string& dir)
{
  struct ceph_dir_result* dirp = nullptr;
  ASSERT_EQ(0, ceph_opendir(cmount, dir.c_str(), &dirp));

  struct dirent de;
  struct ceph_statx stx;
  while (true) {
    int r =
        ceph_readdirplus_r(cmount, dirp, &de, &stx, CEPH_STATX_INO, 0, nullptr);
    if (r == 0) {
      break;
    }
    ASSERT_GT(r, 0);
    if (strcmp(de.d_name, ".") == 0 || strcmp(de.d_name, "..") == 0) {
      continue;
    }
    FAIL() << "directory not empty: " << dir << "/" << de.d_name;
  }
  ASSERT_EQ(0, ceph_closedir(cmount, dirp));
}

} // namespace

TEST(LibCephFS, ParallelUnlinkSameClient)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid);

  create_files(mount.cmount, dir, k_parallel_unlinks);
  parallel_unlink_files(mount.cmount, dir, k_parallel_unlinks);
  assert_dir_empty(mount.cmount, dir);
  ASSERT_EQ(0, ceph_rmdir(mount.cmount, dir.c_str()));
}

TEST(LibCephFS, ParallelUnlinkWithParentDirCaps)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid) + "_caps";

  create_files(mount.cmount, dir, k_parallel_unlinks);
  warm_parent_dir_caps(mount.cmount, dir);

  int parent_fd =
      ceph_open(mount.cmount, dir.c_str(), O_DIRECTORY | O_RDONLY, 0);
  ASSERT_GE(parent_fd, 0);

  parallel_unlink_files(mount.cmount, dir, k_parallel_unlinks);

  ASSERT_EQ(0, ceph_close(mount.cmount, parent_fd));
  assert_dir_empty(mount.cmount, dir);
  ASSERT_EQ(0, ceph_rmdir(mount.cmount, dir.c_str()));
}

TEST(LibCephFS, ParallelUnlinkStress)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid) + "_stress";
  const int files_per_wave = k_parallel_unlinks;

  ASSERT_EQ(0, ceph_mkdir(mount.cmount, dir.c_str(), 0755));
  warm_parent_dir_caps(mount.cmount, dir);

  int parent_fd =
      ceph_open(mount.cmount, dir.c_str(), O_DIRECTORY | O_RDONLY, 0);
  ASSERT_GE(parent_fd, 0);

  for (int wave = 0; wave < k_stress_waves; ++wave) {
    create_files(mount.cmount, dir, files_per_wave, false);
    warm_parent_dir_caps(mount.cmount, dir);
    parallel_unlink_files(mount.cmount, dir, files_per_wave);
    assert_dir_empty(mount.cmount, dir);
  }

  ASSERT_EQ(0, ceph_close(mount.cmount, parent_fd));
  ASSERT_EQ(0, ceph_rmdir(mount.cmount, dir.c_str()));
}

static void
swbuild_replace_loop(
    ceph_mount_info* cmount,
    const std::string& dir,
    int worker_id,
    int iterations,
    std::atomic<bool>* stop,
    std::atomic<int>* errors,
    WorkerFinish finish)
{
  for (int i = 0; i < iterations && !stop->load(); ++i) {
    char path[256];
    snprintf(path, sizeof(path), "%s/replace_%d_%d", dir.c_str(), worker_id, i);
    int fd = ceph_open(cmount, path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
    if (ceph_close(cmount, fd) < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }

    if (ceph_unlink(cmount, path) < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }

    // SWBUILD-style: recreate the same name immediately after unlink.
    fd = ceph_open(cmount, path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
    if (ceph_close(cmount, fd) < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
  }
  finish();
}

static void
concurrent_create_loop(
    ceph_mount_info* cmount,
    const std::string& dir,
    int worker_id,
    int iterations,
    std::atomic<bool>* stop,
    std::atomic<int>* errors,
    WorkerFinish finish)
{
  for (int i = 0; i < iterations && !stop->load(); ++i) {
    char path[256];
    snprintf(path, sizeof(path), "%s/create_%d_%d", dir.c_str(), worker_id, i);
    int fd = ceph_open(cmount, path, O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
    if (ceph_close(cmount, fd) < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
  }
  finish();
}

static void
concurrent_unlink_loop(
    ceph_mount_info* cmount,
    const std::string& dir,
    int count,
    std::atomic<bool>* stop,
    std::atomic<int>* errors,
    WorkerFinish finish)
{
  for (int i = 0; i < count && !stop->load(); ++i) {
    char path[256];
    snprintf(path, sizeof(path), "%s/file_%d", dir.c_str(), i);
    if (ceph_unlink(cmount, path) < 0) {
      errors->fetch_add(1);
      stop->store(true);
      finish();
      return;
    }
  }
  finish();
}

static void
run_worker_pool(
    std::chrono::seconds timeout,
    int expected_workers,
    const std::function<void(
        WorkerFinish finish,
        std::atomic<bool>* stop,
        std::atomic<int>* errors,
        std::vector<std::thread>* threads)>& spawn)
{
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<int> done{0};
  std::mutex cv_lock;
  std::condition_variable cv;
  std::vector<std::thread> threads;
  threads.reserve(expected_workers);

  WorkerFinish finish = [&]() {
    worker_finish(&done, expected_workers, &cv_lock, &cv);
  };

  spawn(finish, &stop, &errors, &threads);

  {
    std::unique_lock lk(cv_lock);
    ASSERT_TRUE(cv.wait_for(
        lk, timeout,
        [&done, expected_workers]() { return done.load() == expected_workers; }))
        << "worker pool timed out after " << timeout.count() << "s (completed "
        << done.load() << "/" << expected_workers << ")";
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(0, errors.load()) << "worker failed";
}

static void
cleanup_bucket_dir(ceph_mount_info* cmount, const std::string& dir)
{
  struct ceph_dir_result* dirp = nullptr;
  if (ceph_opendir(cmount, dir.c_str(), &dirp) != 0) {
    return;
  }
  struct dirent de;
  while (ceph_readdir_r(cmount, dirp, &de) > 0) {
    if (strcmp(de.d_name, ".") == 0 || strcmp(de.d_name, "..") == 0) {
      continue;
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir.c_str(), de.d_name);
    ceph_unlink(cmount, path);
  }
  ceph_closedir(cmount, dirp);
  ceph_rmdir(cmount, dir.c_str());
}

TEST(LibCephFS, ParallelCreateUnlinkWithParentDirCaps)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid) + "_create_unlink";

  create_files(mount.cmount, dir, k_parallel_unlinks);
  warm_parent_dir_caps(mount.cmount, dir);

  // Keep parent open so Fs stays issued (matches SWBUILD / kernel clients).
  int parent_fd =
      ceph_open(mount.cmount, dir.c_str(), O_DIRECTORY | O_RDONLY, 0);
  ASSERT_GE(parent_fd, 0);

  const int expected_workers = k_create_unlink_workers * 2 + 1;
  run_worker_pool(
      k_create_unlink_timeout, expected_workers,
      [&](WorkerFinish finish, std::atomic<bool>* stop,
          std::atomic<int>* errors, std::vector<std::thread>* threads) {
        for (int w = 0; w < k_create_unlink_workers; ++w) {
          threads->emplace_back([cmount = mount.cmount, dir, w, finish, stop,
                                 errors]() {
            swbuild_replace_loop(
                cmount, dir, w, k_create_unlink_iterations, stop, errors,
                finish);
          });
        }
        for (int w = 0; w < k_create_unlink_workers; ++w) {
          threads->emplace_back([cmount = mount.cmount, dir, w, finish, stop,
                                 errors]() {
            concurrent_create_loop(
                cmount, dir, w, k_create_unlink_iterations, stop, errors,
                finish);
          });
        }
        threads->emplace_back([cmount = mount.cmount, dir, finish, stop,
                               errors]() {
          concurrent_unlink_loop(
              cmount, dir, k_parallel_unlinks, stop, errors, finish);
        });
      });

  ASSERT_EQ(0, ceph_close(mount.cmount, parent_fd));

  struct ceph_statx stx;
  ASSERT_EQ(
      ceph_statx(
          mount.cmount, dir.c_str(), &stx,
          CEPH_STATX_VERSION | CEPH_STATX_MTIME, AT_SYMLINK_NOFOLLOW),
      0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, 0u);

  cleanup_bucket_dir(mount.cmount, dir);
}

TEST(LibCephFS, LinkUnlinkDirstatIntervalScatter)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid) + "_scatter";

  ASSERT_EQ(0, ceph_mkdir(mount.cmount, dir.c_str(), 0755));
  warm_parent_dir_caps(mount.cmount, dir);

  int parent_fd =
      ceph_open(mount.cmount, dir.c_str(), O_DIRECTORY | O_RDONLY, 0);
  ASSERT_GE(parent_fd, 0);

  // Prime last_dirstat_prop on the bucket dir, then hammer it faster than
  // mds_dirstat_min_interval (default 1s) with link/unlink + creates while
  // Fs remains issued on the open parent.
  char seed_path[256];
  snprintf(seed_path, sizeof(seed_path), "%s/seed", dir.c_str());
  int seed_fd =
      ceph_open(mount.cmount, seed_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
  ASSERT_GE(seed_fd, 0);
  ASSERT_EQ(0, ceph_close(mount.cmount, seed_fd));
  ASSERT_EQ(0, ceph_unlink(mount.cmount, seed_path));

  const int expected_workers = k_scatter_replace_workers +
                               k_scatter_create_workers;
  run_worker_pool(
      k_scatter_timeout, expected_workers,
      [&](WorkerFinish finish, std::atomic<bool>* stop,
          std::atomic<int>* errors, std::vector<std::thread>* threads) {
        for (int w = 0; w < k_scatter_replace_workers; ++w) {
          threads->emplace_back([cmount = mount.cmount, dir, w, finish, stop,
                                 errors]() {
            swbuild_replace_loop(
                cmount, dir, w, k_scatter_replace_iterations, stop, errors,
                finish);
          });
        }
        for (int w = 0; w < k_scatter_create_workers; ++w) {
          threads->emplace_back([cmount = mount.cmount, dir, w, finish, stop,
                                 errors]() {
            concurrent_create_loop(
                cmount, dir, w, k_scatter_create_iterations, stop, errors,
                finish);
          });
        }
      });

  ASSERT_EQ(0, ceph_close(mount.cmount, parent_fd));

  struct ceph_statx stx;
  ASSERT_EQ(
      ceph_statx(
          mount.cmount, dir.c_str(), &stx,
          CEPH_STATX_VERSION | CEPH_STATX_MTIME, AT_SYMLINK_NOFOLLOW),
      0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, 0u);

  cleanup_bucket_dir(mount.cmount, dir);
}

// CREATE-only storm under a parent with Fs pinned.  Reproduces the
// bucket11 stall where openc wrlocked parent filelock after quiescelock
// and waited on an Fs revoke the client could not process.
TEST(LibCephFS, ConcurrentCreatesWithParentDirCaps)
{
  const int mypid = getpid();
  Mount mount;
  mount.cmount = mount_client();
  ASSERT_NE(mount.cmount, nullptr);
  const std::string dir = test_dir(mypid) + "_create_storm";

  ASSERT_EQ(0, ceph_mkdir(mount.cmount, dir.c_str(), 0755));
  warm_parent_dir_caps(mount.cmount, dir);

  int parent_fd =
      ceph_open(mount.cmount, dir.c_str(), O_DIRECTORY | O_RDONLY, 0);
  ASSERT_GE(parent_fd, 0);

  // Seed one create/unlink so last_dirstat_prop is recent, then flood creates.
  char seed_path[256];
  snprintf(seed_path, sizeof(seed_path), "%s/seed", dir.c_str());
  int seed_fd =
      ceph_open(mount.cmount, seed_path, O_CREAT | O_EXCL | O_WRONLY, 0644);
  ASSERT_GE(seed_fd, 0);
  ASSERT_EQ(0, ceph_close(mount.cmount, seed_fd));
  ASSERT_EQ(0, ceph_unlink(mount.cmount, seed_path));

  run_worker_pool(
      k_create_storm_timeout, k_create_storm_workers,
      [&](WorkerFinish finish, std::atomic<bool>* stop,
          std::atomic<int>* errors, std::vector<std::thread>* threads) {
        for (int w = 0; w < k_create_storm_workers; ++w) {
          threads->emplace_back([cmount = mount.cmount, dir, w, finish, stop,
                                 errors]() {
            concurrent_create_loop(
                cmount, dir, w, k_create_storm_iterations, stop, errors, finish);
          });
        }
      });

  ASSERT_EQ(0, ceph_close(mount.cmount, parent_fd));

  struct ceph_statx stx;
  ASSERT_EQ(
      ceph_statx(
          mount.cmount, dir.c_str(), &stx,
          CEPH_STATX_VERSION | CEPH_STATX_MTIME, AT_SYMLINK_NOFOLLOW),
      0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, 0u);

  cleanup_bucket_dir(mount.cmount, dir);
}
