// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#pragma once

#include "common/ceph_mutex.h"

#ifdef CEPH_DEBUG_MUTEX
#include <thread> // for std::this_thread::get_id()
#endif
#ifdef CEPH_LOCKSTAT
#include "lockstat.h"
#endif

#include <string>

#include <boost/intrusive/list.hpp>

namespace ceph {
/**
 * fair_mutex
 *
 * A FIFO mutex: threads acquire the lock in the order they called lock(),
 * avoiding the starvation that std::mutex can exhibit under sustained
 * contention.
 *
 * Design
 * ------
 * A boolean records whether the lock is held.  Threads that cannot acquire
 * immediately enqueue a stack-allocated waiter on a boost::intrusive::list
 * and block on its condition variable until they reach the front of the
 * queue and the lock is free.  unlock() clears the held flag and, if the
 * queue is non-empty, wakes the head waiter with notify_one(), so there is
 * no thundering herd.
 *
 * A thread must queue not only when the lock is held, but also when the
 * lock is free but waiters are already present: after unlock() the lock is
 * reserved for the head of the queue until that waiter takes it.
 *
 * When to use
 * -----------
 * - A heavily contended lock where many threads block and fairness
 *   matters, e.g. a daemon-wide lock that serializes unrelated work.
 * - Hot paths where std::mutex would let one thread monopolize the lock
 *   while others spin or sleep in the kernel wait queue indefinitely.
 *
 * When not to use
 * ---------------
 * - Low or brief contention: std::mutex is simpler and usually faster
 *   when few threads compete or hold times are short.
 * - Recursive locking: fair_mutex is not recursive; a second lock() from
 *   the same thread deadlocks.
 * - try_lock() fairness: a successful try_lock() takes the lock without
 *   joining the FIFO queue, so it can jump ahead of waiters.
 *
 * Satisfies the BasicLockable requirements; intended as a drop-in
 * replacement for ceph::mutex where FIFO ordering is required.
 */
 class fair_mutex
 #ifdef CEPH_LOCKSTAT
   : public lockstat_detail::LockStat
 #endif
 {
 public:
   fair_mutex(const std::string& name) :
 #ifdef CEPH_LOCKSTAT
     lockstat_detail::LockStat(ceph::mutex::LockType, LOCKSTAT("fair_" + name)),
 #endif
   mutex{ceph::make_mutex(name)}
  {}

  ~fair_mutex() = default;
  fair_mutex(const fair_mutex&) = delete;
  fair_mutex& operator=(const fair_mutex&) = delete;

  void
  lock()
  {
#ifdef CEPH_LOCKSTAT
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_detail::lockstat_clock::now()
            : lockstat_detail::lockstat_clock::zero();
#endif
    std::unique_lock lock(mutex);
    if (locked || !waiters.empty()) {
      waiter w;
      waiters.push_back(w);
      w.cv.wait(lock, [&] { return &waiters.front() == &w && !locked; });
      waiters.erase(waiters.iterator_to(w));
    }
    locked = true;
    _set_locked_by();
#ifdef CEPH_LOCKSTAT
    if (unlikely(wait_start_clock != lockstat_detail::lockstat_clock::zero() &&
                 get_traits())) {
      m_hold_start = lockstat_detail::lockstat_clock::now();
      m_hold_mode = lockstat_detail::LockMode::WRITE;
      record_wait_time(m_hold_start - wait_start_clock, m_hold_mode);
    }
#endif
  }

  bool
  try_lock()
  {
#ifdef CEPH_LOCKSTAT
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_detail::lockstat_clock::now()
            : lockstat_detail::lockstat_clock::zero();
#endif
    std::lock_guard lock(mutex);
    if (locked || !waiters.empty()) {
      return false;
    }
#ifdef CEPH_LOCKSTAT
    if (unlikely(wait_start_clock != lockstat_detail::lockstat_clock::zero() &&
                 get_traits())) {
      m_hold_start = lockstat_detail::lockstat_clock::now();
      m_hold_mode = lockstat_detail::LockMode::TRY_WRITE;
      record_wait_time(m_hold_start - wait_start_clock, m_hold_mode);
    }
#endif
    locked = true;
    _set_locked_by();
    return true;
  }

  void
  unlock()
  {
#ifdef CEPH_LOCKSTAT
    const auto hold_start = m_hold_start;
    if (unlikely(hold_start != lockstat_detail::lockstat_clock::zero())) {
      const auto hold_time =
          lockstat_detail::lockstat_clock::now() - hold_start;
      const auto hold_mode = m_hold_mode;
      m_hold_start = lockstat_detail::lockstat_clock::zero();
      if (get_traits()) {
        record_hold_time(hold_time, hold_mode);
      }
    }
#endif
    std::lock_guard lock(mutex);
    locked = false;
    _reset_locked_by();
    if (!waiters.empty()) {
      waiters.front().cv.notify_one();
    }
  }

  bool
  is_locked() const
  {
    return locked;
  }

#ifdef CEPH_DEBUG_MUTEX
  bool
  is_locked_by_me() const
  {
    return is_locked() && locked_by == std::this_thread::get_id();
  }

private:
  void
  _set_locked_by()
  {
    locked_by = std::this_thread::get_id();
  }

  void
  _reset_locked_by()
  {
    locked_by = {};
  }
#else

private:
  void
  _set_locked_by()
  {}

  void
  _reset_locked_by()
  {}
#endif

private:
  struct waiter : boost::intrusive::list_base_hook<> {
    ceph::condition_variable cv;
  };

  using waiter_list =
      boost::intrusive::list<waiter, boost::intrusive::constant_time_size<false>>;

  bool locked = false;
  waiter_list waiters;
  ceph::mutex mutex;
#ifdef CEPH_DEBUG_MUTEX
  std::thread::id locked_by = {};
#endif
#ifdef CEPH_LOCKSTAT
  lockstat_detail::lockstat_clock::time_point m_hold_start{
      lockstat_detail::lockstat_clock::zero()};
  lockstat_detail::LockMode m_hold_mode{
      lockstat_detail::LockMode::WRITE};
#endif
};
} // namespace ceph
