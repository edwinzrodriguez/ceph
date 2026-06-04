// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_CLIENT_LOCK_H
#define CEPH_CLIENT_CLIENT_LOCK_H

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "common/ceph_mutex.h"
#include "common/reentrant_lock.h"

// Included from Client.h after class Client is defined.

namespace std {

/**
 * Thin RAII wrapper around Client::m_client_lock (a ReentrantLock).
 * lock()/unlock() always delegate to ReentrantLock, which handles
 * same-thread reentrancy via owner/recursion_count.
 *
 * _owns means this guard object has taken one lock() and not yet unlock().
 * After a partner ceph::unique_unlock<Client> drops the lock, call lock()
 * again on the same guard; it re-acquires when is_locked_by_me() is false
 * even if _owns is still true.
 */
template <>
class unique_lock<Client> {
public:
  using mutex_type = Client;

  unique_lock() noexcept
    : _client(nullptr), _owns(false)
  {}

  explicit unique_lock(Client& c)
    : unique_lock(c, defer_lock)
  {
    lock();
  }

  unique_lock(Client& c, defer_lock_t) noexcept
    : _client(&c), _owns(false)
  {}

  unique_lock(Client& c, adopt_lock_t) noexcept
    : _client(&c), _owns(true)
  {}

  unique_lock(const unique_lock&) = delete;
  unique_lock& operator=(const unique_lock&) = delete;
  unique_lock(unique_lock&&) = delete;
  unique_lock& operator=(unique_lock&&) = delete;

  ~unique_lock()
  {
    unlock();
  }

  void lock()
  {
    auto& rl = _client->get_client_lock();
    if (_owns && rl.is_locked_by_me()) {
      return;
    }
    rl.lock();
    _owns = true;
  }

  bool try_lock()
  {
    auto& rl = _client->get_client_lock();
    if (_owns && rl.is_locked_by_me()) {
      return true;
    }
    if (!rl.try_lock()) {
      return false;
    }
    _owns = true;
    return true;
  }

  void unlock()
  {
    if (!_owns) {
      return;
    }
    _owns = false;
    auto& rl = _client->get_client_lock();
    if (rl.is_locked_by_me()) {
      rl.unlock();
    }
  }

  Client *release() noexcept
  {
    _owns = false;
    return _client;
  }

  bool owns_lock() const noexcept
  {
    if (!_owns || !_client) {
      return false;
    }
    return _client->get_client_lock().is_locked_by_me();
  }

  Client *mutex() const noexcept
  {
    return _client;
  }

private:
  Client *_client;
  bool _owns;
};

template <>
class scoped_lock<Client> {
public:
  explicit scoped_lock(Client& c)
    : _lock(c)
  {}

  scoped_lock(const scoped_lock&) = delete;
  scoped_lock& operator=(const scoped_lock&) = delete;

private:
  unique_lock<Client> _lock;
};

} // namespace std

namespace ceph {

template <>
class unique_unlock<Client> {
public:
  explicit unique_unlock(Client& c)
    : _inner(c.get_client_lock())
  {}

  unique_unlock(Client& c, std::defer_lock_t)
    : _inner(c.get_client_lock(), std::defer_lock)
  {}

  void release()
  {
    _inner.release();
  }

  bool released() const
  {
    return _inner.released();
  }

  void abandon() noexcept
  {
    _inner.abandon();
  }

  // Never try_lock-restore: a partner std::unique_lock<Client> may still
  // have _owns set.  It re-acquires via lock() when is_locked_by_me() is false.
  ~unique_unlock() noexcept(false)
  {
    _inner.abandon();
  }

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  unique_unlock<ReentrantLock> _inner;
};

namespace client_lock {

struct detail {
  static ReentrantLock& reentrant(Client& c)
  {
    return c.get_client_lock();
  }
  static reentrant_condition_variable& mount(Client& c)
  {
    return c.get_mount_cond();
  }
  static reentrant_condition_variable& sync(Client& c)
  {
    return c.get_sync_cond();
  }
};

class scoped_drop {
public:
  explicit scoped_drop(Client& c)
    : _unlock(c, std::defer_lock)
  {
    if (detail::reentrant(c).is_locked_by_me()) {
      _unlock.release();
    }
  }

  scoped_drop(const scoped_drop&) = delete;
  scoped_drop& operator=(const scoped_drop&) = delete;

private:
  unique_unlock<Client> _unlock;
};

inline void wait_on(Client& c, reentrant_condition_variable& cond,
                    std::unique_lock<Client>& lock)
{
  lock.lock();
  auto& rl = detail::reentrant(c);
  if (!rl.is_locked_by_me()) {
    rl.lock();
  }
  std::unique_lock<ReentrantLock> rl_lock(rl, std::adopt_lock);
  cond.wait(rl_lock);
  rl_lock.release();
}

template <class Predicate>
void wait_on(Client& c, reentrant_condition_variable& cond,
             std::unique_lock<Client>& lock, Predicate pred)
{
  lock.lock();
  auto& rl = detail::reentrant(c);
  if (!rl.is_locked_by_me()) {
    rl.lock();
  }
  std::unique_lock<ReentrantLock> rl_lock(rl, std::adopt_lock);
  cond.wait(rl_lock, std::move(pred));
  rl_lock.release();
}

inline void wait_mount(Client& c, std::unique_lock<Client>& lock)
{
  wait_on(c, detail::mount(c), lock);
}

template <class Predicate>
void wait_mount(Client& c, std::unique_lock<Client>& lock, Predicate pred)
{
  wait_on(c, detail::mount(c), lock, std::move(pred));
}

template <class Rep, class Period>
std::cv_status wait_mount_for(Client& c, std::unique_lock<Client>& lock,
                              const std::chrono::duration<Rep, Period>& rel_time)
{
  lock.lock();
  auto& rl = detail::reentrant(c);
  if (!rl.is_locked_by_me()) {
    rl.lock();
  }
  std::unique_lock<ReentrantLock> rl_lock(rl, std::adopt_lock);
  auto r = detail::mount(c).wait_for(rl_lock, rel_time);
  rl_lock.release();
  return r;
}

template <class Rep, class Period, class Predicate>
bool wait_mount_for(Client& c, std::unique_lock<Client>& lock,
                    const std::chrono::duration<Rep, Period>& rel_time,
                    Predicate pred)
{
  lock.lock();
  auto& rl = detail::reentrant(c);
  if (!rl.is_locked_by_me()) {
    rl.lock();
  }
  std::unique_lock<ReentrantLock> rl_lock(rl, std::adopt_lock);
  bool r = detail::mount(c).wait_for(rl_lock, rel_time, std::move(pred));
  rl_lock.release();
  return r;
}

/** Re-acquire after unique_unlock<Client> when no partner unique_lock exists. */
inline void reacquire_after_drop(Client& c)
{
  auto& rl = detail::reentrant(c);
  if (!rl.is_locked_by_me()) {
    rl.lock();
  }
}

} // namespace client_lock
} // namespace ceph

#endif // CEPH_CLIENT_CLIENT_LOCK_H