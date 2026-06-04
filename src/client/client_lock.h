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
 * Guard for Client::m_client_lock.  Tracks ownership in this guard object
 * instead of nesting std::unique_lock<ReentrantLock>, which desyncs the
 * reentrant recursion_count from outer RAII scopes.
 *
 * Nested guards on the same thread: only the guard that performed the
 * physical lock() performs unlock() on destruction.
 */
template <>
class unique_lock<Client> {
public:
  using mutex_type = Client;

  unique_lock() noexcept
    : _client(nullptr), _owns(false), _acquired_physical(false)
  {}

  explicit unique_lock(Client& c)
    : unique_lock(c, defer_lock)
  {
    lock();
  }

  unique_lock(Client& c, defer_lock_t) noexcept
    : _client(&c), _owns(false), _acquired_physical(false)
  {}

  unique_lock(Client& c, adopt_lock_t) noexcept
    : _client(&c), _owns(true), _acquired_physical(false)
  {}

  unique_lock(const unique_lock&) = delete;
  unique_lock& operator=(const unique_lock&) = delete;
  unique_lock(unique_lock&&) = delete;
  unique_lock& operator=(unique_lock&&) = delete;

  ~unique_lock()
  {
    finish();
  }

  void lock()
  {
    if (_owns) {
      return;
    }
    auto& rl = _client->get_client_lock();
    if (!rl.is_locked_by_me()) {
      rl.lock();
      _acquired_physical = true;
    }
    _owns = true;
  }

  bool try_lock()
  {
    if (_owns) {
      return true;
    }
    auto& rl = _client->get_client_lock();
    if (rl.is_locked_by_me()) {
      _owns = true;
      return true;
    }
    if (!rl.try_lock()) {
      return false;
    }
    _acquired_physical = true;
    _owns = true;
    return true;
  }

  void unlock()
  {
    if (!_owns) {
      return;
    }
    _owns = false;
    if (_acquired_physical) {
      _client->get_client_lock().unlock();
      _acquired_physical = false;
    }
  }

  // Disassociate this guard without releasing m_client_lock (e.g. cond wait
  // loops that adopt the lock separately).
  Client *release() noexcept
  {
    _owns = false;
    _acquired_physical = false;
    return _client;
  }

  bool owns_lock() const noexcept
  {
    return _owns;
  }

  Client *mutex() const noexcept
  {
    return _client;
  }

private:
  void finish()
  {
    unlock();
  }

  Client *_client;
  bool _owns;
  bool _acquired_physical;
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

  ~unique_unlock() noexcept(false) = default;

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

/**
 * If this thread holds m_client_lock (any reentrant depth), drop it for this
 * scope and restore the saved depth when the object is destroyed.
 */
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

inline std::unique_lock<ReentrantLock> adopt_reentrant(Client& c)
{
  return std::unique_lock<ReentrantLock>(detail::reentrant(c), std::adopt_lock);
}

inline void wait_on(Client& c, reentrant_condition_variable& cond,
                    std::unique_lock<Client>& lock)
{
  ceph_assert(lock.owns_lock());
  auto rl = adopt_reentrant(c);
  cond.wait(rl);
  rl.release();
}

template <class Predicate>
void wait_on(Client& c, reentrant_condition_variable& cond,
             std::unique_lock<Client>& lock, Predicate pred)
{
  ceph_assert(lock.owns_lock());
  auto rl = adopt_reentrant(c);
  cond.wait(rl, std::move(pred));
  rl.release();
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
  ceph_assert(lock.owns_lock());
  auto rl = adopt_reentrant(c);
  auto r = detail::mount(c).wait_for(rl, rel_time);
  rl.release();
  return r;
}

template <class Rep, class Period, class Predicate>
bool wait_mount_for(Client& c, std::unique_lock<Client>& lock,
                    const std::chrono::duration<Rep, Period>& rel_time,
                    Predicate pred)
{
  ceph_assert(lock.owns_lock());
  auto rl = adopt_reentrant(c);
  bool r = detail::mount(c).wait_for(rl, rel_time, std::move(pred));
  rl.release();
  return r;
}

} // namespace client_lock
} // namespace ceph

#endif // CEPH_CLIENT_CLIENT_LOCK_H