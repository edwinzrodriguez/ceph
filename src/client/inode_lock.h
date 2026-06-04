// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_INODE_LOCK_H
#define CEPH_CLIENT_INODE_LOCK_H

#include <memory>
#include <mutex>

#include "common/ceph_mutex.h"
#include "common/reentrant_lock.h"

// Included from Inode.h after struct Inode is defined.

namespace std {

/**
 * inode_lock must be taken before client_lock.  When client_lock is already
 * held, temporarily release it via ceph::unique_unlock, take inode_lock, then
 * re-take client_lock.  All state lives in this guard.
 */
template <>
class unique_lock<Inode> {
public:
  using mutex_type = Inode;

  unique_lock() noexcept
    : _in(nullptr), _owns(false), _inode_acquired(false)
  {}

  explicit unique_lock(const Inode& in)
    : unique_lock(in, defer_lock)
  {
    lock();
  }

  unique_lock(const Inode& in, defer_lock_t) noexcept
    : _in(&in), _owns(false), _inode_acquired(false)
  {}

  unique_lock(const Inode& in, adopt_lock_t) noexcept
    : _in(&in), _owns(true), _inode_acquired(false)
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
    auto& client = _in->get_client_lock();
    if (ceph_mutex_is_locked_by_me(client)) {
      _client_unlock = std::make_unique<ceph::unique_unlock<ceph::ReentrantLock>>(
        client, std::defer_lock);
      _client_unlock->release();
    }
    if (!_in->is_locked_by_me()) {
      _in->m_inode_lock.lock();
      _inode_acquired = true;
    }
    if (_client_unlock) {
      client.lock();
    }
    _owns = true;
  }

  bool try_lock()
  {
    if (_owns) {
      return true;
    }
    auto& client = _in->get_client_lock();
    const bool had_client = ceph_mutex_is_locked_by_me(client);
    if (had_client) {
      _client_unlock = std::make_unique<ceph::unique_unlock<ceph::ReentrantLock>>(
        client, std::defer_lock);
      _client_unlock->release();
    }
    if (_in->is_locked_by_me()) {
      // already held
    } else if (!_in->m_inode_lock.try_lock()) {
      if (_client_unlock) {
        _client_unlock.reset();
      }
      return false;
    } else {
      _inode_acquired = true;
    }
    if (_client_unlock) {
      client.lock();
    }
    _owns = true;
    return true;
  }

  void unlock()
  {
    if (!_owns) {
      return;
    }
    if (_inode_acquired) {
      _in->m_inode_lock.unlock();
      _inode_acquired = false;
    }
    if (_client_unlock) {
      // lock() re-acquired client_lock; abandon so we do not restore twice.
      _client_unlock->abandon();
      _client_unlock.reset();
    }
    _owns = false;
  }

  bool owns_lock() const noexcept
  {
    return _owns;
  }

  Inode *mutex() const noexcept
  {
    return const_cast<Inode *>(_in);
  }

private:
  void finish()
  {
    if (_inode_acquired) {
      // Another path may have dropped m_inode_lock via release_for_wait +
      // abandon (e.g. _create's make_request) while this guard still has
      // _inode_acquired set.
      if (_in->is_locked_by_me()) {
        _in->m_inode_lock.unlock();
      }
      _inode_acquired = false;
    }
    if (_client_unlock) {
      // lock() re-acquired client_lock; abandon so ~unique_unlock does not
      // restore the depth we already released (would strand recursion_count).
      _client_unlock->abandon();
      _client_unlock.reset();
    }
    _owns = false;
  }

  const Inode *_in;
  bool _owns;
  bool _inode_acquired;
  std::unique_ptr<ceph::unique_unlock<ceph::ReentrantLock>> _client_unlock;
};

template <>
class scoped_lock<Inode> {
public:
  explicit scoped_lock(const Inode& in)
    : _lock(in)
  {}

  scoped_lock(const scoped_lock&) = delete;
  scoped_lock& operator=(const scoped_lock&) = delete;

private:
  unique_lock<Inode> _lock;
};

} // namespace std

namespace ceph {

/**
 * Temporarily drop the inode lock (one reentrant nesting level) while the
 * current thread holds it, and restore it on destruction.  Does not change
 * client_lock; mirrors std::unique_lock<Inode>::unlock().
 */
template <>
class unique_unlock<Inode> {
public:
  explicit unique_unlock(Inode& in)
    : _in(&in), _released(false), _inode_released(false)
  {
    release();
  }

  unique_unlock(Inode& in, std::defer_lock_t)
    : _in(&in), _released(false), _inode_released(false)
  {}

  void release()
  {
    if (_released) {
      return;
    }
    if (_in->is_locked_by_me()) {
      _in->m_inode_lock.unlock();
      _inode_released = true;
    }
    _released = true;
  }

  bool released() const
  {
    return _released;
  }

  ~unique_unlock() noexcept(false)
  {
    if (_released && _inode_released) {
      _in->m_inode_lock.lock();
      _inode_released = false;
    }
  }

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  Inode *_in;
  bool _released;
  bool _inode_released;
};

} // namespace ceph

#endif // CEPH_CLIENT_INODE_LOCK_H