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
 * Lock ordering: inode_lock before client_lock.  When client_lock is already
 * held, temporarily drop it via unique_unlock, take inode_lock, then restore
 * client_lock.  Inode::m_inode_lock is a ReentrantLock; lock()/unlock()
 * delegate to it the same way unique_lock<Client> delegates to m_client_lock.
 */
template <>
class unique_lock<Inode> {
public:
  using mutex_type = Inode;

  unique_lock() noexcept
    : _in(nullptr), _owns(false)
  {}

  explicit unique_lock(const Inode& in)
    : unique_lock(in, defer_lock)
  {
    lock();
  }

  unique_lock(const Inode& in, defer_lock_t) noexcept
    : _in(&in), _owns(false)
  {}

  unique_lock(const Inode& in, adopt_lock_t) noexcept
    : _in(&in), _owns(true)
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
    if (_owns && _in->is_locked_by_me()) {
      return;
    }
    auto& client = _in->get_client_lock();
    if (client.is_locked_by_me()) {
      _client_unlock = std::make_unique<ceph::unique_unlock<ceph::ReentrantLock>>(
        client, std::defer_lock);
      _client_unlock->release();
    }
    auto& il = _in->m_inode_lock;
    if (!il.is_locked_by_me()) {
      il.lock();
    }
    if (_client_unlock) {
      client.lock();
    }
    _owns = true;
  }

  bool try_lock()
  {
    if (_owns && _in->is_locked_by_me()) {
      return true;
    }
    auto& client = _in->get_client_lock();
    const bool had_client = client.is_locked_by_me();
    if (had_client) {
      _client_unlock = std::make_unique<ceph::unique_unlock<ceph::ReentrantLock>>(
        client, std::defer_lock);
      _client_unlock->release();
    }
    if (_in->is_locked_by_me()) {
      // already held at inode level
    } else if (!_in->m_inode_lock.try_lock()) {
      if (_client_unlock) {
        _client_unlock->abandon();
        _client_unlock.reset();
      }
      return false;
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
    _owns = false;
    if (_in->is_locked_by_me()) {
      _in->m_inode_lock.unlock();
    }
    if (_client_unlock) {
      _client_unlock->abandon();
      _client_unlock.reset();
    }
  }

  bool owns_lock() const noexcept
  {
    if (!_owns || !_in) {
      return false;
    }
    return _in->is_locked_by_me();
  }

  Inode *mutex() const noexcept
  {
    return const_cast<Inode *>(_in);
  }

private:
  const Inode *_in;
  bool _owns;
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

template <>
class unique_unlock<Inode> {
public:
  explicit unique_unlock(Inode& in)
    : _inner(in.m_inode_lock)
  {}

  unique_unlock(Inode& in, std::defer_lock_t)
    : _inner(in.m_inode_lock, std::defer_lock)
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

  // Restore m_inode_lock after a temporary drop (e.g. get_caps wait).
  ~unique_unlock() noexcept(false) = default;

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  unique_unlock<ReentrantLock> _inner;
};

} // namespace ceph

#endif // CEPH_CLIENT_INODE_LOCK_H