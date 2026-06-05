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
 * inode_lock before client_lock.  When client_lock is already held, drop the
 * full client recursion depth via unique_unlock, take inode_lock, then
 * restore client depth before returning.  unlock() releases only the inode
 * level owned by this guard.
 */
template <>
class unique_lock<Inode> {
public:
  using mutex_type = Inode;

  unique_lock() noexcept
    : _in(nullptr), _rl()
  {}

  explicit unique_lock(const Inode& in)
    : unique_lock(in, defer_lock)
  {
    lock();
  }

  unique_lock(const Inode& in, defer_lock_t) noexcept
    : _in(&in), _rl(in.m_inode_lock, defer_lock)
  {}

  unique_lock(const Inode& in, adopt_lock_t) noexcept
    : _in(&in), _rl(in.m_inode_lock, adopt_lock)
  {}

  unique_lock(const unique_lock&) = delete;
  unique_lock& operator=(const unique_lock&) = delete;
  unique_lock(unique_lock&&) = delete;
  unique_lock& operator=(unique_lock&&) = delete;

  ~unique_lock() = default;

  void lock()
  {
    if (!_in) {
      return;
    }
    auto& client = _in->get_client_lock();
    ceph::unique_unlock<ceph::ReentrantLock> client_stash(client, std::defer_lock);
    if (client.is_locked_by_me()) {
      client_stash.release();
    }
    _rl.lock();
    if (client_stash.released()) {
      client_stash.reacquire();
      client_stash._abandon();
    }
  }

  bool try_lock()
  {
    if (!_in) {
      return false;
    }
    if (_rl.owns_lock()) {
      return true;
    }
    auto& client = _in->get_client_lock();
    ceph::unique_unlock<ceph::ReentrantLock> client_stash(client, std::defer_lock);
    if (client.is_locked_by_me()) {
      client_stash.release();
    }
    if (!_rl.try_lock()) {
      if (client_stash.released()) {
        client_stash.reacquire();
        client_stash._abandon();
      }
      return false;
    }
    if (client_stash.released()) {
      client_stash.reacquire();
      client_stash._abandon();
    }
    return true;
  }

  void unlock()
  {
    _rl.unlock();
  }

  Inode *release() noexcept
  {
    _rl.release();
    Inode *in = const_cast<Inode *>(_in);
    _in = nullptr;
    return in;
  }

  bool owns_lock() const noexcept
  {
    return _rl.owns_lock();
  }

  Inode *mutex() const noexcept
  {
    return const_cast<Inode *>(_in);
  }

private:
  const Inode *_in;
  std::unique_lock<ceph::ReentrantLock> _rl;
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

  void reacquire()
  {
    _inner.reacquire();
  }

  void _abandon() noexcept
  {
    _inner._abandon();
  }

  ~unique_unlock() noexcept(false) = default;

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  unique_unlock<ReentrantLock> _inner;
};

} // namespace ceph

#endif // CEPH_CLIENT_INODE_LOCK_H