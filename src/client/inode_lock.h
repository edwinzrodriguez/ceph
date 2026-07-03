// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_INODE_LOCK_H
#define CEPH_CLIENT_INODE_LOCK_H

#include <memory>
#include <mutex>

#include "client/lock_order.h"
#include "common/ceph_mutex.h"
#include "common/reentrant_lock.h"

// Included from Inode.h after struct Inode is defined.

namespace std {

/**
 * RAII for Inode::m_inode_lock.
 *
 * client_lock and inode_lock must not be held by the same thread at the same
 * time.  Drop client_lock (e.g. unique_unlock<Client> or cl.unlock()) before
 * taking inode_lock.
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
  {
    ceph::client_lock::order::on_inode_locked();
  }

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
    if (!_in || _rl.owns_lock()) {
      return;
    }
    ceph::client_lock::order::assert_no_client_lock(_in->get_client_lock());
    _rl.lock();
    ceph::client_lock::order::on_inode_locked();
  }

  bool try_lock()
  {
    if (!_in) {
      return false;
    }
    if (_rl.owns_lock()) {
      return true;
    }
    ceph::client_lock::order::assert_no_client_lock(_in->get_client_lock());
    if (!_rl.try_lock()) {
      return false;
    }
    ceph::client_lock::order::on_inode_locked();
    return true;
  }

  void unlock()
  {
    if (_rl.owns_lock()) {
      ceph::client_lock::order::on_inode_unlocked();
      _rl.unlock();
    }
  }

  Inode *release() noexcept
  {
    if (_rl.owns_lock()) {
      ceph::client_lock::order::on_inode_unlocked();
    }
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
    : _in(&in), _saved(0), _released(false)
  {
    release();
  }

  unique_unlock(Inode& in, std::defer_lock_t)
    : _in(&in), _saved(0), _released(false)
  {}

  void release()
  {
    if (_released || !_in || !_in->m_inode_lock.is_locked_by_me()) {
      return;
    }
    _saved = _in->m_inode_lock.release_for_wait();
    for (int i = 0; i < _saved; ++i) {
      ceph::client_lock::order::on_inode_unlocked();
    }
    _in->m_inode_lock.native_mutex().unlock();
    _released = true;
  }

  bool released() const
  {
    return _released;
  }

  void reacquire()
  {
    if (!_released || _saved <= 0 || !_in) {
      return;
    }
    ceph::client_lock::order::assert_no_client_lock(_in->get_client_lock());
    _in->m_inode_lock.native_mutex().lock();
    _in->m_inode_lock.restore_after_wait(_saved);
    for (int i = 0; i < _saved; ++i) {
      ceph::client_lock::order::on_inode_locked();
    }
    _released = false;
    _saved = 0;
  }

  void _abandon() noexcept
  {
    if (!_released && _in && _in->m_inode_lock.is_locked_by_me()) {
      int saved = _in->m_inode_lock.release_for_wait();
      for (int i = 0; i < saved; ++i) {
	ceph::client_lock::order::on_inode_unlocked();
      }
      _in->m_inode_lock.native_mutex().unlock();
    }
    _released = false;
    _saved = 0;
  }

  ~unique_unlock() noexcept(false)
  {
    reacquire();
  }

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  Inode *_in;
  int _saved;
  bool _released;
};

} // namespace ceph

#endif // CEPH_CLIENT_INODE_LOCK_H