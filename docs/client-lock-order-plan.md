# Client lock-order migration: prohibit client_lock + inode_lock overlap

## Goal

Reduce deadlock risk and lock juggling by enforcing:

> **A thread must never hold `client_lock` and any `inode_lock` at the same time.**

Hot paths (`ll_read` / `ll_write`) keep **inode_lock only**. Metadata paths (`ms_dispatch`) use **client_lock only**, dropping it before taking inode_lock when inode state is needed.

## Three tiers (must not nest across tiers)

| Tier | Lock | Protects |
|------|------|----------|
| 1 | `client_lock` | `inode_map`, sessions, requests, dispatch |
| 2 | `session_lock` | per-MDS `dirty_list`, `cap_gen`, session lists |
| 3 | `inode_lock` | per-inode caps, size, wait lists |

`session_lock` must not be held across `check_caps` / `cap_is_valid` while `client_lock` may be needed. Snapshot under `session_lock`, process under `inode_lock` only.

## Enforcement (`src/client/lock_order.h`)

- `thread_local inode_lock_depth` — incremented by `std::unique_lock<Inode>`, `ceph::unique_unlock<Inode>`
- `unique_lock<Inode>::lock()` → `assert_no_client_lock()` then `on_inode_locked()`
- `unique_lock<Client>::lock()` → `assert_no_inode_lock()`
- Debug-only: no stash/reacquire bridge in `inode_lock.h`

## PR stack

### PR1 — Instrumentation — **DONE**
- [x] Add `lock_order.h`
- [x] Counter hooks on `unique_lock<Client>` and `unique_lock<Inode>`
- [x] `report_overlap_if_any()` helper (available for spot checks)

### PR2 — Assert + remove inode bridge — **DONE (initial)**
- [x] `assert_no_client_lock` on inode acquire
- [x] `assert_no_inode_lock` on client acquire
- [x] Remove client stash/reacquire from `inode_lock.h`
- [x] Initial overlap fixes (dispatch, sync, get_caps, `_get_vino`)
- [ ] Remaining overlap call sites (many `Client.cc` metadata paths)
- [ ] Async bench clean under debug build

### PR3 — Fix remaining overlap hotspots
- [x] `handle_caps` GRANT/REVOKE: `cl.unlock()` before inode lock
- [x] `handle_client_reply`: drop `cl` before dir/target inode locks
- [x] `flush_caps_sync`: no `client_lock`; inode-only `check_caps`
- [x] `sync_fs` / unmount: drop client before `flush_caps_sync`
- [x] `ClientCaps` get_caps wait: `in_relock` guard
- [x] `get_caps` `check_pool_perm`: drop inode before client
- [ ] Audit `ceph_assert(ceph_mutex_is_locked_by_me(client))` in ClientCaps
- [ ] Path-walk / VFS helpers that take client then inode

### PR4 — Simplify wait/context paths
- [ ] Review `C_ReentrantCond` / `wait_on_context_list` after overlap gone
- [ ] Remove obsolete "inode before client" comments

### PR5 — Harden
- [ ] CI: debug build + async bench smoke
- [ ] Optional lockdep annotations

## Known overlap sites (fix checklist)

| Location | Issue | Status |
|----------|-------|--------|
| `Client.cc` `handle_caps` GRANT/REVOKE | `cl` + `in_lock` | **fixed** |
| `Client.cc` `handle_client_reply` | `cl` + dir/target lock | **fixed** |
| `ClientCaps.cc` `flush_caps_sync` | client assert + inode lock | **fixed** |
| `Client.cc` `sync_fs` / unmount | client held during flush | **fixed** |
| `ClientCaps.cc` get_caps wait / pool perm | overlap | **fixed** |
| `Client.cc` `_get_vino` | client + inode lock | **fixed** (no inode lock) |
| `inode_lock.h` | stash/reacquire bridge | **removed** |
| `Client.cc` path walk / setattr / … | client then inode | pending |

## Finding violations

```bash
# Debug build + bench
cephfs-tool ... bench ...

# Static: client + inode in same function without unlock between
rg 'unique_lock<Client>|scoped_lock<Client>' src/client -l
```

GDB: `p ceph::client_lock::order::inode_lock_depth`

## Progress log

| Date | Change |
|------|--------|
| 2026-06-08 | Plan written; PR1+PR2 infrastructure landed |
| 2026-06-08 | PR2 asserts enabled; initial hotspot fixes; many VFS paths still TODO |