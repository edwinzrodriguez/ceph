# -*- mode: python; coding: utf-8 -*-
#
# GDB helpers for MDS reactor (MDSOpWorkQueue) lane stats.
#
# Usage (live process or core):
#   (gdb) source doc/dev/debug/gdb-macros/mds-reactor-queue-gdb.py
#   (gdb) mds-reactor-queue
#   (gdb) mds-reactor-queue /kinds
#   (gdb) mds-reactor-queue /msgs
#   (gdb) mds-reactor-queue /age
#   (gdb) mds-reactor-queue /kinds /msgs /age
#   (gdb) mds-reactor-queue 0x55555d318000          # ReactorDispatchEngine*
#   (gdb) mds-reactor-queue ((MDSRank*)0x... )
#
# Also prints adaptive lane-slice state when engine symbols exist:
#   Client avg_exec / estimated backlog, adapt_t, effective Client /
#   Maintenance slice ms (mirrors ReactorDispatchEngine::lane_slice_ms).
#
# Auto-discovery (no arg): prefers `this` in a ReactorDispatchEngine frame
# (e.g. abort stack); falls back to mds-rank-op / MDSDaemon.
#
# Lane banks (double-buffered):
#   consumer / internal — drained by the op thread without the lane lock
#                         (lists[producer_bank ^ 1])
#   producer / external — appended to by producers under the lane lock
#                         (lists[producer_bank])

from __future__ import print_function

import collections

import gdb


LANE_NAMES = ("Control", "IOComplete", "Maintenance", "Client")
KIND_NAMES = (
    "InboundMessage",
    "IOCompletion",
    "AdvanceQueues",
    "TrimQuantum",
    "LogTrim",
    "Callable",
)

# Common MDS-facing message types (ceph_fs.h). Unknowns print as hex.
MSG_TYPE_NAMES = {
    1: "SHUTDOWN",
    2: "PING",
    21: "MDS_MAP",
    22: "CLIENT_SESSION",
    23: "CLIENT_RECONNECT",
    24: "CLIENT_REQUEST",
    25: "CLIENT_REQUEST_FORWARD",
    26: "CLIENT_REPLY",
    27: "CLIENT_RECLAIM",
    28: "CLIENT_RECLAIM_REPLY",
    29: "CLIENT_METRICS",
    0x310: "CLIENT_CAPS",
    0x311: "CLIENT_LEASE",
    0x312: "CLIENT_SNAP",
    0x313: "CLIENT_CAPRELEASE",
    0x314: "CLIENT_QUOTA",
}

# Safety cap when walking intrusive lists (constant_time_size is false).
MAX_WALK = 2_000_000


def _error(msg):
    raise gdb.GdbError(msg)


def _as_int(val):
    """Coerce a gdb.Value to int, including bool / enum / char."""
    try:
        return int(val)
    except (gdb.error, TypeError, ValueError):
        pass
    try:
        s = str(val).strip().lower()
        if s in ("true", "false"):
            return 1 if s == "true" else 0
        if s.startswith("true") or s.startswith("false"):
            return 1 if s.startswith("true") else 0
        return int(s.split()[0], 0)
    except (TypeError, ValueError):
        pass
    return None


def _atomic_load(val):
    """Read std::atomic / __atomic_base across libstdc++ / libc++ layouts."""
    seen = set()
    cur = val
    for _ in range(8):
        try:
            addr = int(cur.address)
        except (gdb.error, TypeError, AttributeError):
            addr = id(cur)
        if addr in seen:
            break
        seen.add(addr)

        n = _as_int(cur)
        if n is not None:
            return n

        for key in ("_M_base", "_M_i", "__a_", "_M_i_", "_M_b"):
            try:
                nxt = cur[key]
            except (gdb.error, KeyError, TypeError):
                continue
            n = _as_int(nxt)
            if n is not None:
                return n
            cur = nxt
            break
        else:
            break

    try:
        addr = int(val.address)
        size = int(val.type.sizeof)
        if size <= 0:
            size = 1
        mem = gdb.selected_inferior().read_memory(addr, size)
        if isinstance(mem, memoryview):
            raw = mem.tobytes()
        else:
            raw = bytes(mem)
        return int.from_bytes(raw, byteorder="little", signed=False)
    except Exception:
        pass

    _error("cannot read atomic value: %s" % val.type)


def _atomic_load_opt(val, default=None):
    try:
        return _atomic_load(val)
    except (gdb.GdbError, gdb.error, TypeError, ValueError, KeyError):
        return default


def _unique_ptr_get(up):
    """Dereference std::unique_ptr (libstdc++)."""
    candidates = (
        lambda u: u["_M_t"]["_M_t"]["_M_head_impl"],
        lambda u: u["_M_t"]["_M_t"]["__v_"],
        lambda u: u["_M_t"]["_M_head_impl"],
        lambda u: u["__ptr_"],
    )
    for getter in candidates:
        try:
            p = getter(up)
            if int(p) != 0:
                return p
            return p
        except (gdb.error, KeyError, TypeError):
            continue
    _error("cannot unwrap unique_ptr of type %s" % up.type)


def _intrusive_ptr_get(ip):
    """boost::intrusive_ptr<T> -> T*.

    Prefer raw/first-word or field access; px is private so some GDBs
    reject ip['px']. Fall back to public get() via parse_and_eval.
    """
    # 1) Direct field (when GDB allows private access)
    for key in ("px", "ptr_", "p_"):
        try:
            return ip[key]
        except (gdb.error, KeyError, TypeError):
            continue

    # 2) Raw load: intrusive_ptr is a single T* word
    try:
        addr = int(ip.address)
        ptr_size = int(gdb.lookup_type("void").pointer().sizeof)
        mem = gdb.selected_inferior().read_memory(addr, ptr_size)
        raw = mem.tobytes() if isinstance(mem, memoryview) else bytes(mem)
        ptr_val = int.from_bytes(raw, byteorder="little", signed=False)
        return gdb.Value(ptr_val).cast(gdb.lookup_type("Message").pointer())
    except Exception:
        pass

    # 3) Public get()
    try:
        addr = int(ip.address)
        ip_type = ip.type.strip_typedefs()
        if ip_type.code == gdb.TYPE_CODE_REF:
            ip_type = ip_type.target().strip_typedefs()
        p = gdb.parse_and_eval("((%s *)0x%x)->get()" % (str(ip_type), addr))
        return p
    except (gdb.error, TypeError, ValueError):
        pass
    return None


def _find_field(typ, name):
    """Find a field (incl. protected/private) in typ or its bases."""
    typ = typ.strip_typedefs()
    try:
        fields = typ.fields()
    except gdb.error:
        return None
    for f in fields:
        if getattr(f, "name", None) == name:
            return f
    for f in fields:
        if getattr(f, "is_base_class", False):
            found = _find_field(f.type, name)
            if found is not None:
                return found
    return None


def _message_type(msg_ptr):
    """Message* -> header.type (int)."""
    if msg_ptr is None:
        return None
    try:
        if int(msg_ptr) == 0:
            return None
    except (gdb.error, TypeError, ValueError):
        return None

    # Fast path: walk debug-info fields (works for protected header).
    try:
        msg_ty = gdb.lookup_type("Message")
        header_f = _find_field(msg_ty, "header")
        if header_f is not None and header_f.bitpos is not None:
            header_off = header_f.bitpos // 8
            type_f = _find_field(header_f.type, "type")
            if type_f is not None and type_f.bitpos is not None:
                type_off = header_off + type_f.bitpos // 8
                addr = int(msg_ptr) + type_off
                # ceph_msg_header::type is __le16
                mem = gdb.selected_inferior().read_memory(addr, 2)
                raw = mem.tobytes() if isinstance(mem, memoryview) else bytes(mem)
                return int.from_bytes(raw, byteorder="little", signed=False)
    except Exception:
        pass

    # Public get_type()
    try:
        t = gdb.parse_and_eval("((Message *)0x%x)->get_type()" % int(msg_ptr))
        n = _as_int(t)
        if n is not None:
            return n
    except (gdb.error, TypeError, ValueError):
        pass

    # Last resort: item access (may fail on protected / __le16)
    try:
        n = _as_int(msg_ptr["header"]["type"])
        if n is not None:
            return n
    except (gdb.error, KeyError, TypeError):
        pass
    return None


def _item_msg_type(item):
    """OpWorkItem -> message type int, or None."""
    try:
        if int(item["kind"]) != 0:  # WorkKind::InboundMessage
            return None
    except (gdb.error, TypeError, ValueError):
        return None
    try:
        return _message_type(_intrusive_ptr_get(item["msg"]))
    except (gdb.error, KeyError, TypeError, ValueError):
        return None


def _diagnose_msg_fail(item):
    """One-line reason for a /msgs unwrap failure (first few only)."""
    parts = []
    try:
        parts.append("item=%s" % item.address)
    except Exception:
        pass
    try:
        msg = item["msg"]
        parts.append("msg_type=%s" % msg.type)
        try:
            parts.append("msg.addr=%s" % msg.address)
        except Exception:
            pass
        for key in ("px",):
            try:
                parts.append("%s=%s" % (key, msg[key]))
            except Exception as e:
                parts.append("%s_err=%s" % (key, e))
        try:
            addr = int(msg.address)
            ptr_size = int(gdb.lookup_type("void").pointer().sizeof)
            mem = gdb.selected_inferior().read_memory(addr, ptr_size)
            raw = mem.tobytes() if isinstance(mem, memoryview) else bytes(mem)
            ptr_val = int.from_bytes(raw, byteorder="little", signed=False)
            parts.append("raw_px=0x%x" % ptr_val)
            if ptr_val:
                try:
                    t = gdb.parse_and_eval(
                        "((Message *)0x%x)->get_type()" % ptr_val
                    )
                    parts.append("get_type=%s" % t)
                except Exception as e:
                    parts.append("get_type_err=%s" % e)
                try:
                    msg_ty = gdb.lookup_type("Message")
                    header_f = _find_field(msg_ty, "header")
                    parts.append(
                        "header_field=%s bitpos=%s"
                        % (
                            header_f is not None,
                            getattr(header_f, "bitpos", None)
                            if header_f
                            else None,
                        )
                    )
                except Exception as e:
                    parts.append("header_field_err=%s" % e)
        except Exception as e:
            parts.append("raw_err=%s" % e)
    except Exception as e:
        parts.append("msg_err=%s" % e)
    return "; ".join(parts)


def _time_point_ns(tp):
    """Extract nanosecond tick count from std::chrono::time_point."""
    paths = (
        ("__d", "__r"),
        ("__dur", "__r"),
        ("_M_d", "_M_rep"),
        ("__d",),
    )
    for path in paths:
        cur = tp
        try:
            for key in path:
                cur = cur[key]
            n = _as_int(cur)
            if n is not None:
                return n
        except (gdb.error, KeyError, TypeError):
            continue
    try:
        addr = int(tp.address)
        size = int(tp.type.sizeof)
        if size in (4, 8):
            mem = gdb.selected_inferior().read_memory(addr, size)
            raw = mem.tobytes() if isinstance(mem, memoryview) else bytes(mem)
            return int.from_bytes(raw, byteorder="little", signed=True)
    except Exception:
        pass
    return None


def _is_core_file():
    """True when debugging a core dump (no live inferior clock)."""
    try:
        out = gdb.execute("info files", to_string=True)
        low = out.lower()
        if "local core dump file" in low or "core-file" in low:
            return True
        # e.g. ``/path/to/core'': file format elf64-x86-64
        for line in out.splitlines():
            s = line.strip().lower()
            if "core" in s and "file format" in s:
                return True
    except gdb.error:
        pass
    try:
        # Live processes answer info proc; cores often error.
        gdb.execute("info proc", to_string=True)
    except gdb.error as e:
        if "core" in str(e).lower():
            return True
    return False


def _fast_mono_now_ns():
    """Inferior ceph::fast_mono_clock::now() (or coarse fallback), or None.

    OpWorkItem::enqueued_at uses fast_mono_clock. Never use the host
    monotonic clock: on cores (and many remote attaches) it is unrelated
    to enqueued_at and produces multi-hour bogus waits.
    """
    if _is_core_file():
        return None
    for expr in (
        "ceph::fast_mono_clock::now()",
        "((ceph::fast_mono_clock::time_point)ceph::fast_mono_clock::now())",
        # Older binaries / fallback path
        "ceph::coarse_mono_clock::now()",
        "((ceph::coarse_mono_clock::time_point)ceph::coarse_mono_clock::now())",
    ):
        try:
            ns = _time_point_ns(gdb.parse_and_eval(expr))
            if ns is not None and ns > 0:
                return ns
        except gdb.error:
            continue
    return None


def _pick_age_reference(ages, now_ns):
    """Return (ref_ns, label). Prefer inferior now; else newest enqueue time."""
    newest = max(ages)
    oldest = min(ages)
    span = newest - oldest
    if now_ns is not None and now_ns >= newest:
        oldest_wait = now_ns - oldest
        # Reject polluted "now" (typical host-clock fallback on a core):
        # absolute waits dwarf the enqueue span of this burst.
        span_gate = max(span, 1)
        if oldest_wait <= max(60 * 10**9, 100 * span_gate):
            return now_ns, "vs fast_mono now"
        return (
            newest,
            "vs newest queued item (ignored implausible now; "
            "oldest_wait=%s span=%s)"
            % (_fmt_ns(oldest_wait), _fmt_ns(span)),
        )
    return newest, "vs newest queued item"


def _fmt_ns(ns):
    if ns is None:
        return "?"
    if ns < 0:
        return "<%s>" % _fmt_ns(-ns)
    if ns < 1000:
        return "%dns" % ns
    if ns < 1000 * 1000:
        return "%.1fus" % (ns / 1e3)
    if ns < 1000 * 1000 * 1000:
        return "%.1fms" % (ns / 1e6)
    return "%.2fs" % (ns / 1e9)


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = int(round((pct / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[max(0, min(k, len(sorted_vals) - 1))]


def _msg_type_name(type_id):
    name = MSG_TYPE_NAMES.get(type_id)
    if name:
        return "%s(0x%x)" % (name, type_id)
    return "type=0x%x" % type_id


def _item_enqueued_ns(item):
    try:
        return _time_point_ns(item["enqueued_at"])
    except (gdb.error, KeyError, TypeError):
        return None


def _intrusive_list_root(lst):
    """Return (root_node_value, root_address) for a boost::intrusive::list."""
    try:
        root = lst["data_"]["root_plus_size_"]["m_header"]
        return root, int(root.address)
    except (gdb.error, KeyError):
        pass
    try:
        root = lst["data_"]["root_"]
        return root, int(root.address)
    except (gdb.error, KeyError):
        pass
    _error("unrecognized boost::intrusive::list layout (%s)" % lst.type)


def _walk_intrusive_list(lst, opts):
    """Walk one bank; return dict with counts / optional histograms / ages."""
    root, root_addr = _intrusive_list_root(lst)
    try:
        node = root["next_"]
    except (gdb.error, KeyError):
        _error("list node missing next_ (%s)" % root.type)

    need_item = opts.kinds or opts.msgs or opts.ages
    op_type = None
    if need_item:
        try:
            op_type = gdb.lookup_type("OpWorkItem").pointer()
        except gdb.error:
            need_item = False

    kind_counts = collections.Counter() if opts.kinds else None
    msg_counts = collections.Counter() if opts.msgs else None
    ages = [] if opts.ages else None
    msg_fail = 0
    age_fail = 0
    first_msg_fail_reason = []

    n = 0
    while int(node) != 0 and int(node) != root_addr:
        if n >= MAX_WALK:
            _error("list walk exceeded %d nodes (corrupt?)" % MAX_WALK)

        if need_item and op_type is not None:
            try:
                item = node.cast(op_type)
            except (gdb.error, ValueError, TypeError):
                item = None

            if item is not None:
                if opts.kinds:
                    try:
                        kind_counts[int(item["kind"])] += 1
                    except (gdb.error, ValueError, TypeError):
                        kind_counts[-1] += 1

                if opts.msgs:
                    mt = _item_msg_type(item)
                    if mt is None:
                        try:
                            if int(item["kind"]) == 0:
                                msg_fail += 1
                                if len(first_msg_fail_reason) < 3:
                                    first_msg_fail_reason.append(
                                        _diagnose_msg_fail(item)
                                    )
                        except (gdb.error, TypeError, ValueError):
                            msg_fail += 1
                    else:
                        msg_counts[mt] += 1

                if opts.ages:
                    ns = _item_enqueued_ns(item)
                    if ns is None:
                        age_fail += 1
                    else:
                        ages.append(ns)

        n += 1
        try:
            node = node["next_"]
        except (gdb.error, KeyError):
            _error("broken next_ after %d nodes" % n)

    return {
        "n": n,
        "kinds": kind_counts,
        "msgs": msg_counts,
        "ages": ages,
        "msg_fail": msg_fail,
        "age_fail": age_fail,
        "msg_fail_diag": first_msg_fail_reason,
    }


class _WalkOpts(object):
    __slots__ = ("kinds", "msgs", "ages")

    def __init__(self, kinds=False, msgs=False, ages=False):
        self.kinds = kinds
        self.msgs = msgs
        self.ages = ages


def _lane_stats(lane_q, opts):
    producer = _atomic_load(lane_q["producer_bank"]) & 1
    consumer = producer ^ 1
    lists = lane_q["lists"]
    prod = _walk_intrusive_list(lists[producer], opts)
    cons = _walk_intrusive_list(lists[consumer], opts)
    return {
        "producer_bank": producer,
        "consumer_bank": consumer,
        "producer": prod["n"],
        "consumer": cons["n"],
        "producer_kinds": prod["kinds"],
        "consumer_kinds": cons["kinds"],
        "producer_msgs": prod["msgs"],
        "consumer_msgs": cons["msgs"],
        "producer_ages": prod["ages"],
        "consumer_ages": cons["ages"],
        "msg_fail": prod["msg_fail"] + cons["msg_fail"],
        "age_fail": prod["age_fail"] + cons["age_fail"],
        "msg_fail_diag": (prod.get("msg_fail_diag") or [])
        + (cons.get("msg_fail_diag") or []),
    }


def _find_engine_from_selected_frame():
    """If the selected frame is inside ReactorDispatchEngine, use its `this`."""
    try:
        frame = gdb.selected_frame()
    except gdb.error:
        return None
    while frame is not None:
        try:
            fn = frame.name() or ""
        except gdb.error:
            fn = ""
        if "ReactorDispatchEngine::" in fn:
            try:
                this = frame.read_var("this")
                return this.cast(
                    gdb.lookup_type("ReactorDispatchEngine").pointer()
                )
            except (gdb.error, ValueError):
                pass
        try:
            frame = frame.older()
        except gdb.error:
            break
    return None


def _find_engine_from_op_thread():
    """Locate ReactorDispatchEngine* via reactor frame `this`."""
    engine = _find_engine_from_selected_frame()
    if engine is not None:
        return engine

    try:
        inferior = gdb.selected_inferior()
    except gdb.error:
        return None

    saved = gdb.selected_thread()

    def search(prefer_named):
        for thr in inferior.threads():
            thr.switch()
            name = ""
            try:
                name = thr.name or ""
            except (gdb.error, AttributeError):
                pass
            if prefer_named and "mds-rank-op" not in name:
                continue
            frame = gdb.newest_frame()
            while frame is not None:
                try:
                    fn = frame.name() or ""
                except gdb.error:
                    fn = ""
                if "ReactorDispatchEngine::" in fn:
                    try:
                        this = frame.read_var("this")
                        return this.cast(
                            gdb.lookup_type("ReactorDispatchEngine").pointer()
                        )
                    except (gdb.error, ValueError):
                        pass
                try:
                    frame = frame.older()
                except gdb.error:
                    break
        return None

    try:
        engine = search(prefer_named=True)
        if engine is None:
            engine = search(prefer_named=False)
        return engine
    finally:
        if saved is not None:
            try:
                saved.switch()
            except gdb.error:
                pass


def _find_engine_from_mds_daemon():
    for expr in ("daemon.mds_rank", "mds_daemon.mds_rank", "mds_rank"):
        try:
            rank = gdb.parse_and_eval(expr)
            if int(rank) == 0:
                continue
            return _engine_from_rank(rank)
        except (gdb.error, ValueError, TypeError):
            continue
    return None


def _engine_from_rank(rank_val):
    try:
        rank = rank_val.cast(gdb.lookup_type("MDSRank").pointer())
    except gdb.error:
        rank = rank_val
    try:
        if int(rank) == 0:
            return None
    except (gdb.error, TypeError, ValueError):
        return None

    engine_up = rank["dispatch_engine"]
    base = _unique_ptr_get(engine_up)
    if int(base) == 0:
        return None
    try:
        reactor_ptr = base.cast(gdb.lookup_type("ReactorDispatchEngine").pointer())
        _ = reactor_ptr["queue"]
        return reactor_ptr
    except gdb.error as e:
        _error(
            "dispatch_engine at %s is not a ReactorDispatchEngine (%s)"
            % (base, e)
        )


def _resolve_queue(arg):
    """Return (MDSOpWorkQueue lvalue, ReactorDispatchEngine* or None, note)."""
    arg = (arg or "").strip()

    if not arg or arg.startswith("/"):
        engine = _find_engine_from_op_thread()
        if engine is None:
            engine = _find_engine_from_mds_daemon()
        if engine is None:
            _error(
                "could not auto-find ReactorDispatchEngine; pass an expression "
                "(ReactorDispatchEngine*, MDSOpWorkQueue*, or MDSRank*)"
            )
        return engine["queue"], engine, "auto: %s" % engine

    expr = arg.split()[0]
    try:
        val = gdb.parse_and_eval(expr)
    except gdb.error as e:
        _error("parse failed for %r: %s" % (expr, e))

    typ = str(val.type)
    try:
        is_ptr = val.type.code == gdb.TYPE_CODE_PTR
    except Exception:
        is_ptr = "*" in typ

    def as_ptr(v, type_name):
        t = gdb.lookup_type(type_name)
        if v.type.code == gdb.TYPE_CODE_PTR:
            return v.cast(t.pointer())
        return v.address.cast(t.pointer())

    if "ReactorDispatchEngine" in typ:
        engine = as_ptr(val, "ReactorDispatchEngine")
        return engine["queue"], engine, "arg: ReactorDispatchEngine*"
    if "MDSOpWorkQueue" in typ:
        if is_ptr:
            return val.dereference(), None, "arg: MDSOpWorkQueue*"
        return val, None, "arg: MDSOpWorkQueue"
    if "MDSRank" in typ:
        engine = _engine_from_rank(val if is_ptr else val.address)
        if engine is None:
            _error("MDSRank has null dispatch_engine")
        return engine["queue"], engine, "arg: MDSRank*"
    if "MDSDaemon" in typ:
        daemon = as_ptr(val, "MDSDaemon")
        engine = _engine_from_rank(daemon["mds_rank"])
        if engine is None:
            _error("MDSDaemon::mds_rank missing or null dispatch_engine")
        return engine["queue"], engine, "arg: MDSDaemon*"

    try:
        addr = int(val)
        engine = gdb.Value(addr).cast(
            gdb.lookup_type("ReactorDispatchEngine").pointer()
        )
        _ = engine["queue"]
        return engine["queue"], engine, "arg: address as ReactorDispatchEngine*"
    except (gdb.error, TypeError, ValueError):
        pass

    _error(
        "unsupported argument type %s; expected ReactorDispatchEngine*, "
        "MDSOpWorkQueue*, MDSRank*, MDSDaemon*, or address" % typ
    )


def _fmt_kinds(counter):
    if not counter:
        return ""
    parts = []
    for k in sorted(counter):
        name = KIND_NAMES[k] if 0 <= k < len(KIND_NAMES) else ("kind=%d" % k)
        parts.append("%s=%d" % (name, counter[k]))
    return "{" + ", ".join(parts) + "}"


def _fmt_msgs(counter, top=12):
    if not counter:
        return ""
    items = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    shown = items[:top]
    parts = ["%s=%d" % (_msg_type_name(t), c) for t, c in shown]
    s = "{" + ", ".join(parts) + "}"
    more = len(items) - len(shown)
    if more > 0:
        s += " (+%d more types)" % more
    return s


def _fmt_us(us):
    """Format microseconds as a short human string."""
    if us is None:
        return "?"
    us = int(us)
    if us < 1000:
        return "%dus" % us
    if us < 1_000_000:
        return "%.2fms" % (us / 1000.0)
    return "%.2fs" % (us / 1_000_000.0)


def _lane_depth_atomic(lane_q):
    """O(1) LaneQueue::depth if present; None on older builds."""
    try:
        return _atomic_load_opt(lane_q["depth"], default=None)
    except (gdb.error, KeyError, TypeError, ValueError):
        return None


def _exec_window_avg_us(engine, lane_idx):
    """
    Mirror ReactorDispatchEngine::avg_exec_us for one lane.
    Returns (avg_us, sample_count, window_n) or None if unavailable.
    Warm-up (< min(window_n, 32) samples) uses the 20us Client default.
    """
    try:
        windows = engine["exec_windows"]
        w = windows[lane_idx]
        filled = int(w["filled"])
        window_n = int(w["window_n"])
        sum_us = int(w["sum"])
    except (gdb.error, KeyError, TypeError, ValueError):
        return None
    warm = min(window_n, 32) if window_n > 0 else 32
    if filled < warm:
        avg = 20
    elif filled == 0:
        avg = 0
    else:
        avg = sum_us // filled
        if avg == 0:
            avg = 1
    return avg, filled, window_n


def _adaptive_slice_state(engine, queue):
    """
    Read adaptive-slice caches / Client backlog without walking lists.
    Returns a dict or None if symbols are missing (older binary).
    """
    try:
        target_us = _atomic_load_opt(engine["slice_backlog_target_us"])
        client_max = _atomic_load_opt(engine["client_slice_max_ms"])
        maint_min = _atomic_load_opt(engine["maintenance_slice_min_ms"])
        win_n = _atomic_load_opt(engine["exec_window_n"])
        bases = []
        for i in range(len(LANE_NAMES)):
            bases.append(
                _atomic_load_opt(
                    engine["lane_slice_ms_cached"][i], default=None
                )
            )
    except (gdb.error, KeyError, TypeError, ValueError):
        return None

    lanes = queue["lanes"]
    depths = []
    for i in range(len(LANE_NAMES)):
        depths.append(_lane_depth_atomic(lanes[i]))

    client_idx = LANE_NAMES.index("Client")
    client_avg = _exec_window_avg_us(engine, client_idx)
    if client_avg is None:
        return None
    avg_us, filled, win_filled_n = client_avg
    client_depth = depths[client_idx]
    if client_depth is None:
        backlog_us = None
    else:
        backlog_us = int(client_depth) * int(avg_us)

    # Mirror client_adapt_t() / lane_slice_ms().
    adapt_t = 0.0
    if target_us and target_us > 0 and backlog_us is not None:
        max_ratio = 4.0
        ratio = float(backlog_us) / float(target_us)
        if ratio >= 1.0:
            adapt_t = min(
                1.0, (min(ratio, max_ratio) - 1.0) / (max_ratio - 1.0)
            )

    def effective(lane_idx, base):
        if base is None:
            return None
        base = int(base)
        if base <= 0:
            return base  # drain-until-empty: no adapt
        name = LANE_NAMES[lane_idx]
        if name not in ("Client", "Maintenance") or adapt_t <= 0.0:
            return base
        if name == "Client":
            max_ms = int(client_max) if client_max is not None else base
            if max_ms < base:
                max_ms = base
            return base + int((max_ms - base) * adapt_t)
        min_ms = int(maint_min) if maint_min is not None else 0
        if min_ms < 0:
            min_ms = 0
        if min_ms > base:
            min_ms = base
        return base - int((base - min_ms) * adapt_t)

    eff = [effective(i, bases[i]) for i in range(len(LANE_NAMES))]
    return {
        "target_us": target_us,
        "client_max_ms": client_max,
        "maint_min_ms": maint_min,
        "exec_window_n": win_n,
        "bases": bases,
        "depths": depths,
        "client_avg_us": avg_us,
        "client_samples": filled,
        "client_window_n": win_filled_n,
        "client_backlog_us": backlog_us,
        "adapt_t": adapt_t,
        "effective": eff,
    }


def _print_adaptive_slices(state):
    """Compact block for adaptive Client/Maintenance slices."""
    print(
        "  adaptive slices: target=%s  exec_window_n=%s  "
        "client_max=%sms  maint_min=%sms"
        % (
            _fmt_us(state["target_us"]),
            state["exec_window_n"]
            if state["exec_window_n"] is not None
            else "?",
            state["client_max_ms"]
            if state["client_max_ms"] is not None
            else "?",
            state["maint_min_ms"]
            if state["maint_min_ms"] is not None
            else "?",
        )
    )
    cidx = LANE_NAMES.index("Client")
    print(
        "    Client: depth=%s  avg_exec=%s (%d/%s samples)  "
        "backlog=%s  adapt_t=%.3f"
        % (
            state["depths"][cidx] if state["depths"][cidx] is not None else "?",
            _fmt_us(state["client_avg_us"]),
            state["client_samples"],
            state["client_window_n"],
            _fmt_us(state["client_backlog_us"]),
            state["adapt_t"],
        )
    )
    parts = []
    for i, name in enumerate(LANE_NAMES):
        base = state["bases"][i]
        eff = state["effective"][i]
        if base is None and eff is None:
            continue
        if base == eff:
            parts.append("%s=%sms" % (name, base if base is not None else "?"))
        else:
            parts.append(
                "%s=%s->%sms"
                % (
                    name,
                    base if base is not None else "?",
                    eff if eff is not None else "?",
                )
            )
    print("    effective: %s" % ("  ".join(parts) if parts else "(n/a)"))


def _print_age_stats(label, ages, now_ns):
    if not ages:
        print("    %s: (no enqueued_at samples)" % label)
        return

    ages_sorted_enq = sorted(ages)
    newest = ages_sorted_enq[-1]
    oldest = ages_sorted_enq[0]
    span = newest - oldest

    ref_ns, ref = _pick_age_reference(ages, now_ns)
    waits = sorted(ref_ns - a for a in ages)

    print(
        "    %s: n=%d  %s  oldest_wait=%s  p50=%s  p90=%s  p99=%s  "
        "newest_wait=%s  enqueue_span=%s"
        % (
            label,
            len(waits),
            ref,
            _fmt_ns(waits[-1] if waits else None),
            _fmt_ns(_percentile(waits, 50)),
            _fmt_ns(_percentile(waits, 90)),
            _fmt_ns(_percentile(waits, 99)),
            _fmt_ns(waits[0] if waits else None),
            _fmt_ns(span),
        )
    )


class MdsReactorQueue(gdb.Command):
    """Summarize MDS reactor per-lane producer/consumer queue depths.

    mds-reactor-queue [/kinds] [/msgs] [/age] [EXPR]

    Producer bank = external (enqueue side).
    Consumer bank = internal (op-thread drain side).
    /msgs  — histogram of InboundMessage header.type
    /age   — enqueue-wait stats from OpWorkItem::enqueued_at

    When ReactorDispatchEngine adaptive-slice fields exist, also prints
    Client avg_exec / backlog and effective Client/Maintenance slice ms
    (no list walk — O(1) atomics + ExecWindow).
    """

    def __init__(self):
        super(MdsReactorQueue, self).__init__(
            "mds-reactor-queue", gdb.COMMAND_DATA, gdb.COMPLETE_EXPRESSION
        )

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg) if arg else []
        collect_kinds = False
        collect_msgs = False
        collect_ages = False
        exprs = []
        for a in argv:
            if a in ("/kinds", "-k", "--kinds"):
                collect_kinds = True
            elif a in ("/msgs", "-m", "--msgs"):
                collect_msgs = True
            elif a in ("/age", "-a", "--age"):
                collect_ages = True
            else:
                exprs.append(a)
        expr = " ".join(exprs)
        opts = _WalkOpts(
            kinds=collect_kinds, msgs=collect_msgs, ages=collect_ages
        )

        queue, engine, note = _resolve_queue(expr)
        depth = _atomic_load_opt(queue["depth"], default=None)
        stopping = _atomic_load_opt(queue["stopping"], default=None)

        print("MDS reactor queue (%s)" % note)
        if engine is not None:
            try:
                qmax = _atomic_load_opt(engine["queue_len_max"])
                abort_lim = _atomic_load_opt(engine["queue_len_abort_limit"])
                trim_q = _atomic_load_opt(engine["trim_quantum_queued"])
                log_trim_q = _atomic_load_opt(engine["log_trim_queued"])
                print(
                    "  engine %s  queue_len_max=%s  abort_limit=%s  "
                    "trim_quantum_queued=%s  log_trim_queued=%s"
                    % (engine, qmax, abort_lim, trim_q, log_trim_q)
                )
            except (gdb.error, KeyError, TypeError, ValueError) as e:
                print("  engine %s  (extra fields unavailable: %s)" % (engine, e))
            try:
                adapt = _adaptive_slice_state(engine, queue)
                if adapt is not None:
                    _print_adaptive_slices(adapt)
            except (gdb.error, KeyError, TypeError, ValueError) as e:
                print("  adaptive slices unavailable: %s" % e)

        print(
            "  depth(atomic)=%s  stopping=%s"
            % (
                depth if depth is not None else "?",
                stopping if stopping is not None else "?",
            )
        )
        print(
            "  %-12s %8s %8s %8s %6s  %s"
            % ("lane", "consumer", "producer", "total", "pbank", "notes")
        )
        print(
            "  %-12s %8s %8s %8s %6s  %s"
            % ("", "(internal)", "(external)", "", "", "")
        )

        now_ns = _fast_mono_now_ns() if collect_ages else None
        walked = 0
        all_ages = []
        lanes = queue["lanes"]
        for i in range(len(LANE_NAMES)):
            name = LANE_NAMES[i]
            try:
                st = _lane_stats(lanes[i], opts)
            except gdb.GdbError as e:
                print("  %-12s  ERROR: %s" % (name, e))
                continue
            total = st["consumer"] + st["producer"]
            walked += total
            notes = "c_bank=%d" % st["consumer_bank"]
            depth_at = _lane_depth_atomic(lanes[i])
            if depth_at is not None and depth_at != total:
                notes += " depth_at=%d" % depth_at
            print(
                "  %-12s %8d %8d %8d %6d  %s"
                % (
                    name,
                    st["consumer"],
                    st["producer"],
                    total,
                    st["producer_bank"],
                    notes,
                )
            )
            if collect_kinds:
                if st["consumer_kinds"]:
                    print(
                        "    consumer kinds: %s"
                        % _fmt_kinds(st["consumer_kinds"])
                    )
                if st["producer_kinds"]:
                    print(
                        "    producer kinds: %s"
                        % _fmt_kinds(st["producer_kinds"])
                    )
            if collect_msgs:
                if st["consumer_msgs"]:
                    print(
                        "    consumer msgs: %s" % _fmt_msgs(st["consumer_msgs"])
                    )
                if st["producer_msgs"]:
                    print(
                        "    producer msgs: %s" % _fmt_msgs(st["producer_msgs"])
                    )
                if st["msg_fail"]:
                    print(
                        "    (inbound msg unwrap failures: %d)" % st["msg_fail"]
                    )
                    for i, diag in enumerate(st.get("msg_fail_diag") or []):
                        print("      fail[%d]: %s" % (i, diag))
            if collect_ages:
                if st["consumer_ages"]:
                    _print_age_stats(
                        "consumer age", st["consumer_ages"], now_ns
                    )
                    all_ages.extend(st["consumer_ages"])
                if st["producer_ages"]:
                    _print_age_stats(
                        "producer age", st["producer_ages"], now_ns
                    )
                    all_ages.extend(st["producer_ages"])
                if st["age_fail"]:
                    print(
                        "    (enqueued_at unwrap failures: %d)" % st["age_fail"]
                    )

        print("  %-12s %8s %8s %8d" % ("SUM(walked)", "", "", walked))
        if depth is not None and walked != depth:
            print(
                "  warning: walked sum (%d) != depth atomic (%d) "
                "(race during live attach, or concurrent mutate)"
                % (walked, depth)
            )
        if collect_ages and all_ages:
            print("  overall age:")
            _print_age_stats("all lanes", all_ages, now_ns)
            ref_ns, ref = _pick_age_reference(all_ages, now_ns)
            if "newest queued" in ref:
                print(
                    "  note: ages are relative to the newest queued item "
                    "(no usable inferior fast_mono now — normal on cores). "
                    "enqueue_span is the backlog birth window."
                )


MdsReactorQueue()
print(
    "Loaded mds-reactor-queue "
    "(try: mds-reactor-queue [/kinds] [/msgs] [/age] [EXPR])"
)
