#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil -*-
# vim: ts=4 sw=4 expandtab
#
# Collect MDS blocked-op diagnostics and correlate ops, locks, inodes,
# and client sessions. Intended to run on a node with working `ceph`
# admin access (typically an MDS host or admin node).
#
# Examples:
#   ./mds-stall-debug.py --mds myfs:0
#   ./mds-stall-debug.py --daemon mds.myfs-1.abc123 --output /tmp/mds-debug
#   ./mds-stall-debug.py --mds myfs:0 --reqid client.435625:1356421
#   ./mds-stall-debug.py --mds myfs:0 --all-ops --follow-parents
#   ./mds-stall-debug.py --stall-dir ./mds-stall-foo --extract-log /var/log/ceph/mds.a.log
#   ./mds-stall-debug.py --daemon mds.myfs-0.abc --grafana-url http://localhost:3000
#   ./mds-stall-debug.py --daemon mds.myfs-0.abc --client-debugfs \
#       --client-host client-000 --client-host client-001
#
# Log extract uses stall dir timestamp (…-YYYYMMDD-HHMMSS), blocked/historic op event
# times, and report events — not report.txt collection time for idle (0 blocked op) stalls.
# Idle stalls anchor on last client activity (max op event time), not oldest initiated_at.
# If the stall snapshot is after the log ends but activity is still in-log, use that window;
# otherwise fall back to the log tail (--log-fallback-tail-seconds).
#
# When blocked_ops is 0 (typical reactor reply-collapse / client-idle stalls), also
# collects ops_in_flight, historic ops, session ls (without cap dump by default),
# perf dump, and status. Pass --session-cap-dump to include per-cap details (slow).

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

LOCK_FLAG_NAMES = {
    1: "rdlock",
    2: "wrlock",
    4: "xlock",
    8: "remote_wrlock",
    16: "state_pin",
}

SCATTER_LOCK_TYPES = {"ifile", "inest", "idirfragtree"}
SUSPICIOUS_SCATTER_STATES = {"sync->lock", "excl->sync", "sync->excl", "lock->sync"}

REQID_RE = re.compile(
    r"client_request\((client\.(\d+):(\d+))\s+(\w+)\s+#(0x[0-9a-f]+)/(\S+)"
)
INODE_OBJ_RE = re.compile(r"\[inode (0x[0-9a-f]+)")
PATH_INO_RE = re.compile(r"#(0x[0-9a-f]+)/")

REPORT_STALL_TIME_RE = re.compile(
    r"MDS stall debug report for mds\..+ at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"
)
REPORT_EVENT_TIME_RE = re.compile(
    r"last event: .+ @ (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(?:\+\d{4}|Z))"
)
LOG_LINE_TS_RE = re.compile(
    rb"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)(\+\d{4}|Z)"
)
STALL_DIR_TS_RE = re.compile(r"-(\d{8})-(\d{6})$")


@dataclass
class OpSummary:
    reqid: str
    client_id: str
    tid: str
    opcode: str
    parent_ino_hex: str
    dentry_name: str
    flag_point: str
    age_sec: float
    description: str
    held_locks: List[Dict[str, Any]] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CapRevokeInfo:
    ino_hex: str
    client_id: str
    path: str
    pending: str
    issued: str
    wanted: str
    revokes: List[Dict[str, Any]]
    last_issue_stamp: Optional[str]
    last_sent: Optional[int]
    outstanding: bool
    revoked_caps: str
    notes: List[str] = field(default_factory=list)
    related_ops: List[str] = field(default_factory=list)


@dataclass
class InodeSummary:
    ino_hex: str
    ino_dec: Optional[int]
    path: str
    locks: Dict[str, Dict[str, Any]]
    client_caps: List[Dict[str, Any]]
    loner: Optional[int]
    want_loner: Optional[int]
    pins: Dict[str, int]
    quiesce_block: bool
    notes: List[str] = field(default_factory=list)


class CephMDSClient:
    def __init__(self, conf: Optional[str], timeout: int) -> None:
        self.conf = conf
        self.timeout = timeout

    def _base(self) -> List[str]:
        cmd = ["ceph"]
        if self.conf:
            cmd.extend(["--conf", self.conf])
        return cmd

    @staticmethod
    def is_asok_path(target: str) -> bool:
        return target.endswith(".asok") or "/" in target

    @staticmethod
    def list_sibling_asoks(asok_path: str) -> List[str]:
        try:
            parent = Path(asok_path).expanduser().resolve().parent
        except OSError:
            return []
        if not parent.is_dir():
            return []
        try:
            return sorted(str(p) for p in parent.glob("ceph-mds*.asok"))
        except OSError:
            return []

    @staticmethod
    def is_transient_asok_error(err: str) -> bool:
        transient_markers = (
            "Resource temporarily unavailable",
            "timed out",
            "Timeout",
            "Connection refused",
            "Connection reset",
            "[Errno 11]",
            "[Errno 110]",
            "[Errno 111]",
        )
        return any(marker in err for marker in transient_markers)

    @staticmethod
    def is_unimplemented_asok_error(err: str) -> bool:
        markers = (
            "Function not implemented",
            "(38)",
            "ENOSYS",
            "unknown command",
            "unrecognized command",
        )
        return any(marker in err for marker in markers)

    @staticmethod
    def parse_json_stdout(stdout: str, *, cmd: List[str]) -> Any:
        text = stdout.strip()
        if not text:
            return {}
        # Some admin-socket paths print ERROR on stdout with exit 0.
        if text.startswith("ERROR:") or text.startswith("error:"):
            raise RuntimeError(f"command failed: {' '.join(cmd)}\n{text}")
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            preview = text[:200].replace("\n", "\\n")
            raise RuntimeError(
                f"command returned non-JSON: {' '.join(cmd)}\n{preview}"
            ) from exc

    def run_json(
        self,
        args: List[str],
        *,
        retries: int = 5,
        retry_delay: float = 2.0,
    ) -> Any:
        cmd = self._base() + args + ["--format", "json"]
        last_err = "unknown error"
        for attempt in range(max(1, retries)):
            try:
                proc = subprocess.run(
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                )
            except subprocess.TimeoutExpired as exc:
                last_err = (
                    f"subprocess timed out after {self.timeout}s "
                    f"(MDS may be laggy / holding mds_lock)"
                )
                if attempt + 1 < retries:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise RuntimeError(
                    f"command timed out: {' '.join(cmd)}\n{last_err}"
                ) from exc

            if proc.returncode == 0:
                try:
                    return self.parse_json_stdout(proc.stdout, cmd=cmd)
                except RuntimeError as exc:
                    last_err = str(exc)
                    # Non-JSON / ERROR stdout is not transient; stop retrying.
                    break

            last_err = proc.stderr.strip() or proc.stdout.strip() or "unknown error"
            if self.is_transient_asok_error(last_err) and attempt + 1 < retries:
                time.sleep(retry_delay * (attempt + 1))
                continue
            break

        hint = ""
        asok = None
        if len(args) >= 2 and args[0] in ("--admin-daemon", "daemon"):
            asok = args[1]
        if asok and self.is_asok_path(asok):
            if "Connection refused" in last_err or "No such file" in last_err:
                siblings = self.list_sibling_asoks(asok)
                hint = (
                    "\nHint: admin socket unreachable (stale path or wrong FS name). "
                    f"Tried: {asok}"
                )
                if siblings:
                    hint += "\nAvailable MDS sockets:\n  " + "\n  ".join(siblings)
                else:
                    hint += "\nCheck: ls /var/run/ceph/ceph-mds*.asok"
            elif self.is_unimplemented_asok_error(last_err):
                hint = (
                    "\nHint: admin-socket command is not implemented by this MDS "
                    "build (common for reactor MDS or trimmed command sets). "
                    "Continuing with partial collection is OK when "
                    "--client-debugfs / --grafana-url is set."
                )
            elif self.is_transient_asok_error(last_err):
                hint = (
                    "\nHint: MDS admin socket is not responding (common when "
                    "rank is active(laggy) / stuck on mds_lock). Retry later, or "
                    "re-run with --client-debugfs / --grafana-url to collect "
                    "non-MDS evidence while the asok is wedged."
                )
        if last_err.startswith("command failed:") or last_err.startswith(
            "command returned non-JSON:"
        ):
            raise RuntimeError(f"{last_err}{hint}")
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\n{last_err}{hint}"
        )

    def tell(self, mds_target: str, cmd: List[str]) -> Any:
        return self.run_json(["tell", f"mds.{mds_target}"] + cmd)

    def daemon(self, daemon_name: str, cmd: List[str]) -> Any:
        # Socket paths must use --admin-daemon; `ceph daemon <name>` is for
        # daemon names resolved via the cluster (e.g. mds.foo).
        if self.is_asok_path(daemon_name):
            return self.run_json(["--admin-daemon", daemon_name] + cmd)
        return self.run_json(["daemon", daemon_name] + cmd)


def hex_ino_to_dec(ino_hex: str) -> int:
    return int(ino_hex, 16)


def dec_ino_to_hex(ino_dec: int) -> str:
    return f"0x{ino_dec:x}"


def parse_reqid_from_description(desc: str) -> Optional[Tuple[str, str, str, str, str, str]]:
    m = REQID_RE.search(desc)
    if not m:
        return None
    reqid, client_id, tid, opcode, parent_ino, dentry = m.groups()
    return reqid, client_id, tid, opcode, parent_ino, dentry


def extract_inos_from_op(op: Dict[str, Any]) -> Set[str]:
    inos: Set[str] = set()
    desc = op.get("description", "")
    m = parse_reqid_from_description(desc)
    if m:
        inos.add(m[4])

    td = op.get("type_data") or {}
    for lock_entry in td.get("locks") or []:
        obj_str = lock_entry.get("object_string") or ""
        m2 = INODE_OBJ_RE.search(obj_str)
        if m2:
            inos.add(m2.group(1))
        ino = (lock_entry.get("object") or {}).get("ino")
        if ino is not None:
            inos.add(dec_ino_to_hex(int(ino)))
    return inos


def summarize_lock_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    obj_str = entry.get("object_string") or ""
    ino_hex = None
    m = INODE_OBJ_RE.search(obj_str)
    if m:
        ino_hex = m.group(1)
    lock = entry.get("lock") or {}
    return {
        "ino_hex": ino_hex,
        "object_string": obj_str,
        "lock_type": lock.get("type"),
        "lock_state": lock.get("state"),
        "num_rdlocks": lock.get("num_rdlocks", 0),
        "num_wrlocks": lock.get("num_wrlocks", 0),
        "num_xlocks": lock.get("num_xlocks", 0),
        "held_by_op": LOCK_FLAG_NAMES.get(entry.get("flags", 0), f"flags={entry.get('flags')}"),
        "wrlock_target": entry.get("wrlock_target"),
    }


def op_from_json(op: Dict[str, Any]) -> OpSummary:
    desc = op.get("description", "")
    td = op.get("type_data") or {}
    parsed = parse_reqid_from_description(desc)
    if parsed:
        reqid, client_id, tid, opcode, parent_ino, dentry = parsed
    else:
        reqid = ""
        client_id = ""
        tid = ""
        opcode = ""
        parent_ino = ""
        dentry = ""
        m = PATH_INO_RE.search(desc)
        if m:
            parent_ino = m.group(1)

    return OpSummary(
        reqid=reqid,
        client_id=client_id,
        tid=tid,
        opcode=opcode,
        parent_ino_hex=parent_ino,
        dentry_name=dentry,
        flag_point=td.get("flag_point") or "",
        age_sec=float(op.get("age") or 0),
        description=desc,
        held_locks=[summarize_lock_entry(x) for x in (td.get("locks") or [])],
        events=list(td.get("events") or []),
    )


def normalize_ino_hex(ino: Any) -> Optional[str]:
    if ino is None:
        return None
    if isinstance(ino, int):
        return dec_ino_to_hex(ino)
    s = str(ino).strip().lower()
    if not s:
        return None
    if s.startswith("0x"):
        return "0x" + s[2:]
    try:
        return dec_ino_to_hex(int(s, 0))
    except ValueError:
        return None


def unwrap_cap_entry(entry: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    nested = entry.get("cap")
    if isinstance(nested, dict):
        return nested
    return entry


def extract_session_caps(sess: Dict[str, Any]) -> List[Dict[str, Any]]:
    caps = sess.get("caps")
    if not isinstance(caps, list):
        return []
    out: List[Dict[str, Any]] = []
    for entry in caps:
        cap = unwrap_cap_entry(entry)
        if cap is not None:
            out.append(cap)
    return out


def caps_letters_only(s: str) -> Set[str]:
    return {c for c in s if c.isupper()}


def caps_revoked_letters(pending: str, issued: str) -> str:
    pending_set = caps_letters_only(pending)
    return "".join(sorted(caps_letters_only(issued) - pending_set))


def op_initiated_at(op: OpSummary) -> Optional[str]:
    for ev in op.events:
        if ev.get("event") == "initiated":
            return ev.get("time")
    return None


def build_cap_revoke_correlation(
    ops: List[OpSummary],
    inode_by_hex: Dict[str, InodeSummary],
    sessions: Dict[str, Any],
) -> Tuple[List[CapRevokeInfo], List[Dict[str, Any]]]:
    relevant_inos: Set[str] = set(inode_by_hex.keys())
    for op in ops:
        if op.parent_ino_hex:
            relevant_inos.add(op.parent_ino_hex)
        for lk in op.held_locks:
            if lk.get("ino_hex"):
                relevant_inos.add(lk["ino_hex"])

    issued_by_ino_client: Dict[Tuple[str, str], str] = {}
    for ino_hex, summary in inode_by_hex.items():
        for cap in summary.client_caps:
            issued_by_ino_client[(ino_hex, str(cap.get("client_id")))] = cap.get("issued") or ""

    cap_infos: List[CapRevokeInfo] = []
    for cid, sess in sessions.items():
        for cap in extract_session_caps(sess):
            ino_hex = normalize_ino_hex(cap.get("ino"))
            if not ino_hex or ino_hex not in relevant_inos:
                continue

            pending = cap.get("pending") or ""
            issued = issued_by_ino_client.get((ino_hex, cid), "")
            wanted = cap.get("wanted") or ""
            revokes = list(cap.get("revokes") or [])
            revoked = caps_revoked_letters(pending, issued)
            outstanding = bool(revokes) or (issued and pending != issued)

            info = CapRevokeInfo(
                ino_hex=ino_hex,
                client_id=cid,
                path=(inode_by_hex.get(ino_hex) or InodeSummary(
                    ino_hex, None, "", {}, [], None, None, {}, False,
                )).path,
                pending=pending,
                issued=issued,
                wanted=wanted,
                revokes=revokes,
                last_issue_stamp=cap.get("last_issue_stamp"),
                last_sent=cap.get("last_sent"),
                outstanding=outstanding,
                revoked_caps=revoked,
            )

            if revokes:
                info.notes.append(
                    f"session has {len(revokes)} outstanding cap revoke(s)"
                )
            if revoked:
                info.notes.append(f"revoking cap letters: {revoked}")
            if issued and pending != issued:
                info.notes.append(f"pending={pending} issued={issued}")

            summary = inode_by_hex.get(ino_hex)
            if summary and summary.loner == int(cid):
                info.notes.append(f"client.{cid} is loner on this inode")

            for op in ops:
                if op.client_id != cid:
                    continue
                op_inos = {op.parent_ino_hex} if op.parent_ino_hex else set()
                op_inos.update(lk.get("ino_hex") for lk in op.held_locks if lk.get("ino_hex"))
                if ino_hex not in op_inos:
                    continue
                info.related_ops.append(op.reqid)
                initiated = op_initiated_at(op)
                if (
                    outstanding
                    and initiated
                    and info.last_issue_stamp
                    and initiated[:19] == info.last_issue_stamp[:19]
                ):
                    info.notes.append(
                        f"cap revoke issued at unlink start for op {op.reqid}"
                    )
                if (
                    outstanding
                    and "wrlock" in op.flag_point
                    and ino_hex == op.parent_ino_hex
                ):
                    info.notes.append(
                        f"same-client circular wait: op {op.reqid} waiting on "
                        f"parent scatter wrlock while cap revoke outstanding"
                    )

            if outstanding:
                cap_infos.append(info)

    correlations: List[Dict[str, Any]] = []
    for info in cap_infos:
        correlations.append({
            "ino_hex": info.ino_hex,
            "path": info.path,
            "client_id": info.client_id,
            "pending": info.pending,
            "issued": info.issued,
            "wanted": info.wanted,
            "revoked_caps": info.revoked_caps,
            "revokes": info.revokes,
            "last_issue_stamp": info.last_issue_stamp,
            "last_sent": info.last_sent,
            "related_ops": info.related_ops,
            "notes": info.notes,
        })

    return cap_infos, correlations


def apply_cap_revoke_notes(
    cap_infos: List[CapRevokeInfo],
    inode_by_hex: Dict[str, InodeSummary],
) -> None:
    for info in cap_infos:
        summary = inode_by_hex.get(info.ino_hex)
        if not summary:
            continue
        for note in info.notes:
            tagged = f"cap revoke: {note}"
            if tagged not in summary.notes:
                summary.notes.append(tagged)


def summarize_inode(ino_hex: str, dump: Dict[str, Any]) -> InodeSummary:
    lock_names = [
        "versionlock", "authlock", "linklock", "dirfragtreelock", "filelock",
        "xattrlock", "snaplock", "nestlock", "flocklock", "policylock", "quiescelock",
    ]
    locks = {}
    for name in lock_names:
        if name in dump and dump[name]:
            locks[name] = dump[name]

    ino_dec = dump.get("ino")
    return InodeSummary(
        ino_hex=ino_hex,
        ino_dec=int(ino_dec) if ino_dec is not None else None,
        path=dump.get("path") or "",
        locks=locks,
        client_caps=list(dump.get("client_caps") or []),
        loner=dump.get("loner"),
        want_loner=dump.get("want_loner"),
        pins=dict(dump.get("pins") or {}),
        quiesce_block=bool(dump.get("quiesce_block")),
    )


def analyze_inode(summary: InodeSummary, client_ids: Set[str]) -> List[str]:
    notes: List[str] = []
    filelock = summary.locks.get("filelock") or {}
    nestlock = summary.locks.get("nestlock") or {}
    quiesce = summary.locks.get("quiescelock") or {}

    fl_state = filelock.get("state")
    if fl_state in SUSPICIOUS_SCATTER_STATES:
        notes.append(f"filelock in transitional state {fl_state!r}")

    if quiesce.get("num_wrlocks", 0) > 0 and filelock.get("num_wrlocks", 0) == 0:
        notes.append("quiescelock wr held but filelock has no wrlock (typical mid-acquire scatter stall)")

    for cap in summary.client_caps:
        cid = str(cap.get("client_id"))
        if cid not in client_ids:
            continue
        pending = cap.get("pending") or ""
        issued = cap.get("issued") or ""
        if pending != issued:
            notes.append(
                f"client.{cid} caps pending downgrade ({pending} vs issued {issued})"
            )
        if summary.loner == int(cid) or summary.want_loner == int(cid):
            notes.append(f"client.{cid} is loner/want_loner on this inode")

    if summary.pins.get("waiter", 0) > 0:
        notes.append(f"waiter pin={summary.pins.get('waiter')}")
    if summary.quiesce_block:
        notes.append("quiesce_block=true")

    return notes


def analyze_op(
    op: OpSummary,
    inode_by_hex: Dict[str, InodeSummary],
) -> List[str]:
    notes: List[str] = []
    if "wrlock" in op.flag_point:
        notes.append(f"waiting on wrlock ({op.flag_point})")

    held_types = {(lk.get("lock_type"), lk.get("held_by_op")) for lk in op.held_locks}
    has_link_xlock = any(t == "ilink" and h == "xlock" for t, h in held_types)
    has_quiesce_wr = any(t == "iquiesce" and h == "wrlock" for t, h in held_types)

    if op.opcode in ("unlink", "rmdir") and has_quiesce_wr and not has_link_xlock:
        notes.append(
            "unlink/rmdir holds parent quiesce wr but not file linklock yet; "
            "likely stuck in path_traverse parent scatter wrlock (see MDCache.cc "
            "rdlock_path + xlock_dentry branch)"
        )

    parent = inode_by_hex.get(op.parent_ino_hex)
    if parent:
        for note in analyze_inode(parent, {op.client_id} if op.client_id else set()):
            notes.append(f"parent dir: {note}")
        for note in parent.notes:
            if note.startswith("cap revoke:"):
                notes.append(f"parent dir: {note[len('cap revoke:'):].lstrip()}")

    return notes


def add_path_hierarchy_edges(
    edges: List[Dict[str, Any]],
    inode_by_hex: Dict[str, InodeSummary],
) -> None:
    by_path: Dict[str, str] = {}
    for ino_hex, summary in inode_by_hex.items():
        if summary.path:
            by_path[summary.path.rstrip("/") or "/"] = ino_hex

    for ino_hex, summary in inode_by_hex.items():
        path = summary.path.rstrip("/") if summary.path else ""
        if not path or path == "/":
            continue
        parts = path.split("/")
        for i in range(1, len(parts)):
            parent_path = "/" + "/".join(parts[1:i]) if i > 1 else "/"
            parent_hex = by_path.get(parent_path)
            if parent_hex and parent_hex != ino_hex:
                edges.append({
                    "from": f"inode:{parent_hex}",
                    "to": f"inode:{ino_hex}",
                    "kind": "path_parent",
                })


def build_graph(
    ops: List[OpSummary],
    inode_by_hex: Dict[str, InodeSummary],
    sessions: Dict[str, Any],
    cap_infos: Optional[List[CapRevokeInfo]] = None,
    path_edges: bool = True,
) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    for op in ops:
        nodes.append({"id": f"op:{op.reqid}", "kind": "op", "label": op.reqid})
        if op.client_id:
            edges.append({
                "from": f"op:{op.reqid}",
                "to": f"client:{op.client_id}",
                "kind": "client",
            })
        for lk in op.held_locks:
            if not lk.get("ino_hex"):
                continue
            inode_id = f"inode:{lk['ino_hex']}"
            edges.append({
                "from": f"op:{op.reqid}",
                "to": inode_id,
                "kind": "holds",
                "lock_type": lk.get("lock_type"),
                "held_as": lk.get("held_by_op"),
                "lock_state": lk.get("lock_state"),
            })

    for ino_hex, summary in inode_by_hex.items():
        nodes.append({
            "id": f"inode:{ino_hex}",
            "kind": "inode",
            "label": summary.path or ino_hex,
            "notes": summary.notes,
        })
        for cap in summary.client_caps:
            cid = str(cap.get("client_id"))
            edges.append({
                "from": f"inode:{ino_hex}",
                "to": f"client:{cid}",
                "kind": "caps",
                "pending": cap.get("pending"),
                "issued": cap.get("issued"),
            })

    for info in cap_infos or []:
        edges.append({
            "from": f"client:{info.client_id}",
            "to": f"inode:{info.ino_hex}",
            "kind": "cap_revoke",
            "pending": info.pending,
            "issued": info.issued,
            "revoked_caps": info.revoked_caps,
            "last_issue_stamp": info.last_issue_stamp,
            "revokes": info.revokes,
            "related_ops": info.related_ops,
        })
        for reqid in info.related_ops:
            edges.append({
                "from": f"op:{reqid}",
                "to": f"inode:{info.ino_hex}",
                "kind": "blocked_by_cap_revoke",
                "client_id": info.client_id,
                "revoked_caps": info.revoked_caps,
            })

    for cid, sess in sessions.items():
        nodes.append({"id": f"client:{cid}", "kind": "client", "label": f"client.{cid}"})
        edges.append({
            "from": f"client:{cid}",
            "to": f"session:{cid}",
            "kind": "session",
            "state": sess.get("state"),
            "requests_in_flight": sess.get("requests_in_flight"),
            "num_caps": sess.get("num_caps"),
        })
        nodes.append({"id": f"session:{cid}", "kind": "session", "label": f"session client.{cid}"})

    # shared inode edges between ops
    inode_ops: Dict[str, List[str]] = defaultdict(list)
    for op in ops:
        for lk in op.held_locks:
            if lk.get("ino_hex"):
                inode_ops[lk["ino_hex"]].append(op.reqid)
    for ino_hex, reqids in inode_ops.items():
        if len(reqids) > 1:
            edges.append({
                "from": f"inode:{ino_hex}",
                "to": f"inode:{ino_hex}",
                "kind": "shared_by_ops",
                "ops": reqids,
            })

    if path_edges:
        add_path_hierarchy_edges(edges, inode_by_hex)

    return {"nodes": nodes, "edges": edges}


def write_text_report(
    path: Path,
    mds_target: str,
    ops: List[OpSummary],
    inode_by_hex: Dict[str, InodeSummary],
    sessions: Dict[str, Any],
    cap_correlations: Optional[List[Dict[str, Any]]] = None,
    blocked_count: Optional[int] = None,
    idle_diag: Optional[Dict[str, Any]] = None,
) -> None:
    lines: List[str] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines.append(f"MDS stall debug report for mds.{mds_target} at {now}")
    if blocked_count is not None:
        lines.append(f"Blocked ops (daemon count): {blocked_count}")
    if idle_diag:
        lines.append("Idle stall diagnostics: collected (0 blocked ops)")
    lines.append("")

    if idle_diag:
        lines.extend(format_idle_diag_report(idle_diag))
        lines.append("")

    lines.append("== Blocked / investigated ops ==")
    for op in ops:
        lines.append(f"- {op.reqid or op.description}")
        lines.append(f"  opcode={op.opcode} age={op.age_sec:.1f}s flag_point={op.flag_point!r}")
        if op.parent_ino_hex:
            lines.append(f"  path target: {op.parent_ino_hex}/{op.dentry_name}")
        for note in analyze_op(op, inode_by_hex):
            lines.append(f"  * {note}")
        if op.held_locks:
            lines.append("  held locks:")
            for lk in op.held_locks:
                lines.append(
                    "    - {ino} {type} held_as={held} state={state} "
                    "rd={rd} wr={wr} xl={xl}".format(
                        ino=lk.get("ino_hex") or "?",
                        type=lk.get("lock_type"),
                        held=lk.get("held_by_op"),
                        state=lk.get("lock_state"),
                        rd=lk.get("num_rdlocks"),
                        wr=lk.get("num_wrlocks"),
                        xl=lk.get("num_xlocks"),
                    )
                )
        if op.events:
            lines.append(f"  last event: {op.events[-1].get('event')} @ {op.events[-1].get('time')}")
        lines.append("")

    lines.append("== Inodes ==")
    for ino_hex in sorted(inode_by_hex.keys()):
        summary = inode_by_hex[ino_hex]
        lines.append(f"- {ino_hex} ({summary.ino_dec}) {summary.path}")
        for name, lock in summary.locks.items():
            lines.append(
                f"    {name}: state={lock.get('state')} "
                f"rd={lock.get('num_rdlocks', 0)} wr={lock.get('num_wrlocks', 0)} "
                f"xl={lock.get('num_xlocks', 0)}"
            )
        for cap in summary.client_caps:
            lines.append(
                f"    caps client.{cap.get('client_id')}: "
                f"pending={cap.get('pending')} issued={cap.get('issued')}"
            )
        if summary.loner is not None:
            lines.append(f"    loner={summary.loner} want_loner={summary.want_loner}")
        for note in summary.notes:
            lines.append(f"    * {note}")
        lines.append("")

    if cap_correlations:
        lines.append("== Cap revoke correlation ==")
        for entry in cap_correlations:
            lines.append(
                f"- {entry['ino_hex']} {entry.get('path') or ''} "
                f"client.{entry['client_id']}"
            )
            lines.append(
                f"    pending={entry['pending']} issued={entry['issued']} "
                f"wanted={entry['wanted']}"
            )
            if entry.get("revoked_caps"):
                lines.append(f"    revoked cap letters: {entry['revoked_caps']}")
            if entry.get("last_issue_stamp"):
                lines.append(f"    last_issue_stamp={entry['last_issue_stamp']}")
            if entry.get("revokes"):
                lines.append(f"    revokes={entry['revokes']}")
            if entry.get("related_ops"):
                lines.append(f"    related ops: {', '.join(entry['related_ops'])}")
            for note in entry.get("notes") or []:
                lines.append(f"    * {note}")
            lines.append("")

    if sessions:
        lines.append("== Client sessions ==")
        for cid, sess in sorted(sessions.items(), key=lambda x: int(x[0])):
            lines.append(
                f"- client.{cid}: state={sess.get('state')} "
                f"requests_in_flight={sess.get('requests_in_flight')} "
                f"num_caps={sess.get('num_caps')}"
            )
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def normalize_ops_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    if "ops" in payload and isinstance(payload["ops"], list):
        return payload["ops"]
    nested = payload.get("ops_in_flight")
    if isinstance(nested, dict) and isinstance(nested.get("ops"), list):
        return nested["ops"]
    return []


def normalize_historic_ops_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    hist = payload.get("op_history")
    if isinstance(hist, dict) and isinstance(hist.get("ops"), list):
        return hist["ops"]
    if isinstance(payload.get("ops"), list):
        return payload["ops"]
    return []


def count_in_flight_ops(payload: Any) -> int:
    if isinstance(payload, dict):
        if "num_ops" in payload:
            return int(payload["num_ops"])
        nested = payload.get("ops_in_flight")
        if isinstance(nested, dict):
            if "num_ops" in nested:
                return int(nested["num_ops"])
            return len(normalize_ops_payload(nested))
    return len(normalize_ops_payload(payload))


PERF_DISPATCH_KEYS = (
    "dispatch_queue_len",
    "dispatch_queue_len_max",
    "dispatch_inbound",
    "dispatch_io_completions",
    "dispatch_enqueue_usec",
    "dispatch_execute_usec",
    "dispatch_enqueue_usec_client",
    "dispatch_execute_usec_client",
    "dispatch_enqueue_usec_control",
    "dispatch_execute_usec_control",
    "dispatch_enqueue_usec_maintenance",
    "dispatch_execute_usec_maintenance",
    "dispatch_enqueue_usec_io",
    "dispatch_execute_usec_io",
    "req",
    "reply",
    "forward",
    "dir_update",
    "dir_split",
    "dir_merge",
    "slow_reqs",
)


IDLE_DIAG_ARTIFACTS = (
    "ops_in_flight.json",
    "historic_ops.json",
    "sessions_all_caps.json",
    "perf_dump.json",
    "perf_dispatch_summary.json",
    "status.json",
    "idle_summary.json",
)

PROMETHEUS_TIMESERIES_ARTIFACT = "prometheus_timeseries.json"
CLIENT_DEBUGFS_DIR = "client_debugfs"
CLIENT_DEBUGFS_SUMMARY = "client_debugfs_summary.json"
CLIENT_DEBUGFS_DEFAULT_FILES = ("mdsc", "mds_sessions", "osdc", "caps")
CLIENT_DEBUGFS_DIR_RE = re.compile(
    r"^(?P<fsid>[0-9a-fA-F-]{36})\.client(?P<client_id>\d+)$"
)
MDSC_LINE_RE = re.compile(
    r"^(?P<tid>\d+)\t"
    r"(?P<mds>mds\d+|\(no request\)|\(no session\))\t"
    r"(?P<op>\S+)"
    r"(?:\t(?P<unsafe>\(unsafe\))?)?"
    r"(?:\t(?P<rest>.*))?$"
)


def invoke_optional(invoke, cmd: List[str]) -> Tuple[Any, Optional[str]]:
    try:
        return invoke(cmd), None
    except RuntimeError as exc:
        return {}, str(exc)


def session_ls_cmd(*, client_id: Optional[str] = None, cap_dump: bool = False) -> List[str]:
    cmd = ["session", "ls"]
    if client_id is not None:
        cmd.append(f"--filters={client_id}")
    if cap_dump:
        cmd.append("--cap_dump")
    return cmd


def invoke_session_ls(
    invoke,
    *,
    client_id: Optional[str] = None,
    cap_dump: bool = False,
) -> Any:
    try:
        return invoke(session_ls_cmd(client_id=client_id, cap_dump=cap_dump))
    except RuntimeError:
        if client_id is None:
            raise
        cmd = ["session", "ls", client_id]
        if cap_dump:
            cmd.append("--cap_dump")
        return invoke(cmd)


def extract_mds_perf_summary(perf_dump: Any) -> Dict[str, Any]:
    if not isinstance(perf_dump, dict):
        return {}
    mds = perf_dump.get("mds")
    if not isinstance(mds, dict):
        return {}
    return {key: mds[key] for key in PERF_DISPATCH_KEYS if key in mds}


def summarize_sessions_idle(payload: Any) -> Dict[str, Any]:
    sessions = normalize_sessions_payload(payload)
    by_id: Dict[str, Dict[str, Any]] = {}
    total_requests_in_flight = 0
    clients_with_requests = 0
    outstanding_revokes = 0
    caps_pending_downgrade = 0

    for sess in sessions:
        cid = session_client_id(sess)
        if cid:
            by_id[cid] = sess
        reqs = int(sess.get("requests_in_flight") or 0)
        total_requests_in_flight += reqs
        if reqs:
            clients_with_requests += 1
        for cap in extract_session_caps(sess):
            revokes = cap.get("revokes") or []
            if isinstance(revokes, list):
                outstanding_revokes += len(revokes)
            pending = cap.get("pending") or ""
            issued = cap.get("issued") or ""
            if issued and pending != issued:
                caps_pending_downgrade += 1

    return {
        "session_count": len(sessions),
        "clients_with_requests_in_flight": clients_with_requests,
        "total_requests_in_flight": total_requests_in_flight,
        "outstanding_cap_revokes": outstanding_revokes,
        "caps_pending_downgrade": caps_pending_downgrade,
        "sessions_by_id": by_id,
    }


def summarize_idle_diagnostics(
    *,
    ops_in_flight: Any,
    historic_ops: Any,
    sessions_payload: Any,
    perf_dump: Any,
    status: Any,
    errors: Dict[str, str],
    session_cap_dump: bool = False,
) -> Dict[str, Any]:
    in_flight_list = normalize_ops_payload(ops_in_flight)
    historic_list = normalize_historic_ops_payload(historic_ops)
    session_summary = summarize_sessions_idle(sessions_payload)
    perf_summary = extract_mds_perf_summary(perf_dump)

    recent_ops: List[Dict[str, Any]] = []
    for op in historic_list[:8]:
        if not isinstance(op, dict):
            continue
        td = op.get("type_data") or {}
        recent_ops.append(
            {
                "description": op.get("description") or td.get("description") or "",
                "duration": op.get("duration"),
                "age": op.get("age"),
                "flag_point": td.get("flag_point"),
            }
        )

    status_summary: Dict[str, Any] = {}
    if isinstance(status, dict):
        for key in ("state", "rank", "incarnation", "standby_replay"):
            if key in status:
                status_summary[key] = status[key]

    return {
        "num_ops_in_flight": count_in_flight_ops(ops_in_flight),
        "in_flight_ops_sample": [
            {
                "description": op.get("description"),
                "age": op.get("age"),
                "flag_point": (op.get("type_data") or {}).get("flag_point"),
            }
            for op in in_flight_list[:8]
            if isinstance(op, dict)
        ],
        "historic_ops_count": len(historic_list),
        "recent_historic_ops": recent_ops,
        "sessions": {
            k: v
            for k, v in session_summary.items()
            if k != "sessions_by_id"
        },
        "perf_dispatch": perf_summary,
        "status": status_summary,
        "session_cap_dump": session_cap_dump,
        "collection_errors": errors,
    }


def format_idle_diag_report(idle_diag: Dict[str, Any]) -> List[str]:
    lines = ["== Idle stall diagnostics (0 blocked ops) =="]
    lines.append(
        f"In-flight ops (all ages): {idle_diag.get('num_ops_in_flight', 0)}"
    )
    lines.append(
        f"Recent historic ops: {idle_diag.get('historic_ops_count', 0)}"
    )

    sessions = idle_diag.get("sessions") or {}
    session_cap_dump = idle_diag.get("session_cap_dump", False)
    if session_cap_dump:
        lines.append(
            "Sessions: {count} total, {req_clients} with requests_in_flight "
            "({req_total} total), {revokes} outstanding cap revokes, "
            "{pending} caps with pending!=issued".format(
                count=sessions.get("session_count", 0),
                req_clients=sessions.get("clients_with_requests_in_flight", 0),
                req_total=sessions.get("total_requests_in_flight", 0),
                revokes=sessions.get("outstanding_cap_revokes", 0),
                pending=sessions.get("caps_pending_downgrade", 0),
            )
        )
    else:
        lines.append(
            "Sessions: {count} total, {req_clients} with requests_in_flight "
            "({req_total} total) (cap revoke counts require --session-cap-dump)".format(
                count=sessions.get("session_count", 0),
                req_clients=sessions.get("clients_with_requests_in_flight", 0),
                req_total=sessions.get("total_requests_in_flight", 0),
            )
        )

    perf = idle_diag.get("perf_dispatch") or {}
    if perf:
        perf_bits = []
        for key in (
            "dispatch_queue_len",
            "dispatch_queue_len_max",
            "dispatch_inbound",
            "dispatch_io_completions",
        ):
            if key in perf:
                perf_bits.append(f"{key}={perf[key]}")
        if perf_bits:
            lines.append("Dispatch perf: " + ", ".join(perf_bits))

    status = idle_diag.get("status") or {}
    if status:
        lines.append(
            "MDS status: "
            + ", ".join(f"{k}={v}" for k, v in sorted(status.items()))
        )

    for op in idle_diag.get("recent_historic_ops") or []:
        desc = op.get("description") or "?"
        duration = op.get("duration")
        if duration is not None:
            lines.append(f"  recent slow/historic: {desc} duration={duration}s")
        else:
            lines.append(f"  recent historic: {desc}")

    errors = idle_diag.get("collection_errors") or {}
    if errors:
        lines.append("Collection errors:")
        for name, err in sorted(errors.items()):
            lines.append(f"  - {name}: {err}")

    lines.append(
        "Note: no blocked/long-running MDS ops were found. Clients may have "
        "stopped sending metadata requests, or be stuck client-side. Inspect "
        "session caps, client stacks, and dispatch queue metrics."
    )
    return lines


def should_collect_idle_diagnostics(
    skip: bool,
    blocked_count: Optional[int],
    blocked_ops: List[Dict[str, Any]],
) -> bool:
    if skip:
        return False
    if blocked_count == 0:
        return True
    return blocked_count is None and not blocked_ops


def write_json_artifact(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def collect_idle_stall_diagnostics(
    invoke,
    output_dir: Path,
    *,
    session_cap_dump: bool = False,
) -> Dict[str, Any]:
    """Collect extra artifacts when dump_blocked_ops is empty."""
    errors: Dict[str, str] = {}

    ops_in_flight, err = invoke_optional(invoke, ["dump_ops_in_flight"])
    if err:
        errors["dump_ops_in_flight"] = err
    write_json_artifact(output_dir / "ops_in_flight.json", ops_in_flight)

    historic_ops, err = invoke_optional(invoke, ["dump_historic_ops_by_duration"])
    if err:
        errors["dump_historic_ops_by_duration"] = err
    write_json_artifact(output_dir / "historic_ops.json", historic_ops)

    sessions_payload, err = invoke_optional(
        invoke, session_ls_cmd(cap_dump=session_cap_dump),
    )
    if err:
        errors["session_ls"] = err
    write_json_artifact(output_dir / "sessions_all_caps.json", sessions_payload)

    perf_dump, err = invoke_optional(invoke, ["perf", "dump"])
    if err:
        errors["perf_dump"] = err
    write_json_artifact(output_dir / "perf_dump.json", perf_dump)
    perf_summary = extract_mds_perf_summary(perf_dump)
    write_json_artifact(output_dir / "perf_dispatch_summary.json", perf_summary)

    status, err = invoke_optional(invoke, ["status"])
    if err:
        errors["status"] = err
    write_json_artifact(output_dir / "status.json", status)

    summary = summarize_idle_diagnostics(
        ops_in_flight=ops_in_flight,
        historic_ops=historic_ops,
        sessions_payload=sessions_payload,
        perf_dump=perf_dump,
        status=status,
        errors=errors,
        session_cap_dump=session_cap_dump,
    )
    write_json_artifact(output_dir / "idle_summary.json", summary)
    return summary


def load_idle_diagnostics_from_dir(
    input_dir: Path,
    output_dir: Path,
) -> Optional[Dict[str, Any]]:
    summary_path = input_dir / "idle_summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        for name in IDLE_DIAG_ARTIFACTS:
            src = input_dir / name
            if src.exists():
                (output_dir / name).write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        return summary

    payloads: Dict[str, Any] = {}
    for name in IDLE_DIAG_ARTIFACTS:
        src = input_dir / name
        if not src.exists():
            continue
        payloads[name] = json.loads(src.read_text(encoding="utf-8"))
        (output_dir / name).write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    if not payloads:
        return None

    sessions_payload = payloads.get("sessions_all_caps.json", {})
    session_cap_dump = sessions_payload_has_cap_dump(sessions_payload)
    errors: Dict[str, str] = {}
    return summarize_idle_diagnostics(
        ops_in_flight=payloads.get("ops_in_flight.json", {}),
        historic_ops=payloads.get("historic_ops.json", {}),
        sessions_payload=sessions_payload,
        perf_dump=payloads.get("perf_dump.json", {}),
        status=payloads.get("status.json", {}),
        errors=errors,
        session_cap_dump=session_cap_dump,
    )


def merge_idle_sessions(
    sessions: Dict[str, Any],
    idle_diag: Optional[Dict[str, Any]],
    sessions_payload: Any,
) -> Dict[str, Any]:
    merged = dict(sessions)
    if isinstance(sessions_payload, dict):
        for cid, sess in summarize_sessions_idle(sessions_payload).get(
            "sessions_by_id", {}
        ).items():
            merged.setdefault(cid, sess)
    return merged


@dataclass
class LogExtractWindow:
    stall_time: datetime
    start: datetime
    end: datetime
    source_times: List[str] = field(default_factory=list)
    blocked_ops: int = 0
    last_activity: Optional[datetime] = None
    fallback_used: bool = False
    fallback_reason: Optional[str] = None


def parse_ceph_timestamp(raw: str) -> datetime:
    """Parse Ceph log / op-history timestamps into UTC-aware datetimes."""
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    elif len(text) >= 5 and text[-5] in "+-" and text[-4:].isdigit():
        if ":" not in text[-6:]:
            text = text[:-2] + ":" + text[-2:]
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_ceph_timestamp(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}+0000"


def parse_log_line_timestamp(line: bytes) -> Optional[datetime]:
    m = LOG_LINE_TS_RE.match(line)
    if not m:
        return None
    return parse_ceph_timestamp(m.group(1).decode("ascii") + m.group(2).decode("ascii"))


def parse_stall_dir_timestamp(stall_dir: Path) -> Optional[datetime]:
    """Parse the collection timestamp embedded in mds-stall-*-YYYYMMDD-HHMMSS."""
    m = STALL_DIR_TS_RE.search(stall_dir.name)
    if not m:
        return None
    return datetime.strptime(
        f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S",
    ).replace(tzinfo=timezone.utc)


def append_op_event_times(
    ops: Iterable[Any],
    event_times: List[datetime],
    source_times: List[str],
    label: str,
) -> None:
    for op in ops:
        if not isinstance(op, dict):
            continue
        td = op.get("type_data") or {}
        for ev in td.get("events") or []:
            ev_time = ev.get("time")
            if ev_time:
                source_times.append(f"{label}:event:{ev_time}")
                event_times.append(parse_ceph_timestamp(ev_time))


def append_op_history_times(
    ops: Iterable[Any],
    times: List[datetime],
    source_times: List[str],
    label: str,
) -> None:
    for op in ops:
        if not isinstance(op, dict):
            continue
        initiated_raw = op.get("initiated_at")
        initiated: Optional[datetime] = None
        if initiated_raw:
            initiated = parse_ceph_timestamp(initiated_raw)
            source_times.append(f"{label}:initiated:{initiated_raw}")
            times.append(initiated)
        age = op.get("age")
        if initiated is not None and age is not None:
            end_guess = initiated + timedelta(seconds=float(age))
            source_times.append(
                f"{label}:initiated+age:{format_ceph_timestamp(end_guess)}"
            )
            times.append(end_guess)
        td = op.get("type_data") or {}
        for ev in td.get("events") or []:
            ev_time = ev.get("time")
            if ev_time:
                source_times.append(f"{label}:event:{ev_time}")
                times.append(parse_ceph_timestamp(ev_time))


def collect_stall_times(stall_dir: Path) -> LogExtractWindow:
    """Derive a log time window from saved stall artifacts."""
    times: List[datetime] = []
    event_times: List[datetime] = []
    source_times: List[str] = []
    blocked_ops = 0
    report_header_time: Optional[datetime] = None

    dump_ts = parse_stall_dir_timestamp(stall_dir)
    if dump_ts is not None:
        source_times.append(f"stall_dir:{format_ceph_timestamp(dump_ts)}")
        times.append(dump_ts)

    report_path = stall_dir / "report.txt"
    if report_path.exists():
        report_lines = report_path.read_text(encoding="utf-8").splitlines()
        if report_lines:
            m = REPORT_STALL_TIME_RE.search(report_lines[0])
            if m:
                report_header_time = parse_ceph_timestamp(m.group(1))
                source_times.append(f"report:{m.group(1)}")
                times.append(report_header_time)
        for line in report_lines:
            m = REPORT_EVENT_TIME_RE.search(line)
            if m:
                source_times.append(f"report_event:{m.group(1)}")
                ev_ts = parse_ceph_timestamp(m.group(1))
                times.append(ev_ts)
                event_times.append(ev_ts)

    blocked_path = stall_dir / "blocked_ops.json"
    if blocked_path.exists():
        payload = json.loads(blocked_path.read_text(encoding="utf-8"))
        ops = normalize_ops_payload(payload)
        blocked_ops = len(ops)
        append_op_history_times(ops, times, source_times, "blocked")
        append_op_event_times(ops, event_times, source_times, "blocked")

    historic_path = stall_dir / "historic_ops.json"
    if historic_path.exists():
        payload = json.loads(historic_path.read_text(encoding="utf-8"))
        historic_ops = normalize_historic_ops_payload(payload)
        append_op_event_times(historic_ops, event_times, source_times, "historic")
        if blocked_ops > 0:
            append_op_history_times(historic_ops, times, source_times, "historic")

    in_flight_path = stall_dir / "ops_in_flight.json"
    if in_flight_path.exists():
        payload = json.loads(in_flight_path.read_text(encoding="utf-8"))
        in_flight_ops = normalize_ops_payload(payload)
        append_op_event_times(in_flight_ops, event_times, source_times, "in_flight")
        if blocked_ops > 0:
            append_op_history_times(in_flight_ops, times, source_times, "in_flight")

    if not times and not event_times:
        raise RuntimeError(
            f"could not derive stall timestamps from {stall_dir} "
            "(need stall dir timestamp, report.txt, blocked_ops.json, "
            "historic_ops.json, and/or ops_in_flight.json)"
        )

    last_activity: Optional[datetime] = max(event_times) if event_times else None

    if blocked_ops > 0:
        stall_time = max(times)
        start = min(times)
        end = stall_time
    else:
        # report.txt header is collection time, not stall onset; prefer dump dir time.
        if dump_ts is not None:
            stall_time = dump_ts
        elif last_activity is not None:
            stall_time = last_activity
        else:
            stall_time = report_header_time or max(times)
        if last_activity is not None:
            # Anchor on last client-visible activity, not oldest historic initiated_at.
            start = last_activity
            end = last_activity
        else:
            start = stall_time
            end = stall_time

    return LogExtractWindow(
        stall_time=stall_time,
        start=start,
        end=end,
        source_times=source_times,
        blocked_ops=blocked_ops,
        last_activity=last_activity,
    )


def bracket_log_window(
    window: LogExtractWindow,
    padding_before: int,
    padding_after: int,
    *,
    idle_before: Optional[int] = None,
) -> LogExtractWindow:
    if window.blocked_ops == 0 and window.last_activity is not None:
        before = idle_before if idle_before is not None else padding_before
    else:
        before = padding_before
    return LogExtractWindow(
        stall_time=window.stall_time,
        start=window.start - timedelta(seconds=before),
        end=window.end + timedelta(seconds=padding_after),
        source_times=window.source_times,
        blocked_ops=window.blocked_ops,
        last_activity=window.last_activity,
        fallback_used=window.fallback_used,
        fallback_reason=window.fallback_reason,
    )


def clamp_window_to_log(
    window: LogExtractWindow,
    first_ts: Optional[datetime],
    last_ts: Optional[datetime],
) -> LogExtractWindow:
    start = window.start
    end = window.end
    if first_ts is not None and start < first_ts:
        start = first_ts
    if last_ts is not None and end > last_ts:
        end = last_ts
    if end < start:
        end = start
    return LogExtractWindow(
        stall_time=window.stall_time,
        start=start,
        end=end,
        source_times=window.source_times,
        blocked_ops=window.blocked_ops,
        last_activity=window.last_activity,
        fallback_used=window.fallback_used,
        fallback_reason=window.fallback_reason,
    )


def parse_prometheus_step(step: str) -> int:
    text = step.strip().lower()
    m = re.fullmatch(r"(\d+)(s|m|h)?", text)
    if not m:
        raise ValueError(f"invalid prometheus step: {step!r}")
    value = int(m.group(1))
    unit = m.group(2) or "s"
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    return value * 3600


def promql_re_escape(text: str) -> str:
    """Escape a literal for a PromQL double-quoted regex matcher (~=\"...\").

    PromQL unescapes the quotes before RE2 sees the pattern. In that string
    only a few escapes are legal (\\\\, \\\", \\n, ...); \\. and \\- are
    rejected as unknown escapes (\"unknown escape sequence U+002E '.'\").
    Hyphen is not special in RE2 outside a character class, so leave it
    alone. Other regex metacharacters are escaped, then every backslash is
    doubled so the string value is valid RE2 (a '.' becomes '\\\\.' in the
    query text). Quotes are escaped for the PromQL string, not for RE2.
    """
    escaped = re.sub(r"([\\.^$|?*+()\[\]{}])", r"\\\1", text)
    escaped = escaped.replace("\\", "\\\\")
    return escaped.replace('"', '\\"')


def promql_quote(text: str) -> str:
    """Escape a literal for a PromQL double-quoted exact matcher."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def infer_mds_daemon_regex(mds_target: str) -> str:
    text = str(mds_target)
    if text.endswith(".asok"):
        text = Path(text).stem
    elif "/" in text:
        text = Path(text).name
        if text.endswith(".asok"):
            text = text[:-5]
    # asok files are ceph-mds.<name>.asok. ceph-exporter's ceph_daemon label
    # is mds.<name> (no "ceph-" prefix). Match either.
    if text.startswith("ceph-mds."):
        text = text[len("ceph-"):]
    if text.startswith("mds."):
        rest = text[len("mds."):]
        return "(ceph-)?mds\\\\." + promql_re_escape(rest)
    if re.fullmatch(r"[^:]+:\d+", text):
        return "(ceph-)?mds\\\\..*"
    return promql_re_escape(text)


def infer_cluster_label(status_payload: Any) -> Optional[str]:
    if not isinstance(status_payload, dict):
        return None
    for key in ("cluster", "cluster_name", "fs"):
        value = status_payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def build_prometheus_selector(
    daemon_re: str,
    cluster: Optional[str] = None,
) -> str:
    labels = [f'ceph_daemon=~"{daemon_re}"']
    if cluster:
        labels.append(f'cluster="{promql_quote(cluster)}"')
    return "{" + ",".join(labels) + "}"


def build_prometheus_queries(selector: str) -> Dict[str, str]:
    rate_iv = "30s"

    def counter_rate(metric: str) -> str:
        return f"sum(rate({metric}{selector}[{rate_iv}]))"

    def time_avg(prefix: str) -> str:
        return (
            f"sum(rate({prefix}_sum{selector}[{rate_iv}])) / "
            f"sum(rate({prefix}_count{selector}[{rate_iv}]))"
        )

    return {
        "dispatch_queue_len": f"max({('ceph_mds_dispatch_queue_len' + selector)})",
        "dispatch_queue_len_max": f"max({('ceph_mds_dispatch_queue_len_max' + selector)})",
        "dispatch_inbound_rate": counter_rate("ceph_mds_dispatch_inbound"),
        "dispatch_io_completions_rate": counter_rate("ceph_mds_dispatch_io_completions"),
        "handle_client_request_rate": counter_rate(
            "ceph_mds_server_handle_client_request"
        ),
        "handle_client_caps_rate": counter_rate("ceph_mds_handle_client_caps"),
        "reply_rate": counter_rate("ceph_mds_reply"),
        "dispatch_enqueue_usec": time_avg("ceph_mds_dispatch_enqueue_usec"),
        "dispatch_execute_usec": time_avg("ceph_mds_dispatch_execute_usec"),
        "dispatch_enqueue_usec_client": time_avg(
            "ceph_mds_dispatch_enqueue_usec_client"
        ),
        "dispatch_execute_usec_client": time_avg(
            "ceph_mds_dispatch_execute_usec_client"
        ),
        "dispatch_enqueue_usec_control": time_avg(
            "ceph_mds_dispatch_enqueue_usec_control"
        ),
        "dispatch_execute_usec_control": time_avg(
            "ceph_mds_dispatch_execute_usec_control"
        ),
        "dispatch_enqueue_usec_maintenance": time_avg(
            "ceph_mds_dispatch_enqueue_usec_maintenance"
        ),
        "dispatch_execute_usec_maintenance": time_avg(
            "ceph_mds_dispatch_execute_usec_maintenance"
        ),
        "dispatch_enqueue_usec_io": time_avg("ceph_mds_dispatch_enqueue_usec_io"),
        "dispatch_execute_usec_io": time_avg("ceph_mds_dispatch_execute_usec_io"),
        "cache_trim_throttle_rate": counter_rate("ceph_mds_cache_cache_trim_throttle"),
        "mem_heap": f"max({('ceph_mds_mem_heap' + selector)})",
        "mem_cap": f"max({('ceph_mds_mem_cap' + selector)})",
        "caps": f"max({('ceph_mds_caps' + selector)})",
    }


def derive_metrics_window(
    stall_dir: Path,
    *,
    padding_before: int,
    padding_after: int,
    idle_before: int,
) -> LogExtractWindow:
    try:
        window = collect_stall_times(stall_dir)
    except RuntimeError:
        # No MDS stall artifacts yet (common when asok is wedged): use "now".
        now = datetime.now(timezone.utc)
        window = LogExtractWindow(
            stall_time=now,
            start=now,
            end=now,
            source_times=[f"fallback_now:{format_ceph_timestamp(now)}"],
            blocked_ops=0,
            last_activity=now,
            fallback_used=True,
            fallback_reason=(
                "no stall timestamps available; using current time as metrics window"
            ),
        )
    return bracket_log_window(
        window,
        padding_before,
        padding_after,
        idle_before=idle_before,
    )


class PrometheusClient:
    def __init__(
        self,
        base_url: str,
        *,
        auth: Optional[Tuple[str, str]] = None,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.timeout = timeout

    def _request_json(self, path: str, params: Dict[str, str]) -> Any:
        query = urllib.parse.urlencode(params)
        url = f"{self.base_url}{path}?{query}"
        headers = {"Accept": "application/json"}
        req = urllib.request.Request(url, headers=headers, method="GET")
        if self.auth:
            user, password = self.auth
            token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"HTTP {exc.code} from {url}: {detail[:500]}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"request failed for {url}: {exc}") from exc
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON from {url}: {body[:200]!r}") from exc
        if payload.get("status") != "success":
            raise RuntimeError(
                f"prometheus query failed for {url}: "
                f"{payload.get('errorType')}: {payload.get('error')}"
            )
        return payload

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> Any:
        if end <= start:
            end = start + timedelta(seconds=step_seconds)
        params = {
            "query": query,
            "start": f"{start.timestamp():.3f}",
            "end": f"{end.timestamp():.3f}",
            "step": str(step_seconds),
        }
        return self._request_json("/api/v1/query_range", params)


def resolve_prometheus_client(
    *,
    grafana_url: Optional[str],
    prometheus_url: Optional[str],
    auth: Optional[Tuple[str, str]] = None,
    timeout: int = 60,
) -> Tuple[PrometheusClient, str]:
    if prometheus_url:
        base = prometheus_url.rstrip("/")
        return PrometheusClient(base, auth=auth, timeout=timeout), base

    if not grafana_url:
        raise RuntimeError("one of --grafana-url or --prometheus-url is required")

    grafana_base = grafana_url.rstrip("/")
    headers = {"Accept": "application/json"}
    req = urllib.request.Request(
        f"{grafana_base}/api/datasources",
        headers=headers,
        method="GET",
    )
    if auth:
        user, password = auth
        token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            datasources = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"failed to list Grafana datasources (HTTP {exc.code}): {detail[:500]}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"failed to reach Grafana at {grafana_base}: {exc}"
        ) from exc

    for ds in datasources:
        if ds.get("type") != "prometheus":
            continue
        ds_id = ds.get("id")
        if ds_id is None:
            continue
        proxy_base = f"{grafana_base}/api/datasources/proxy/{ds_id}"
        return PrometheusClient(proxy_base, auth=auth, timeout=timeout), proxy_base

    raise RuntimeError(
        f"no Prometheus datasource found in Grafana at {grafana_base}"
    )


def prometheus_point_count(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    data = payload.get("data") or {}
    total = 0
    for series in data.get("result") or []:
        total += len(series.get("values") or [])
    return total


# ceph-exporter stops scraping a wedged MDS asok, so the stall window itself
# is often after the last sample. Look this far back before giving up.
PROMETHEUS_EMPTY_LOOKBACK = timedelta(hours=12)


def collect_prometheus_timeseries(
    *,
    stall_dir: Path,
    mds_target: str,
    grafana_url: Optional[str] = None,
    prometheus_url: Optional[str] = None,
    auth: Optional[Tuple[str, str]] = None,
    padding_before: int = 60,
    padding_after: int = 120,
    idle_before: int = 600,
    step: str = "15s",
    timeout: int = 60,
) -> Dict[str, Any]:
    window = derive_metrics_window(
        stall_dir,
        padding_before=padding_before,
        padding_after=padding_after,
        idle_before=idle_before,
    )
    step_seconds = parse_prometheus_step(step)
    status_payload: Any = {}
    status_path = stall_dir / "status.json"
    if status_path.exists():
        status_payload = json.loads(status_path.read_text(encoding="utf-8"))

    daemon_re = infer_mds_daemon_regex(mds_target)
    cluster = infer_cluster_label(status_payload)
    selector = build_prometheus_selector(daemon_re, cluster)
    queries = build_prometheus_queries(selector)
    client, resolved_url = resolve_prometheus_client(
        grafana_url=grafana_url,
        prometheus_url=prometheus_url,
        auth=auth,
        timeout=timeout,
    )

    series: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    query_start = window.start
    window_note: Optional[str] = None
    probe_name = "caps" if "caps" in queries else next(iter(queries), None)
    if probe_name is not None:
        try:
            probe = client.query_range(
                queries[probe_name], query_start, window.end, step_seconds,
            )
            if prometheus_point_count(probe) == 0:
                expanded = window.end - PROMETHEUS_EMPTY_LOOKBACK
                if expanded < query_start:
                    probe = client.query_range(
                        queries[probe_name], expanded, window.end, step_seconds,
                    )
                    if prometheus_point_count(probe) > 0:
                        query_start = expanded
                        window_note = (
                            "stall window had no Prometheus samples; "
                            "ceph-exporter often stops scraping a wedged MDS. "
                            "Expanded start by "
                            f"{int(PROMETHEUS_EMPTY_LOOKBACK.total_seconds() // 3600)}h."
                        )
        except RuntimeError:
            pass

    for name, query in queries.items():
        try:
            series[name] = client.query_range(
                query, query_start, window.end, step_seconds,
            )
        except RuntimeError as exc:
            errors[name] = str(exc)
            series[name] = {"status": "error", "error": str(exc), "query": query}

    points = sum(prometheus_point_count(item) for item in series.values())
    with_samples = sum(
        1 for item in series.values() if prometheus_point_count(item) > 0
    )

    return {
        "grafana_url": grafana_url,
        "prometheus_url": resolved_url,
        "mds_target": mds_target,
        "daemon_filter": daemon_re,
        "cluster": cluster,
        "window": {
            "start": format_ceph_timestamp(query_start),
            "end": format_ceph_timestamp(window.end),
            "requested_start": format_ceph_timestamp(window.start),
            "stall_time": format_ceph_timestamp(window.stall_time),
            "blocked_ops": window.blocked_ops,
            "last_activity": (
                format_ceph_timestamp(window.last_activity)
                if window.last_activity is not None
                else None
            ),
            "step": step,
            "note": window_note,
            "series_with_samples": with_samples,
            "points": points,
        },
        "queries": queries,
        "series": series,
        "errors": errors,
    }


def maybe_collect_prometheus_timeseries(
    *,
    stall_dir: Path,
    mds_target: str,
    grafana_url: Optional[str],
    prometheus_url: Optional[str],
    grafana_auth: Optional[Tuple[str, str]],
    padding_before: int,
    padding_after: int,
    idle_before: int,
    step: str,
    timeout: int,
) -> Optional[Dict[str, Any]]:
    if not grafana_url and not prometheus_url:
        return None
    try:
        payload = collect_prometheus_timeseries(
            stall_dir=stall_dir,
            mds_target=mds_target,
            grafana_url=grafana_url,
            prometheus_url=prometheus_url,
            auth=grafana_auth,
            padding_before=padding_before,
            padding_after=padding_after,
            idle_before=idle_before,
            step=step,
            timeout=timeout,
        )
    except Exception as exc:
        payload = {
            "grafana_url": grafana_url,
            "prometheus_url": prometheus_url,
            "mds_target": mds_target,
            "errors": {"collect": str(exc)},
            "series": {},
            "queries": {},
        }
        print(f"warning: prometheus/grafana collection failed: {exc}", file=sys.stderr)
    write_json_artifact(stall_dir / PROMETHEUS_TIMESERIES_ARTIFACT, payload)
    return payload


def format_prometheus_timeseries_report(payload: Dict[str, Any]) -> List[str]:
    lines = ["== Prometheus time series =="]
    window = payload.get("window") or {}
    lines.append(
        "Window: {start} .. {end} (step {step})".format(
            start=window.get("start", "?"),
            end=window.get("end", "?"),
            step=window.get("step", "?"),
        )
    )
    lines.append(
        "Prometheus: {url}".format(url=payload.get("prometheus_url", "?"))
    )
    lines.append(
        "Daemon filter: {daemon}".format(daemon=payload.get("daemon_filter", "?"))
    )
    window_note = window.get("note")
    if window_note:
        lines.append(f"Note: {window_note}")
    series = payload.get("series") or {}
    with_samples = window.get("series_with_samples")
    if with_samples is None:
        with_samples = sum(
            1 for item in series.values() if prometheus_point_count(item) > 0
        )
    lines.append(
        f"Series with samples: {with_samples}/{len(series)}"
    )
    errors = payload.get("errors") or {}
    if errors:
        lines.append(f"Query errors: {len(errors)}")
        for name, err in sorted(errors.items()):
            lines.append(f"  - {name}: {err}")
    return lines


def append_prometheus_report_section(
    report_path: Path,
    payload: Dict[str, Any],
) -> None:
    if not report_path.exists():
        return
    section = format_prometheus_timeseries_report(payload)
    with report_path.open("a", encoding="utf-8") as handle:
        handle.write("\n")
        handle.write("\n".join(section))
        handle.write("\n")


def infer_mds_target_from_stall_dir(stall_dir: Path, fallback: str) -> str:
    report_path = stall_dir / "report.txt"
    if report_path.exists():
        first_line = report_path.read_text(encoding="utf-8").splitlines()[:1]
        if first_line:
            m = re.search(
                r"MDS stall debug report for mds\.(.+) at ",
                first_line[0],
            )
            if m:
                return m.group(1)
    return fallback


def parse_mdsc_text(text: str) -> Dict[str, Any]:
    requests: List[Dict[str, Any]] = []
    ops: Dict[str, int] = defaultdict(int)
    mds_targets: Dict[str, int] = defaultdict(int)
    unsafe_count = 0
    for line in text.splitlines():
        line = line.rstrip()
        if not line:
            continue
        m = MDSC_LINE_RE.match(line)
        if not m:
            requests.append({"raw": line, "parse_error": True})
            continue
        entry = {
            "tid": int(m.group("tid")),
            "mds": m.group("mds"),
            "op": m.group("op"),
            "unsafe": bool(m.group("unsafe")),
            "detail": (m.group("rest") or "").strip(),
            "raw": line,
        }
        requests.append(entry)
        ops[entry["op"]] += 1
        mds_targets[entry["mds"]] += 1
        if entry["unsafe"]:
            unsafe_count += 1
    return {
        "request_count": len(requests),
        "unsafe_count": unsafe_count,
        "ops": dict(sorted(ops.items())),
        "mds_targets": dict(sorted(mds_targets.items())),
        "requests": requests,
    }


def parse_mds_sessions_text(text: str) -> Dict[str, Any]:
    info: Dict[str, Any] = {"raw_lines": text.splitlines()}
    for line in text.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].rstrip(":"), parts[1].strip().strip('"')
        if key in ("id", "global_id", "client_id", "name"):
            info[key] = value
    return info


def read_text_file_local(path: Path, timeout: int) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except PermissionError:
        proc = subprocess.run(
            ["sudo", "cat", str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            err = proc.stderr.strip() or proc.stdout.strip() or "permission denied"
            raise RuntimeError(f"failed to read {path}: {err}")
        return proc.stdout
    except OSError as exc:
        raise RuntimeError(f"failed to read {path}: {exc}") from exc


def list_client_debugfs_dirs_local(root: Path) -> List[Path]:
    if not root.is_dir():
        return []
    dirs: List[Path] = []
    try:
        entries = list(root.iterdir())
    except PermissionError:
        proc = subprocess.run(
            ["sudo", "ls", "-1", str(root)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            return []
        entries = [root / name for name in proc.stdout.splitlines() if name]
    for entry in entries:
        name = entry.name if isinstance(entry, Path) else str(entry)
        path = entry if isinstance(entry, Path) else root / name
        if CLIENT_DEBUGFS_DIR_RE.match(path.name):
            dirs.append(path)
    return sorted(dirs, key=lambda p: p.name)


def ssh_run(
    host: str,
    remote_cmd: str,
    *,
    timeout: int,
    ssh_user: Optional[str] = None,
) -> str:
    target = f"{ssh_user}@{host}" if ssh_user else host
    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        target,
        remote_cmd,
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip() or "ssh failed"
        raise RuntimeError(f"ssh {target}: {err}")
    return proc.stdout


def list_client_debugfs_dirs_remote(
    host: str,
    root: str,
    *,
    timeout: int,
    ssh_user: Optional[str] = None,
) -> List[str]:
    script = (
        f"if [ -d {root!r} ]; then "
        f"ls -1 {root!r} 2>/dev/null || sudo ls -1 {root!r}; "
        f"fi"
    )
    out = ssh_run(host, script, timeout=timeout, ssh_user=ssh_user)
    names: List[str] = []
    for name in out.splitlines():
        name = name.strip()
        if CLIENT_DEBUGFS_DIR_RE.match(name):
            names.append(name)
    return sorted(names)


def read_text_file_remote(
    host: str,
    path: str,
    *,
    timeout: int,
    ssh_user: Optional[str] = None,
) -> str:
    quoted = path.replace("'", "'\"'\"'")
    script = f"cat '{quoted}' 2>/dev/null || sudo cat '{quoted}'"
    return ssh_run(host, script, timeout=timeout, ssh_user=ssh_user)


def sanitize_host_label(host: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", host) or "host"


def collect_client_debugfs_instance(
    *,
    host_label: str,
    instance_name: str,
    files: Dict[str, str],
    dest_dir: Path,
) -> Dict[str, Any]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    m = CLIENT_DEBUGFS_DIR_RE.match(instance_name)
    summary: Dict[str, Any] = {
        "host": host_label,
        "instance": instance_name,
        "fsid": m.group("fsid") if m else None,
        "client_id": m.group("client_id") if m else None,
        "files": {},
        "mdsc": {},
        "mds_sessions": {},
        "errors": {},
    }
    for name, text in files.items():
        out_path = dest_dir / name
        out_path.write_text(text, encoding="utf-8")
        summary["files"][name] = {
            "path": str(out_path),
            "bytes": len(text.encode("utf-8")),
            "lines": text.count("\n") + (1 if text and not text.endswith("\n") else 0),
        }
        if name == "mdsc":
            summary["mdsc"] = parse_mdsc_text(text)
        elif name == "mds_sessions":
            summary["mds_sessions"] = parse_mds_sessions_text(text)
    return summary


def collect_client_debugfs_local(
    *,
    output_dir: Path,
    root: Path,
    filenames: Iterable[str],
    timeout: int,
) -> Dict[str, Any]:
    host_label = "localhost"
    host_dir = output_dir / CLIENT_DEBUGFS_DIR / sanitize_host_label(host_label)
    instances = list_client_debugfs_dirs_local(root)
    results: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}
    if not root.exists():
        errors["root"] = f"missing {root}"
    elif not instances:
        errors["root"] = f"no client dirs under {root}"

    for instance_dir in instances:
        files: Dict[str, str] = {}
        for name in filenames:
            path = instance_dir / name
            try:
                files[name] = read_text_file_local(path, timeout)
            except RuntimeError as exc:
                errors[f"{instance_dir.name}/{name}"] = str(exc)
        if not files:
            continue
        results.append(
            collect_client_debugfs_instance(
                host_label=host_label,
                instance_name=instance_dir.name,
                files=files,
                dest_dir=host_dir / instance_dir.name,
            )
        )
    return {
        "host": host_label,
        "root": str(root),
        "instances": results,
        "errors": errors,
    }


def collect_client_debugfs_remote(
    *,
    output_dir: Path,
    host: str,
    root: str,
    filenames: Iterable[str],
    timeout: int,
    ssh_user: Optional[str] = None,
) -> Dict[str, Any]:
    host_label = host
    host_dir = output_dir / CLIENT_DEBUGFS_DIR / sanitize_host_label(host_label)
    errors: Dict[str, str] = {}
    results: List[Dict[str, Any]] = []
    try:
        names = list_client_debugfs_dirs_remote(
            host, root, timeout=timeout, ssh_user=ssh_user,
        )
    except RuntimeError as exc:
        return {
            "host": host_label,
            "root": root,
            "instances": [],
            "errors": {"root": str(exc)},
        }
    if not names:
        errors["root"] = f"no client dirs under {root} on {host}"

    for name in names:
        files: Dict[str, str] = {}
        for fname in filenames:
            remote_path = f"{root.rstrip('/')}/{name}/{fname}"
            try:
                files[fname] = read_text_file_remote(
                    host, remote_path, timeout=timeout, ssh_user=ssh_user,
                )
            except RuntimeError as exc:
                errors[f"{name}/{fname}"] = str(exc)
        if not files:
            continue
        results.append(
            collect_client_debugfs_instance(
                host_label=host_label,
                instance_name=name,
                files=files,
                dest_dir=host_dir / name,
            )
        )
    return {
        "host": host_label,
        "root": root,
        "instances": results,
        "errors": errors,
    }


def summarize_client_debugfs(host_payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_instances = 0
    total_requests = 0
    total_unsafe = 0
    by_op: Dict[str, int] = defaultdict(int)
    hosts: List[Dict[str, Any]] = []
    for host_payload in host_payloads:
        host_reqs = 0
        host_unsafe = 0
        instance_summaries: List[Dict[str, Any]] = []
        for inst in host_payload.get("instances") or []:
            mdsc = inst.get("mdsc") or {}
            req_count = int(mdsc.get("request_count") or 0)
            unsafe = int(mdsc.get("unsafe_count") or 0)
            host_reqs += req_count
            host_unsafe += unsafe
            total_requests += req_count
            total_unsafe += unsafe
            total_instances += 1
            for op, count in (mdsc.get("ops") or {}).items():
                by_op[op] += int(count)
            instance_summaries.append(
                {
                    "instance": inst.get("instance"),
                    "client_id": inst.get("client_id"),
                    "request_count": req_count,
                    "unsafe_count": unsafe,
                    "ops": mdsc.get("ops") or {},
                }
            )
        hosts.append(
            {
                "host": host_payload.get("host"),
                "root": host_payload.get("root"),
                "instance_count": len(instance_summaries),
                "request_count": host_reqs,
                "unsafe_count": host_unsafe,
                "instances": instance_summaries,
                "errors": host_payload.get("errors") or {},
            }
        )
    return {
        "host_count": len(host_payloads),
        "instance_count": total_instances,
        "request_count": total_requests,
        "unsafe_count": total_unsafe,
        "ops": dict(sorted(by_op.items())),
        "hosts": hosts,
    }


def collect_client_debugfs(
    *,
    output_dir: Path,
    root: str = "/sys/kernel/debug/ceph",
    hosts: Optional[List[str]] = None,
    filenames: Optional[Iterable[str]] = None,
    ssh_user: Optional[str] = None,
    timeout: int = 60,
    include_local: bool = True,
) -> Dict[str, Any]:
    names = tuple(filenames or CLIENT_DEBUGFS_DEFAULT_FILES)
    (output_dir / CLIENT_DEBUGFS_DIR).mkdir(parents=True, exist_ok=True)
    host_payloads: List[Dict[str, Any]] = []

    if include_local and not hosts:
        host_payloads.append(
            collect_client_debugfs_local(
                output_dir=output_dir,
                root=Path(root),
                filenames=names,
                timeout=timeout,
            )
        )
    elif include_local and hosts:
        # Explicit hosts imply remote-only unless localhost is listed.
        pass

    for host in hosts or []:
        if host in ("localhost", "127.0.0.1", "::1"):
            host_payloads.append(
                collect_client_debugfs_local(
                    output_dir=output_dir,
                    root=Path(root),
                    filenames=names,
                    timeout=timeout,
                )
            )
            continue
        host_payloads.append(
            collect_client_debugfs_remote(
                output_dir=output_dir,
                host=host,
                root=root,
                filenames=names,
                timeout=timeout,
                ssh_user=ssh_user,
            )
        )

    summary = summarize_client_debugfs(host_payloads)
    summary["files"] = list(names)
    summary["collected_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    write_json_artifact(output_dir / CLIENT_DEBUGFS_SUMMARY, summary)
    write_json_artifact(
        output_dir / CLIENT_DEBUGFS_DIR / "hosts.json",
        {"hosts": host_payloads},
    )
    return summary


def format_client_debugfs_report(summary: Dict[str, Any]) -> List[str]:
    lines = ["== Client kernel debugfs (mdsc) =="]
    lines.append(
        "Hosts={hosts}, mounts={instances}, outstanding mdsc requests={reqs} "
        "(unsafe={unsafe})".format(
            hosts=summary.get("host_count", 0),
            instances=summary.get("instance_count", 0),
            reqs=summary.get("request_count", 0),
            unsafe=summary.get("unsafe_count", 0),
        )
    )
    ops = summary.get("ops") or {}
    if ops:
        lines.append(
            "Ops: "
            + ", ".join(f"{op}={count}" for op, count in sorted(ops.items()))
        )
    for host in summary.get("hosts") or []:
        lines.append(
            f"- {host.get('host')}: {host.get('instance_count', 0)} mount(s), "
            f"{host.get('request_count', 0)} request(s)"
        )
        for inst in host.get("instances") or []:
            cid = inst.get("client_id") or "?"
            lines.append(
                f"    client.{cid} ({inst.get('instance')}): "
                f"{inst.get('request_count', 0)} request(s)"
            )
            sample_ops = inst.get("ops") or {}
            if sample_ops:
                lines.append(
                    "      "
                    + ", ".join(
                        f"{op}={count}" for op, count in sorted(sample_ops.items())
                    )
                )
        for name, err in sorted((host.get("errors") or {}).items()):
            lines.append(f"    error {name}: {err}")
    return lines


def append_client_debugfs_report_section(
    report_path: Path,
    summary: Dict[str, Any],
) -> None:
    if not report_path.exists():
        return
    section = format_client_debugfs_report(summary)
    with report_path.open("a", encoding="utf-8") as handle:
        handle.write("\n")
        handle.write("\n".join(section))
        handle.write("\n")


def build_log_tail_fallback_window(
    window: LogExtractWindow,
    *,
    first_ts: datetime,
    last_ts: datetime,
    tail_seconds: int,
    reason: str,
) -> LogExtractWindow:
    """Use the last N seconds of an available log when the stall window misses it."""
    end = last_ts
    start = end - timedelta(seconds=tail_seconds)
    if first_ts > start:
        start = first_ts
    return LogExtractWindow(
        stall_time=window.stall_time,
        start=start,
        end=end,
        source_times=window.source_times,
        blocked_ops=window.blocked_ops,
        last_activity=window.last_activity,
        fallback_used=True,
        fallback_reason=reason,
    )


def log_file_time_span(log_path: Path) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (first, last) timestamps in a log file without reading it whole."""
    size = log_path.stat().st_size
    if size == 0:
        return None, None
    with log_path.open("rb") as f:
        first = parse_log_line_timestamp(f.readline())
        tail_pos = max(0, size - 65536)
        f.seek(tail_pos)
        if tail_pos > 0:
            f.readline()
        last = None
        for line in f:
            ts = parse_log_line_timestamp(line)
            if ts is not None:
                last = ts
    return first, last


def align_to_line_start(f, pos: int) -> int:
    if pos <= 0:
        return 0
    f.seek(pos)
    while True:
        ch = f.read(1)
        if not ch:
            return f.tell()
        if ch == b"\n":
            return f.tell()
        if f.tell() >= pos + 4096:
            return pos


def timestamp_at_offset(f, pos: int) -> Optional[datetime]:
    aligned = align_to_line_start(f, pos)
    f.seek(aligned)
    line = f.readline()
    if not line:
        return None
    return parse_log_line_timestamp(line)


def bisect_log_offset(
    f,
    file_size: int,
    target: datetime,
    *,
    find_first_ge: bool,
) -> int:
    lo, hi = 0, file_size
    while lo < hi:
        mid = (lo + hi) // 2
        ts = timestamp_at_offset(f, mid)
        if ts is None:
            lo = mid + 1
            continue
        if find_first_ge:
            if ts < target:
                lo = mid + 1
            else:
                hi = mid
        else:
            if ts <= target:
                lo = mid + 1
            else:
                hi = mid
    return lo


def extract_mds_log_slice(
    log_path: Path,
    window: LogExtractWindow,
    output_path: Path,
    chunk_size: int = 8 * 1024 * 1024,
) -> Dict[str, Any]:
    """Extract [start, end] from a large MDS log using binary search on timestamps."""
    if not log_path.is_file():
        raise RuntimeError(f"MDS log not found: {log_path}")

    file_size = log_path.stat().st_size
    if file_size == 0:
        raise RuntimeError(f"MDS log is empty: {log_path}")

    with log_path.open("rb") as src:
        start_off = bisect_log_offset(
            src, file_size, window.start, find_first_ge=True,
        )
        end_off = bisect_log_offset(
            src, file_size, window.end, find_first_ge=False,
        )
        if end_off < start_off:
            end_off = start_off

        output_path.parent.mkdir(parents=True, exist_ok=True)
        header = (
            "# MDS log extract\n"
            f"# source: {log_path}\n"
            f"# stall_time: {format_ceph_timestamp(window.stall_time)}\n"
        )
        if window.last_activity is not None:
            header += (
                f"# last_activity: {format_ceph_timestamp(window.last_activity)}\n"
            )
        header += (
            f"# window_start: {format_ceph_timestamp(window.start)}\n"
            f"# window_end: {format_ceph_timestamp(window.end)}\n"
            f"# byte_range: {start_off}-{end_off} of {file_size}\n"
            f"# blocked_ops: {window.blocked_ops}\n"
        )
        if window.fallback_used:
            header += (
                "# fallback_used: true\n"
                f"# fallback_reason: {window.fallback_reason or 'log tail fallback'}\n"
            )
        header += "#\n"
        header = header.encode("utf-8")

        bytes_written = len(header)
        line_count = 0
        with output_path.open("wb") as dst:
            dst.write(header)
            src.seek(start_off)
            remaining = end_off - start_off
            while remaining > 0:
                data = src.read(min(chunk_size, remaining))
                if not data:
                    break
                dst.write(data)
                bytes_written += len(data)
                line_count += data.count(b"\n")
                remaining -= len(data)

    meta = {
        "source_log": str(log_path),
        "output_log": str(output_path),
        "stall_time": format_ceph_timestamp(window.stall_time),
        "window_start": format_ceph_timestamp(window.start),
        "window_end": format_ceph_timestamp(window.end),
        "byte_start": start_off,
        "byte_end": end_off,
        "source_size": file_size,
        "bytes_written": bytes_written,
        "approx_lines": line_count,
        "blocked_ops": window.blocked_ops,
        "idle_stall": window.blocked_ops == 0,
        "source_times": window.source_times,
        "fallback_used": window.fallback_used,
    }
    if window.last_activity is not None:
        meta["last_activity"] = format_ceph_timestamp(window.last_activity)
    if window.fallback_reason:
        meta["fallback_reason"] = window.fallback_reason
    meta_path = output_path.with_suffix(".json")
    meta_path.write_text(
        json.dumps(meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    meta["metadata_json"] = str(meta_path)
    return meta


def run_log_extract(
    stall_dir: Path,
    log_path: Path,
    output_path: Optional[Path],
    padding_before: int,
    padding_after: int,
    idle_before: int = 600,
    fallback_tail_seconds: int = 1200,
) -> Dict[str, Any]:
    base_window = collect_stall_times(stall_dir)
    requested = bracket_log_window(
        base_window,
        padding_before,
        padding_after,
        idle_before=idle_before,
    )
    if output_path is None:
        output_path = stall_dir / "log_extract.log"
    first_ts, last_ts = log_file_time_span(log_path)

    needs_fallback = False
    fallback_reason = ""
    if first_ts is not None and last_ts is not None:
        activity_in_log = (
            base_window.last_activity is not None
            and first_ts <= base_window.last_activity <= last_ts
        )
        if base_window.stall_time > last_ts and not activity_in_log:
            needs_fallback = True
            fallback_reason = (
                "stall snapshot "
                f"{format_ceph_timestamp(base_window.stall_time)} is after log end "
                f"{format_ceph_timestamp(last_ts)}"
            )
        elif requested.end < first_ts or requested.start > last_ts:
            needs_fallback = True
            fallback_reason = (
                "log file timestamps do not overlap the stall window; "
                f"log spans {format_ceph_timestamp(first_ts)} .. "
                f"{format_ceph_timestamp(last_ts)}"
            )

    if (
        needs_fallback
        and fallback_tail_seconds > 0
        and last_ts is not None
        and first_ts is not None
    ):
        window = build_log_tail_fallback_window(
            base_window,
            first_ts=first_ts,
            last_ts=last_ts,
            tail_seconds=fallback_tail_seconds,
            reason=(
                f"{fallback_reason}; using last {fallback_tail_seconds}s of log"
            ),
        )
        meta = extract_mds_log_slice(log_path, window, output_path)
        meta["requested_window_start"] = format_ceph_timestamp(requested.start)
        meta["requested_window_end"] = format_ceph_timestamp(requested.end)
    else:
        window = clamp_window_to_log(requested, first_ts, last_ts)
        meta = extract_mds_log_slice(log_path, window, output_path)
        if (
            meta.get("approx_lines", 0) == 0
            and fallback_tail_seconds > 0
            and last_ts is not None
            and first_ts is not None
        ):
            window = build_log_tail_fallback_window(
                base_window,
                first_ts=first_ts,
                last_ts=last_ts,
                tail_seconds=fallback_tail_seconds,
                reason=(
                    "no log lines matched the stall window; "
                    f"using last {fallback_tail_seconds}s of log"
                ),
            )
            meta = extract_mds_log_slice(log_path, window, output_path)
            meta["requested_window_start"] = format_ceph_timestamp(requested.start)
            meta["requested_window_end"] = format_ceph_timestamp(requested.end)

    if first_ts is not None and last_ts is not None:
        meta["log_first"] = format_ceph_timestamp(first_ts)
        meta["log_last"] = format_ceph_timestamp(last_ts)
    if base_window.last_activity is not None:
        meta["last_activity"] = format_ceph_timestamp(base_window.last_activity)
    if window.fallback_used:
        meta["warning"] = window.fallback_reason
    elif needs_fallback and fallback_tail_seconds <= 0:
        meta["warning"] = (
            f"{fallback_reason}; log tail fallback disabled "
            f"(--log-fallback-tail-seconds 0)"
        )

    meta_path = Path(meta["metadata_json"])
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return meta


def normalize_sessions_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    sessions = payload.get("sessions") or []
    out: List[Dict[str, Any]] = []
    for entry in sessions:
        if isinstance(entry, dict) and "session" in entry:
            out.append(entry["session"])
        elif isinstance(entry, dict):
            out.append(entry)
    return out


def sessions_payload_has_cap_dump(payload: Any) -> bool:
    for sess in normalize_sessions_payload(payload):
        caps = sess.get("caps")
        if isinstance(caps, list) and caps:
            return True
    return False


CLIENT_ENTITY_RE = re.compile(r"client\.(\d+)")


def session_client_id(sess: Dict[str, Any]) -> Optional[str]:
    sid = sess.get("id")
    if sid is not None:
        return str(sid)

    entity = sess.get("entity")
    if isinstance(entity, dict):
        name = entity.get("name")
        if isinstance(name, dict) and name.get("num") is not None:
            return str(name["num"])
        if isinstance(name, str):
            m = CLIENT_ENTITY_RE.match(name)
            if m:
                return m.group(1)

    inst = sess.get("inst")
    if isinstance(inst, str):
        m = CLIENT_ENTITY_RE.search(inst)
        if m:
            return m.group(1)
    elif isinstance(inst, dict):
        name = inst.get("name")
        if isinstance(name, dict) and name.get("num") is not None:
            return str(name["num"])
        if isinstance(name, str):
            m = CLIENT_ENTITY_RE.match(name)
            if m:
                return m.group(1)

    info = sess.get("info")
    if isinstance(info, dict):
        nested = session_client_id(info)
        if nested is not None:
            return nested

    return None


def finalize_analysis(
    output_dir: Path,
    mds_target: str,
    summaries: List[OpSummary],
    client_ids: Set[str],
    inode_by_hex: Dict[str, InodeSummary],
    sessions: Dict[str, Any],
    blocked_count: Optional[int],
    extra_summary: Optional[Dict[str, Any]] = None,
    idle_diag: Optional[Dict[str, Any]] = None,
) -> None:
    cap_infos, cap_correlations = build_cap_revoke_correlation(
        summaries, inode_by_hex, sessions,
    )
    apply_cap_revoke_notes(cap_infos, inode_by_hex)

    graph = build_graph(
        summaries, inode_by_hex, sessions, cap_infos=cap_infos, path_edges=True,
    )
    (output_dir / "graph.json").write_text(
        json.dumps(graph, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_dir / "cap_revokes.json").write_text(
        json.dumps({"correlations": cap_correlations}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_text_report(
        output_dir / "report.txt",
        mds_target=mds_target,
        ops=summaries,
        inode_by_hex=inode_by_hex,
        sessions=sessions,
        cap_correlations=cap_correlations,
        blocked_count=blocked_count,
        idle_diag=idle_diag,
    )

    summary = {
        "blocked_ops": blocked_count,
        "investigated_ops": len(summaries),
        "inodes_dumped": len(inode_by_hex),
        "clients": sorted(client_ids, key=int),
        "outstanding_cap_revokes": len(cap_correlations),
        "output_dir": str(output_dir),
    }
    if idle_diag:
        summary["idle_diagnostics"] = True
        summary["num_ops_in_flight"] = idle_diag.get("num_ops_in_flight", 0)
        summary["historic_ops_count"] = idle_diag.get("historic_ops_count", 0)
        sess = idle_diag.get("sessions") or {}
        summary["session_count"] = sess.get("session_count", 0)
        summary["total_requests_in_flight"] = sess.get("total_requests_in_flight", 0)
    if extra_summary:
        summary.update(extra_summary)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )


def collect_from_dir(
    input_dir: Path,
    output_dir: Path,
    reqid_filter: Optional[str],
    all_ops: bool,
    follow_parents: bool,
    max_inodes: int,
    *,
    grafana_url: Optional[str] = None,
    prometheus_url: Optional[str] = None,
    grafana_auth: Optional[Tuple[str, str]] = None,
    metrics_padding_before: int = 60,
    metrics_padding_after: int = 120,
    metrics_idle_before: int = 600,
    metrics_step: str = "15s",
    metrics_timeout: int = 60,
    client_debugfs: bool = False,
    client_debugfs_root: str = "/sys/kernel/debug/ceph",
    client_hosts: Optional[List[str]] = None,
    client_debugfs_files: Optional[List[str]] = None,
    client_ssh_user: Optional[str] = None,
) -> None:
    blocked_path = input_dir / "blocked_ops.json"
    locks_path = input_dir / "ops_locks.json"
    if not locks_path.exists():
        raise RuntimeError(f"missing {locks_path}")

    blocked = json.loads(blocked_path.read_text(encoding="utf-8")) if blocked_path.exists() else {}
    ops_with_locks = json.loads(locks_path.read_text(encoding="utf-8"))

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, payload in (("blocked_ops.json", blocked), ("ops_locks.json", ops_with_locks)):
        (output_dir / name).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    idle_diag = load_idle_diagnostics_from_dir(input_dir, output_dir)

    blocked_ops = normalize_ops_payload(blocked)
    blocked_count = None
    if isinstance(blocked, dict) and "num_blocked_ops" in blocked:
        blocked_count = blocked.get("num_blocked_ops")
    elif blocked_ops:
        blocked_count = len(blocked_ops)
    else:
        blocked_count = 0
    lock_ops = normalize_ops_payload(ops_with_locks)
    lock_ops_by_desc = {op.get("description"): op for op in lock_ops}

    selected: List[Dict[str, Any]] = []
    if reqid_filter:
        for op in lock_ops:
            desc = op.get("description") or ""
            td = op.get("type_data") or {}
            rid = td.get("reqid") or {}
            entity = rid.get("entity") or {}
            synthesized = ""
            if entity.get("type") == "client" and "tid" in rid:
                synthesized = f"client.{entity.get('num')}:{rid.get('tid')}"
            if reqid_filter in desc or reqid_filter == synthesized:
                selected.append(op)
    elif all_ops:
        selected = lock_ops
    else:
        for op in blocked_ops:
            desc = op.get("description")
            selected.append(lock_ops_by_desc.get(desc, op))
        if not selected:
            for op in lock_ops:
                fp = (op.get("type_data") or {}).get("flag_point") or ""
                if "waiting" in fp or "failed" in fp:
                    selected.append(op)

    summaries = [op_from_json(op) for op in selected]
    client_ids: Set[str] = {s.client_id for s in summaries if s.client_id}

    inos: Set[str] = set()
    for op in selected:
        inos.update(extract_inos_from_op(op))

    inode_by_hex: Dict[str, InodeSummary] = {}
    inode_dir = input_dir / "inodes"
    out_inode_dir = output_dir / "inodes"
    out_inode_dir.mkdir(exist_ok=True)
    for ino_hex in sorted(inos):
        if len(inode_by_hex) >= max_inodes:
            break
        src = inode_dir / f"{ino_hex}.json"
        if src.exists():
            dump = json.loads(src.read_text(encoding="utf-8"))
        else:
            dump = {"error": "missing in dump dir", "ino_hex": ino_hex}
        (out_inode_dir / f"{ino_hex}.json").write_text(
            json.dumps(dump, indent=2, sort_keys=True), encoding="utf-8"
        )
        if "error" not in dump:
            summary = summarize_inode(ino_hex, dump)
            summary.notes.extend(analyze_inode(summary, client_ids))
            inode_by_hex[ino_hex] = summary

    sessions: Dict[str, Any] = {}
    sess_dir = input_dir / "sessions"
    out_sess_dir = output_dir / "sessions"
    out_sess_dir.mkdir(exist_ok=True)
    for cid in sorted(client_ids, key=int):
        src = sess_dir / f"client_{cid}.json"
        if src.exists():
            sess_list = json.loads(src.read_text(encoding="utf-8"))
            (out_sess_dir / src.name).write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
            for sess in normalize_sessions_payload(sess_list):
                if session_client_id(sess) == cid:
                    sessions[cid] = sess
                    break

    if idle_diag:
        sessions_path = output_dir / "sessions_all_caps.json"
        if sessions_path.exists():
            sessions_payload = json.loads(sessions_path.read_text(encoding="utf-8"))
            sessions = merge_idle_sessions(sessions, idle_diag, sessions_payload)
            for cid in sessions:
                client_ids.add(cid)

    mds_target = infer_mds_target_from_stall_dir(input_dir, str(input_dir))
    prom_payload = None
    if grafana_url or prometheus_url:
        prom_payload = collect_prometheus_timeseries(
            stall_dir=input_dir,
            mds_target=mds_target,
            grafana_url=grafana_url,
            prometheus_url=prometheus_url,
            auth=grafana_auth,
            padding_before=metrics_padding_before,
            padding_after=metrics_padding_after,
            idle_before=metrics_idle_before,
            step=metrics_step,
            timeout=metrics_timeout,
        )
        write_json_artifact(
            output_dir / PROMETHEUS_TIMESERIES_ARTIFACT, prom_payload,
        )

    finalize_analysis(
        output_dir=output_dir,
        mds_target=mds_target,
        summaries=summaries,
        client_ids=client_ids,
        inode_by_hex=inode_by_hex,
        sessions=sessions,
        blocked_count=blocked_count,
        extra_summary={"source_dir": str(input_dir)},
        idle_diag=idle_diag,
    )
    if prom_payload:
        append_prometheus_report_section(output_dir / "report.txt", prom_payload)
    if client_debugfs:
        debugfs_summary = collect_client_debugfs(
            output_dir=output_dir,
            root=client_debugfs_root,
            hosts=client_hosts,
            filenames=client_debugfs_files,
            ssh_user=client_ssh_user,
            timeout=metrics_timeout,
            include_local=not client_hosts,
        )
        append_client_debugfs_report_section(
            output_dir / "report.txt", debugfs_summary,
        )


def collect(
    client: CephMDSClient,
    invoke,
    mds_target: str,
    output_dir: Path,
    reqid_filter: Optional[str],
    all_ops: bool,
    follow_parents: bool,
    max_inodes: int,
    skip_idle_diagnostics: bool = False,
    session_cap_dump: bool = False,
    grafana_url: Optional[str] = None,
    prometheus_url: Optional[str] = None,
    grafana_auth: Optional[Tuple[str, str]] = None,
    metrics_padding_before: int = 60,
    metrics_padding_after: int = 120,
    metrics_idle_before: int = 600,
    metrics_step: str = "15s",
    metrics_timeout: int = 60,
    client_debugfs: bool = False,
    client_debugfs_root: str = "/sys/kernel/debug/ceph",
    client_hosts: Optional[List[str]] = None,
    client_debugfs_files: Optional[List[str]] = None,
    client_ssh_user: Optional[str] = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    collection_errors: Dict[str, str] = {}
    allow_partial = bool(client_debugfs or grafana_url or prometheus_url)

    blocked, err = invoke_optional(invoke, ["dump_blocked_ops"])
    if err:
        collection_errors["dump_blocked_ops"] = err
        print(
            f"warning: MDS dump_blocked_ops failed (asok may be wedged): {err}",
            file=sys.stderr,
        )
        if not allow_partial:
            raise RuntimeError(err)
    write_json_artifact(output_dir / "blocked_ops.json", blocked)

    blocked_count = None
    count_payload, err = invoke_optional(invoke, ["dump_blocked_ops_count"])
    if err:
        collection_errors["dump_blocked_ops_count"] = err
    elif isinstance(count_payload, dict):
        blocked_count = count_payload.get("num_blocked_ops")

    ops_with_locks, err = invoke_optional(invoke, ["ops", "--flags=locks"])
    if err:
        collection_errors["ops_locks"] = err
        print(
            f"warning: MDS ops --flags=locks failed: {err}",
            file=sys.stderr,
        )
        if not allow_partial and "dump_blocked_ops" in collection_errors:
            raise RuntimeError(err)
    write_json_artifact(output_dir / "ops_locks.json", ops_with_locks)

    blocked_ops = normalize_ops_payload(blocked)
    idle_diag = None
    sessions_payload: Any = {}
    # If asok failed, still try idle extras when blocked set is empty/unknown.
    treat_as_idle = should_collect_idle_diagnostics(
        skip_idle_diagnostics, blocked_count, blocked_ops,
    ) or (
        not skip_idle_diagnostics
        and "dump_blocked_ops" in collection_errors
        and not blocked_ops
    )
    if treat_as_idle:
        idle_diag = collect_idle_stall_diagnostics(
            invoke, output_dir, session_cap_dump=session_cap_dump,
        )
        if idle_diag.get("collection_errors"):
            collection_errors.update(
                {
                    f"idle.{k}": v
                    for k, v in (idle_diag.get("collection_errors") or {}).items()
                }
            )
        sessions_path = output_dir / "sessions_all_caps.json"
        if sessions_path.exists():
            sessions_payload = json.loads(sessions_path.read_text(encoding="utf-8"))

    lock_ops = normalize_ops_payload(ops_with_locks)
    lock_ops_by_desc = {op.get("description"): op for op in lock_ops}

    selected: List[Dict[str, Any]] = []
    if reqid_filter:
        for op in lock_ops:
            desc = op.get("description") or ""
            td = op.get("type_data") or {}
            rid = td.get("reqid") or {}
            entity = rid.get("entity") or {}
            synthesized = ""
            if entity.get("type") == "client" and "tid" in rid:
                synthesized = f"client.{entity.get('num')}:{rid.get('tid')}"
            if reqid_filter in desc or reqid_filter == synthesized:
                selected.append(op)
        if not selected:
            op_payload, err = invoke_optional(
                invoke, ["op", "get", reqid_filter, "--flags=locks"],
            )
            if err:
                collection_errors["op_get"] = err
            elif op_payload:
                selected.append(op_payload)
    elif all_ops:
        selected = lock_ops
    else:
        for op in blocked_ops:
            desc = op.get("description")
            selected.append(lock_ops_by_desc.get(desc, op))
        if not selected:
            for op in lock_ops:
                fp = (op.get("type_data") or {}).get("flag_point") or ""
                if "waiting" in fp or "failed" in fp:
                    selected.append(op)

    summaries = [op_from_json(op) for op in selected]
    client_ids: Set[str] = {s.client_id for s in summaries if s.client_id}

    inos: Set[str] = set()
    for op in selected:
        inos.update(extract_inos_from_op(op))

    inode_by_hex: Dict[str, InodeSummary] = {}
    inode_dir = output_dir / "inodes"
    inode_dir.mkdir(exist_ok=True)

    for ino_hex in sorted(inos):
        if len(inode_by_hex) >= max_inodes:
            break
        ino_dec = hex_ino_to_dec(ino_hex)
        try:
            dump = invoke(["dump", "inode", str(ino_dec)])
        except RuntimeError as e:
            dump = {"error": str(e), "ino_hex": ino_hex, "ino": ino_dec}
            collection_errors[f"dump_inode.{ino_hex}"] = str(e)
        (inode_dir / f"{ino_hex}.json").write_text(
            json.dumps(dump, indent=2, sort_keys=True), encoding="utf-8"
        )
        if "error" not in dump:
            summary = summarize_inode(ino_hex, dump)
            summary.notes.extend(analyze_inode(summary, client_ids))
            inode_by_hex[ino_hex] = summary

    if follow_parents:
        # Add parent/child edges in graph later from path prefixes; no extra dumps yet.
        pass

    sessions: Dict[str, Any] = {}
    session_dir = output_dir / "sessions"
    session_dir.mkdir(exist_ok=True)
    for cid in sorted(client_ids, key=int):
        try:
            sess_list = invoke_session_ls(
                invoke, client_id=cid, cap_dump=session_cap_dump,
            )
        except RuntimeError as e:
            collection_errors[f"session_ls.{cid}"] = str(e)
            continue
        (session_dir / f"client_{cid}.json").write_text(
            json.dumps(sess_list, indent=2, sort_keys=True), encoding="utf-8"
        )
        for sess in normalize_sessions_payload(sess_list):
            if session_client_id(sess) == cid:
                sessions[cid] = sess
                break

    sessions = merge_idle_sessions(sessions, idle_diag, sessions_payload)
    for cid in sessions:
        client_ids.add(cid)

    prom_payload = maybe_collect_prometheus_timeseries(
        stall_dir=output_dir,
        mds_target=mds_target,
        grafana_url=grafana_url,
        prometheus_url=prometheus_url,
        grafana_auth=grafana_auth,
        padding_before=metrics_padding_before,
        padding_after=metrics_padding_after,
        idle_before=metrics_idle_before,
        step=metrics_step,
        timeout=metrics_timeout,
    )

    finalize_analysis(
        output_dir=output_dir,
        mds_target=mds_target,
        summaries=summaries,
        client_ids=client_ids,
        inode_by_hex=inode_by_hex,
        sessions=sessions,
        blocked_count=blocked_count,
        idle_diag=idle_diag,
        extra_summary=(
            {"collection_errors": collection_errors} if collection_errors else None
        ),
    )
    if prom_payload:
        append_prometheus_report_section(output_dir / "report.txt", prom_payload)
    if client_debugfs:
        debugfs_summary = collect_client_debugfs(
            output_dir=output_dir,
            root=client_debugfs_root,
            hosts=client_hosts,
            filenames=client_debugfs_files,
            ssh_user=client_ssh_user,
            timeout=metrics_timeout,
            include_local=not client_hosts,
        )
        append_client_debugfs_report_section(
            output_dir / "report.txt", debugfs_summary,
        )

    if collection_errors:
        write_json_artifact(
            output_dir / "collection_errors.json", collection_errors,
        )
        print(
            f"warning: {len(collection_errors)} MDS collection error(s); "
            f"see {output_dir / 'collection_errors.json'}",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect and correlate MDS blocked-op / lock / inode diagnostics.",
    )
    target = parser.add_mutually_exclusive_group(required=False)
    target.add_argument(
        "--mds",
        help="MDS tell target, e.g. myfs:0 or cephfs:0",
    )
    target.add_argument(
        "--daemon",
        help="MDS daemon name or admin socket path for `ceph daemon`, "
        "e.g. mds.myfs-1.abc123 or /var/run/ceph/ceph-mds.*.asok",
    )
    target.add_argument(
        "--asok",
        help="Alias for --daemon with an admin socket path",
    )
    parser.add_argument(
        "--from-dir",
        type=Path,
        help="Analyze saved JSON artifacts instead of querying live MDS "
        "(expects ops_locks.json, optional blocked_ops.json, inodes/, sessions/)",
    )
    parser.add_argument(
        "--conf",
        default=os.environ.get("CEPH_CONF"),
        help="ceph.conf path (default: $CEPH_CONF or ceph default)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Directory for JSON artifacts and report (default: ./mds-stall-<ts>)",
    )
    parser.add_argument(
        "--reqid",
        help="Investigate a single op, e.g. client.435625:1356421",
    )
    parser.add_argument(
        "--all-ops",
        action="store_true",
        help="Investigate all in-flight ops with locks, not just blocked ones",
    )
    parser.add_argument(
        "--follow-parents",
        action="store_true",
        help="Reserved: also walk parent inodes from dumped paths",
    )
    parser.add_argument(
        "--max-inodes",
        type=int,
        default=64,
        help="Maximum inode dumps to collect (default: 64)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout per ceph command in seconds (default: 120)",
    )
    parser.add_argument(
        "--stall-dir",
        type=Path,
        help="Directory containing a prior stall report (blocked_ops.json, report.txt)",
    )
    parser.add_argument(
        "--extract-log",
        type=Path,
        help="Path to a (possibly huge) MDS log; extract the stall time window to a slice",
    )
    parser.add_argument(
        "--log-output",
        type=Path,
        default=None,
        help="Output path for extracted log (default: <stall-dir>/log_extract.log)",
    )
    parser.add_argument(
        "--log-padding-before",
        type=int,
        default=60,
        help=(
            "Seconds before earliest blocked-op timestamp to include (default: 60); "
            "idle stalls use --log-idle-before instead"
        ),
    )
    parser.add_argument(
        "--log-padding-after",
        type=int,
        default=120,
        help=(
            "Seconds after stall end / last client activity to include (default: 120)"
        ),
    )
    parser.add_argument(
        "--log-idle-before",
        type=int,
        default=600,
        help=(
            "For 0 blocked-op stalls, seconds before last client activity event "
            "to include (default: 600)"
        ),
    )
    parser.add_argument(
        "--log-fallback-tail-seconds",
        type=int,
        default=1200,
        help=(
            "When the stall window misses the log (common for idle stalls), "
            "extract the last N seconds of the available log (default: 1200; 0 disables)"
        ),
    )
    parser.add_argument(
        "--skip-idle-diagnostics",
        action="store_true",
        help="Do not collect idle-stall extras when blocked_ops is 0",
    )
    parser.add_argument(
        "--session-cap-dump",
        action="store_true",
        help=(
            "Include per-cap details in session ls (very slow with many caps; "
            "needed for cap-revoke correlation)"
        ),
    )
    parser.add_argument(
        "--grafana-url",
        help=(
            "Grafana base URL (e.g. http://localhost:3000) to fetch Prometheus "
            "time series for reactor dispatch counters over the stall window"
        ),
    )
    parser.add_argument(
        "--prometheus-url",
        help=(
            "Prometheus base URL (e.g. http://localhost:9095). If set, queries "
            "Prometheus directly instead of via Grafana"
        ),
    )
    parser.add_argument(
        "--grafana-user",
        help="Grafana basic-auth username (optional; anonymous access is typical)",
    )
    parser.add_argument(
        "--grafana-password",
        default="",
        help="Grafana basic-auth password",
    )
    parser.add_argument(
        "--metrics-step",
        default="15s",
        help="Prometheus query_range step (default: 15s)",
    )
    parser.add_argument(
        "--metrics-timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds for Grafana/Prometheus queries (default: 60)",
    )
    parser.add_argument(
        "--client-debugfs",
        action="store_true",
        help=(
            "Collect kernel CephFS client debugfs files under "
            "/sys/kernel/debug/ceph/*/ (mdsc, mds_sessions, osdc, caps). "
            "Use --client-host for remote clients via SSH"
        ),
    )
    parser.add_argument(
        "--client-debugfs-root",
        default="/sys/kernel/debug/ceph",
        help="Client debugfs root (default: /sys/kernel/debug/ceph)",
    )
    parser.add_argument(
        "--client-host",
        action="append",
        default=[],
        dest="client_hosts",
        help=(
            "SSH host to scrape client debugfs from (repeatable). "
            "If omitted with --client-debugfs, scrapes localhost"
        ),
    )
    parser.add_argument(
        "--client-ssh-user",
        help="SSH username for --client-host (default: current user)",
    )
    parser.add_argument(
        "--client-debugfs-files",
        default=",".join(CLIENT_DEBUGFS_DEFAULT_FILES),
        help=(
            "Comma-separated debugfs filenames to collect "
            f"(default: {','.join(CLIENT_DEBUGFS_DEFAULT_FILES)})"
        ),
    )
    args = parser.parse_args()
    grafana_auth = None
    if args.grafana_user:
        grafana_auth = (args.grafana_user, args.grafana_password)
    if args.asok:
        if args.daemon and args.daemon != args.asok:
            parser.error("use only one of --daemon and --asok")
        args.daemon = args.asok

    def maybe_extract_log(stall_dir: Path) -> Optional[Dict[str, Any]]:
        if not args.extract_log:
            return None
        try:
            meta = run_log_extract(
                stall_dir=stall_dir,
                log_path=args.extract_log,
                output_path=args.log_output,
                padding_before=args.log_padding_before,
                padding_after=args.log_padding_after,
                idle_before=args.log_idle_before,
                fallback_tail_seconds=args.log_fallback_tail_seconds,
            )
        except RuntimeError as e:
            print(f"error: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"Extracted log slice to {meta['output_log']}")
        print(f"  metadata: {meta['metadata_json']}")
        if meta.get("last_activity"):
            print(f"  last activity: {meta['last_activity']}")
        print(
            f"  window: {meta['window_start']} .. {meta['window_end']} "
            f"(~{meta['approx_lines']} lines, {meta['bytes_written']} bytes)"
        )
        if meta.get("warning"):
            print(f"  warning: {meta['warning']}", file=sys.stderr)
        return meta

    extract_only = args.extract_log and not (args.mds or args.daemon or args.from_dir)
    if extract_only:
        stall_dir = args.stall_dir
        if stall_dir is None:
            parser.error("--extract-log-only run requires --stall-dir")
        maybe_extract_log(stall_dir)
        return 0

    def metrics_kwargs() -> Dict[str, Any]:
        files = [
            part.strip()
            for part in (args.client_debugfs_files or "").split(",")
            if part.strip()
        ]
        return {
            "grafana_url": args.grafana_url,
            "prometheus_url": args.prometheus_url,
            "grafana_auth": grafana_auth,
            "metrics_padding_before": args.log_padding_before,
            "metrics_padding_after": args.log_padding_after,
            "metrics_idle_before": args.log_idle_before,
            "metrics_step": args.metrics_step,
            "metrics_timeout": args.metrics_timeout,
            "client_debugfs": args.client_debugfs,
            "client_debugfs_root": args.client_debugfs_root,
            "client_hosts": args.client_hosts or None,
            "client_debugfs_files": files or list(CLIENT_DEBUGFS_DEFAULT_FILES),
            "client_ssh_user": args.client_ssh_user,
        }

    if args.from_dir and not (args.mds or args.daemon):
        # offline replay only
        if args.output is None:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            args.output = Path.cwd() / f"mds-stall-replay-{ts}"
        try:
            collect_from_dir(
                input_dir=args.from_dir,
                output_dir=args.output,
                reqid_filter=args.reqid,
                all_ops=args.all_ops,
                follow_parents=args.follow_parents,
                max_inodes=args.max_inodes,
                **metrics_kwargs(),
            )
        except RuntimeError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"Wrote diagnostics to {args.output}")
        print(f"  report: {args.output / 'report.txt'}")
        print(f"  graph:  {args.output / 'graph.json'}")
        print(f"  revokes:{args.output / 'cap_revokes.json'}")
        if (args.output / CLIENT_DEBUGFS_SUMMARY).exists():
            print(f"  debugfs:{args.output / CLIENT_DEBUGFS_SUMMARY}")
        maybe_extract_log(args.stall_dir or args.from_dir or args.output)
        return 0

    if not (args.mds or args.daemon):
        parser.error("one of --mds, --daemon, or --from-dir is required")

    if args.output is None:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        target_name = (args.mds or args.daemon or "mds").replace(":", "_").replace("/", "_")
        args.output = Path.cwd() / f"mds-stall-{target_name}-{ts}"

    ceph = CephMDSClient(conf=args.conf, timeout=args.timeout)
    if args.mds:
        mds_target = args.mds
        invoke = lambda cmd: ceph.tell(mds_target, cmd)
    else:
        mds_target = args.daemon
        invoke = lambda cmd: ceph.daemon(args.daemon, cmd)

    try:
        collect(
            client=ceph,
            invoke=invoke,
            mds_target=mds_target,
            output_dir=args.output,
            reqid_filter=args.reqid,
            all_ops=args.all_ops,
            follow_parents=args.follow_parents,
            max_inodes=args.max_inodes,
            skip_idle_diagnostics=args.skip_idle_diagnostics,
            session_cap_dump=args.session_cap_dump,
            **metrics_kwargs(),
        )
    except RuntimeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    print(f"Wrote diagnostics to {args.output}")
    print(f"  report: {args.output / 'report.txt'}")
    print(f"  graph:  {args.output / 'graph.json'}")
    print(f"  revokes:{args.output / 'cap_revokes.json'}")
    if (args.output / "idle_summary.json").exists():
        print(f"  idle:   {args.output / 'idle_summary.json'}")
    if (args.output / PROMETHEUS_TIMESERIES_ARTIFACT).exists():
        print(f"  metrics:{args.output / PROMETHEUS_TIMESERIES_ARTIFACT}")
    if (args.output / CLIENT_DEBUGFS_SUMMARY).exists():
        print(f"  debugfs:{args.output / CLIENT_DEBUGFS_SUMMARY}")
    maybe_extract_log(args.stall_dir or args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
