#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil -*-
# vim: ts=4 sw=4 expandtab
#
# Distributed crash monitor for MDS + ceph-fuse.
#
# Watches the MDS admin socket (local) and ceph-fuse asoks on --client-host
# peers (via SSH). At watch start, records each process's log path with
# `config get log_file`. When ANY monitored process dies:
#   1. `log dump` surviving processes
#   2. copy all known logs into one stall directory
#   3. run mds-stall-debug.py for MDS/client diagnostics
#   4. if the MDS died: gdb the newest core + copy settings
#
# Example:
#   ./mds-crash-monitor.py \
#       --asok /var/run/ceph/ceph-mds.mds_reactor.mon-000.0.asok \
#       --output-dir /cephfs_perf/results/my-run \
#       --client-host client-000 --client-host client-001 \
#       --crash-dir /crash \
#       --ceph-mds /home/root/usr/local/wip-mds-hotpath-phase3/bin/ceph-mds
#

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


DEFAULT_CLIENT_HOSTS = [
    "client-000",
    "client-001",
    "client2-000",
    "client2-001",
    "client2-002",
    "client2-003",
]


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def load_stall_debug(stall_debug: Path) -> types.ModuleType:
    """Import mds-stall-debug.py helpers (hyphenated filename)."""
    name = "mds_stall_debug_helpers"
    spec = importlib.util.spec_from_file_location(name, stall_debug)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {stall_debug}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def wait_inventory_alive(
    sd: types.ModuleType,
    inventory: List[Dict[str, Any]],
    *,
    interval: float,
    probe_timeout: float,
    ssh_user: Optional[str],
) -> None:
    log(f"waiting for {len(inventory)} monitored asok(s) to become healthy")
    while True:
        pending = [
            p for p in inventory
            if not sd.probe_monitored_process(p, probe_timeout, ssh_user=ssh_user)
        ]
        if not pending:
            log("all monitored asoks are healthy")
            return
        names = ", ".join(
            f"{p.get('role')}:{p.get('host')}" for p in pending[:6]
        )
        if len(pending) > 6:
            names += f", +{len(pending) - 6} more"
        log(f"still waiting ({len(pending)}): {names}")
        time.sleep(interval)


def watch_inventory_until_any_dead(
    sd: types.ModuleType,
    inventory: List[Dict[str, Any]],
    *,
    interval: float,
    probe_timeout: float,
    fail_threshold: int,
    ssh_user: Optional[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Poll every monitored process. Return (crashed_proc, meta) when any one
    fails ``fail_threshold`` consecutive probes.
    """
    threshold = max(1, fail_threshold)
    log(
        f"watching {len(inventory)} process(es) "
        f"(interval={interval}s, fail_threshold={threshold})"
    )
    for proc in inventory:
        log(
            "  {role} {host} asok={asok} log_file={log} ({src})".format(
                role=proc.get("role"),
                host=proc.get("host"),
                asok=proc.get("asok"),
                log=proc.get("log_file"),
                src=proc.get("log_file_source"),
            )
        )

    fails: Dict[str, int] = {p["id"]: 0 for p in inventory}
    first_fail_at: Dict[str, float] = {}

    while True:
        for proc in inventory:
            pid = proc["id"]
            alive = sd.probe_monitored_process(
                proc, probe_timeout, ssh_user=ssh_user,
            )
            if alive:
                if fails[pid]:
                    log(f"{pid} recovered; resetting failure streak")
                fails[pid] = 0
                first_fail_at.pop(pid, None)
                continue

            fails[pid] += 1
            if pid not in first_fail_at:
                first_fail_at[pid] = time.monotonic()
            log(f"{pid} probe failed ({fails[pid]}/{threshold})")
            if fails[pid] >= threshold:
                meta = {
                    "first_fail_monotonic": first_fail_at[pid],
                    "detected_at": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "crashed_id": pid,
                }
                return proc, meta
        time.sleep(interval)


def run_stall_debug(
    *,
    stall_dir: Path,
    stall_debug: Path,
    mds_asok: Path,
    client_hosts: Sequence[str],
    grafana_url: str,
    metrics_step: str,
    extra_args: Sequence[str],
    ssh_user: Optional[str] = None,
) -> None:
    """Run mds-stall-debug.py into an existing stall directory."""
    cmd: List[str] = [
        sys.executable,
        str(stall_debug),
        "--daemon",
        str(mds_asok),
        "--output",
        str(stall_dir),
        "--client-debugfs",
        "--grafana-url",
        grafana_url,
        "--metrics-step",
        metrics_step,
    ]
    for host in client_hosts:
        cmd.extend(["--client-host", host])
    if ssh_user:
        cmd.extend(["--client-ssh-user", ssh_user])
    cmd.extend(extra_args)

    log(f"running stall debug: {' '.join(cmd)}")
    # Stall-debug may fail parts of live asok collection after a crash; still
    # keep going so we can attach gdb / keep log captures already written.
    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0:
        log(f"warning: mds-stall-debug.py exited {proc.returncode}")


def looks_like_core(path: Path) -> bool:
    name = path.name
    if path.is_dir() or path.is_symlink():
        return False
    if name.startswith("."):
        return False
    if name in {"README", "README.txt"}:
        return False
    if name == "core" or name.startswith("core.") or name.startswith("core-"):
        return True
    if "coredump" in name.lower() or name.endswith(".core"):
        return True
    try:
        with path.open("rb") as f:
            magic = f.read(4)
        return magic == b"\x7fELF"
    except OSError:
        return False


def find_newest_core(
    crash_dir: Path,
    *,
    not_before: Optional[float],
    wait_s: float,
    poll_s: float,
) -> Optional[Path]:
    if not crash_dir.is_dir():
        log(f"warning: crash dir does not exist: {crash_dir}")
        return None

    deadline = time.monotonic() + wait_s
    while True:
        cores = [p for p in crash_dir.rglob("*") if looks_like_core(p)]
        if not_before is not None:
            pool = [p for p in cores if p.stat().st_mtime >= not_before]
        else:
            pool = cores
        if pool:
            best = max(pool, key=lambda p: p.stat().st_mtime)
            log(f"found core: {best}")
            return best
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_s)

    cores = [p for p in crash_dir.rglob("*") if looks_like_core(p)]
    if not cores:
        log(f"no core files found under {crash_dir}")
        return None
    best = max(cores, key=lambda p: p.stat().st_mtime)
    log(f"warning: no fresh core; using newest available: {best}")
    return best


def run_gdb_capture(
    *,
    stall_dir: Path,
    ceph_mds: Path,
    core: Path,
    gdb_macro: Path,
    gdb_bin: str,
) -> Path:
    out = stall_dir / "gdb_dump.log"
    cmd = [
        gdb_bin,
        "--batch",
        "-q",
        "-ex",
        "thread apply all bt",
        "-ex",
        f"source {gdb_macro}",
        "-ex",
        "mds-reactor-queue /kinds /msgs /age",
        "-ex",
        "quit",
        str(ceph_mds),
        str(core),
    ]
    log(f"running gdb -> {out}")
    with out.open("w", encoding="utf-8") as fh:
        fh.write(f"# cmd: {' '.join(cmd)}\n")
        fh.flush()
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, check=False)
    if proc.returncode != 0:
        log(f"warning: gdb exited {proc.returncode} (see {out})")
    else:
        log(f"wrote {out}")
    return out


def copy_settings(stall_dir: Path, settings: Optional[Path]) -> None:
    if settings is None:
        return
    if settings.is_file():
        dest = stall_dir / settings.name
        log(f"copying {settings} -> {dest}")
        shutil.copy2(settings, dest)
    else:
        log(f"warning: settings file not found: {settings}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Distributed monitor for MDS + ceph-fuse asoks. When any monitored "
            "process crashes, dump survivor logs, collect them, and run "
            "mds-stall-debug.py (plus gdb if the MDS died)."
        ),
    )
    p.add_argument(
        "--asok",
        type=Path,
        required=True,
        help="MDS admin socket path to watch (local)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory in which the stall capture folder is created",
    )
    p.add_argument(
        "--crash-dir",
        type=Path,
        default=Path("/crash"),
        help="Directory to search for the newest core dump (default: /crash)",
    )
    p.add_argument(
        "--stall-debug",
        type=Path,
        default=Path("/cephfs_perf/mds-stall-debug.py"),
        help="Path to mds-stall-debug.py",
    )
    p.add_argument(
        "--gdb-macro",
        type=Path,
        default=Path("/cephfs_perf/mds-reactor-queue-gdb.py"),
        help="Path to mds-reactor-queue-gdb.py",
    )
    p.add_argument(
        "--ceph-mds",
        type=Path,
        default=Path("/home/root/usr/local/wip-mds-hotpath-phase3/bin/ceph-mds"),
        help="ceph-mds binary matching the core (for gdb)",
    )
    p.add_argument(
        "--gdb",
        default="gdb",
        help="gdb binary (default: gdb)",
    )
    p.add_argument(
        "--mds-log",
        type=Path,
        default=None,
        help="Optional MDS log path override "
        "(default: asok `config get log_file`, else "
        "/var/log/ceph/<asok-basename>.log)",
    )
    p.add_argument(
        "--log-dir",
        type=Path,
        default=Path("/var/log/ceph"),
        help="Fallback directory when deriving MDS/fuse log names",
    )
    p.add_argument(
        "--fuse-asok-dir",
        type=Path,
        default=Path("/var/run/ceph"),
        help="Directory for ceph-fuse-<host>.asok on clients "
        "(default: /var/run/ceph)",
    )
    p.add_argument(
        "--mds-settings",
        type=Path,
        default=Path("/etc/ceph/mds-settings.conf"),
        help="Settings file to copy into the stall dir",
    )
    p.add_argument(
        "--client-host",
        action="append",
        default=None,
        help="Client host whose ceph-fuse asok is monitored (repeatable; "
        f"default: {', '.join(DEFAULT_CLIENT_HOSTS)})",
    )
    p.add_argument(
        "--client-ssh-user",
        help="SSH username for probing/copying from --client-host",
    )
    p.add_argument(
        "--no-fuse-watch",
        action="store_true",
        help="Only watch the MDS asok (legacy single-process mode)",
    )
    p.add_argument(
        "--grafana-url",
        default="http://mon-000:3000",
        help="Passed through to mds-stall-debug.py",
    )
    p.add_argument(
        "--metrics-step",
        default="5s",
        help="Passed through to mds-stall-debug.py",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between asok probes (default: 2)",
    )
    p.add_argument(
        "--probe-timeout",
        type=float,
        default=5.0,
        help="Timeout for each asok probe (default: 5)",
    )
    p.add_argument(
        "--fail-threshold",
        type=int,
        default=2,
        help="Consecutive failed probes before treating a process as crashed "
        "(default: 2)",
    )
    p.add_argument(
        "--core-wait",
        type=float,
        default=30.0,
        help="Seconds to wait for a fresh core after MDS crash (default: 30)",
    )
    p.add_argument(
        "--no-wait-alive",
        action="store_true",
        help="Do not wait for monitored asoks to be healthy before watching",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout for log dump / log copy operations (default: 120)",
    )
    p.add_argument(
        "--stall-debug-arg",
        action="append",
        default=[],
        help="Extra arg forwarded to mds-stall-debug.py (repeatable)",
    )
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    asok: Path = args.asok.expanduser()
    output_dir: Path = args.output_dir.expanduser()
    crash_dir: Path = args.crash_dir.expanduser()
    stall_debug: Path = args.stall_debug.expanduser()
    gdb_macro: Path = args.gdb_macro.expanduser()
    ceph_mds: Path = args.ceph_mds.expanduser()
    mds_log_override = (
        str(args.mds_log.expanduser()) if args.mds_log is not None else None
    )
    settings = args.mds_settings.expanduser() if args.mds_settings else None
    client_hosts = args.client_host if args.client_host else list(DEFAULT_CLIENT_HOSTS)
    ssh_user = args.client_ssh_user

    output_dir.mkdir(parents=True, exist_ok=True)

    if not stall_debug.is_file():
        log(f"error: stall-debug script not found: {stall_debug}")
        return 2
    if not ceph_mds.is_file():
        log(f"error: ceph-mds binary not found: {ceph_mds}")
        return 2
    if not gdb_macro.is_file():
        log(f"warning: gdb macro not found (gdb source may fail): {gdb_macro}")

    try:
        sd = load_stall_debug(stall_debug)
    except Exception as exc:  # noqa: BLE001 - surface import errors clearly
        log(f"error: failed to import helpers from {stall_debug}: {exc}")
        return 2

    inventory = sd.build_cluster_watch_inventory(
        mds_asok=str(asok),
        mds_host=None,  # local MDS
        mds_log=mds_log_override,
        client_hosts=None if args.no_fuse_watch else client_hosts,
        asok_dir=str(args.fuse_asok_dir.expanduser()),
        client_fuse_log_dir=str(args.log_dir.expanduser()),
        ssh_user=ssh_user,
        timeout=min(args.timeout, 30),
        include_local_fuse=False,
    )
    if not inventory:
        log("error: empty watch inventory")
        return 2

    inv_path = output_dir / "cluster_watch_inventory.json"
    inv_path.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    log(f"wrote inventory: {inv_path}")

    if not args.no_wait_alive:
        wait_inventory_alive(
            sd,
            inventory,
            interval=args.interval,
            probe_timeout=args.probe_timeout,
            ssh_user=ssh_user,
        )

    watch_started_wall = time.time()
    crashed, meta = watch_inventory_until_any_dead(
        sd,
        inventory,
        interval=args.interval,
        probe_timeout=args.probe_timeout,
        fail_threshold=args.fail_threshold,
        ssh_user=ssh_user,
    )
    crashed_role = str(crashed.get("role") or "unknown")
    crashed_asok = str(crashed.get("asok") or "")
    log(
        f"{crashed_role} appears down ({crashed.get('id')}); "
        "dumping survivors and collecting artifacts"
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    stall_dir = output_dir / f"mds-stall-{crashed_role}-{ts}"
    stall_dir.mkdir(parents=True, exist_ok=True)

    # 1) Dump survivors + copy all logs (fast path before stall-debug).
    sd.collect_on_monitored_abort(
        output_dir=stall_dir,
        crashed_asok=crashed_asok,
        crashed_role=crashed_role,
        inventory=inventory,
        ssh_user=ssh_user,
        timeout=args.timeout,
        watch_meta=meta,
        event=f"{crashed_role}-abort",
    )

    # 2) Stall diagnostics (MDS asok may be dead; best-effort).
    run_stall_debug(
        stall_dir=stall_dir,
        stall_debug=stall_debug,
        mds_asok=asok,
        client_hosts=client_hosts,
        grafana_url=args.grafana_url,
        metrics_step=args.metrics_step,
        extra_args=args.stall_debug_arg,
        ssh_user=ssh_user,
    )

    # 3) MDS-only: gdb core + settings.
    if crashed_role == "mds":
        not_before = watch_started_wall - 5.0
        core = find_newest_core(
            crash_dir,
            not_before=not_before,
            wait_s=args.core_wait,
            poll_s=1.0,
        )
        if core is not None:
            run_gdb_capture(
                stall_dir=stall_dir,
                ceph_mds=ceph_mds,
                core=core,
                gdb_macro=gdb_macro,
                gdb_bin=args.gdb,
            )
            (stall_dir / "core_path.txt").write_text(
                str(core.resolve()) + "\n", encoding="utf-8",
            )
        else:
            log("skipping gdb capture (no core)")
        copy_settings(stall_dir, settings)
    else:
        log(f"skipping gdb (crash was {crashed_role}, not mds)")
        copy_settings(stall_dir, settings)

    log(f"done: artifacts in {stall_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
