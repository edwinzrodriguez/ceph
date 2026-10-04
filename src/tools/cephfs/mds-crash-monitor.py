#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil -*-
# vim: ts=4 sw=4 expandtab
#
# Watch an MDS admin socket and, when the daemon disappears (crash/abort),
# collect stall diagnostics, a gdb backtrace + reactor queue dump from the
# newest core, and copies of the MDS log / settings into the stall directory.
#
# Example:
#   ./mds-crash-monitor.py \
#       --asok /var/run/ceph/ceph-mds.mds_reactor.mon-000.0.asok \
#       --output-dir /cephfs_perf/results/my-run \
#       --crash-dir /crash \
#       --ceph-mds /home/root/usr/local/wip-mds-hotpath-phase3/bin/ceph-mds
#

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence


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


def asok_alive(asok: Path, timeout: float) -> bool:
    """True if the admin socket exists and answers a cheap command."""
    if not asok.is_socket() and not asok.exists():
        return False
    try:
        proc = subprocess.run(
            ["ceph", "--admin-daemon", str(asok), "status"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False
    # Any successful JSON/status reply means the daemon is up. Connection /
    # missing-socket failures return non-zero.
    return proc.returncode == 0


def wait_until_alive(
    asok: Path,
    interval: float,
    probe_timeout: float,
) -> None:
    log(f"waiting for MDS asok to become healthy: {asok}")
    while True:
        if asok_alive(asok, probe_timeout):
            log("MDS asok is healthy")
            return
        time.sleep(interval)


def watch_until_dead(
    asok: Path,
    interval: float,
    probe_timeout: float,
    fail_threshold: int,
) -> float:
    """
    Poll until the asok fails ``fail_threshold`` times in a row after having
    been alive. Returns monotonic time of the first consecutive failure.
    """
    log(f"watching {asok} (interval={interval}s, fail_threshold={fail_threshold})")
    consecutive_fails = 0
    first_fail_at: Optional[float] = None
    while True:
        if asok_alive(asok, probe_timeout):
            if consecutive_fails:
                log("MDS asok recovered; resetting failure streak")
            consecutive_fails = 0
            first_fail_at = None
        else:
            consecutive_fails += 1
            if first_fail_at is None:
                first_fail_at = time.monotonic()
            log(
                f"MDS asok probe failed ({consecutive_fails}/{fail_threshold})"
            )
            if consecutive_fails >= fail_threshold:
                assert first_fail_at is not None
                return first_fail_at
        time.sleep(interval)


def derive_mds_log(asok: Path, log_dir: Path) -> Path:
    # ceph-mds.mds_reactor.mon-000.0.asok -> ceph-mds.mds_reactor.mon-000.0.log
    return log_dir / f"{asok.name.removesuffix('.asok')}.log"


def run_stall_debug(
    *,
    cwd: Path,
    stall_debug: Path,
    asok: Path,
    client_hosts: Sequence[str],
    grafana_url: str,
    metrics_step: str,
    extra_args: Sequence[str],
) -> Path:
    """
    Run mds-stall-debug.py with cwd=output-dir so it creates
    mds-stall-<name>-<ts> underneath. Returns that directory.
    """
    before = {p.resolve() for p in cwd.glob("mds-stall-*") if p.is_dir()}
    cmd: List[str] = [
        sys.executable,
        str(stall_debug),
        "--daemon",
        str(asok),
        "--client-debugfs",
        "--grafana-url",
        grafana_url,
        "--metrics-step",
        metrics_step,
    ]
    for host in client_hosts:
        cmd.extend(["--client-host", host])
    cmd.extend(extra_args)

    log(f"running stall debug in {cwd}: {' '.join(cmd)}")
    # Stall-debug may fail parts of live asok collection after a crash; still
    # keep going so we can attach gdb / copy logs into whatever dir it made.
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    if proc.returncode != 0:
        log(f"warning: mds-stall-debug.py exited {proc.returncode}")

    after = [p for p in cwd.glob("mds-stall-*") if p.is_dir()]
    created = [p for p in after if p.resolve() not in before]
    candidates = created if created else after
    if not candidates:
        # Fallback: create a capture dir ourselves so gdb/log still land somewhere.
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        fallback = cwd / f"mds-stall-{asok.name}-{ts}"
        fallback.mkdir(parents=True, exist_ok=True)
        log(f"no stall dir created; using fallback {fallback}")
        return fallback
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    log(f"stall directory: {newest}")
    return newest


def looks_like_core(path: Path) -> bool:
    name = path.name
    if path.is_dir() or path.is_symlink():
        return False
    if name.startswith("."):
        return False
    if name in {"README", "README.txt"}:
        return False
    # Common layouts: bare "core", core.<pid>, core-ceph-mds-*, abrt dumps, etc.
    if name == "core" or name.startswith("core.") or name.startswith("core-"):
        return True
    if "coredump" in name.lower() or name.endswith(".core"):
        return True
    # ELF note: cheap magic check
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
    """
    Return the newest core under crash_dir. If ``not_before`` (epoch seconds)
    is set, prefer cores with mtime >= that; wait up to ``wait_s`` for one.
    """
    if not crash_dir.is_dir():
        log(f"warning: crash dir does not exist: {crash_dir}")
        return None

    deadline = time.monotonic() + wait_s
    best: Optional[Path] = None
    while True:
        cores = [p for p in crash_dir.rglob("*") if looks_like_core(p)]
        if not_before is not None:
            fresh = [p for p in cores if p.stat().st_mtime >= not_before]
            pool = fresh if fresh else []
        else:
            pool = cores
        if pool:
            best = max(pool, key=lambda p: p.stat().st_mtime)
            log(f"found core: {best}")
            return best
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_s)

    # Fall back to absolute newest core even if older than crash detection.
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


def copy_artifacts(
    stall_dir: Path,
    *,
    mds_log: Optional[Path],
    settings: Optional[Path],
) -> None:
    if mds_log is not None:
        if mds_log.is_file():
            dest = stall_dir / mds_log.name
            log(f"copying {mds_log} -> {dest}")
            shutil.copy2(mds_log, dest)
        else:
            log(f"warning: MDS log not found: {mds_log}")
    if settings is not None:
        if settings.is_file():
            dest = stall_dir / settings.name
            log(f"copying {settings} -> {dest}")
            shutil.copy2(settings, dest)
        else:
            log(f"warning: settings file not found: {settings}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Monitor an MDS admin socket and collect stall-debug + gdb "
            "artifacts when the daemon crashes/aborts."
        ),
    )
    p.add_argument(
        "--asok",
        type=Path,
        required=True,
        help="MDS admin socket path to watch",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory in which mds-stall-debug.py creates its stall folder",
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
        help="MDS log to copy into the stall dir "
        "(default: /var/log/ceph/<asok-basename>.log)",
    )
    p.add_argument(
        "--log-dir",
        type=Path,
        default=Path("/var/log/ceph"),
        help="Directory used when deriving --mds-log from the asok name",
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
        help="Client host for --client-debugfs (repeatable; "
        f"default: {', '.join(DEFAULT_CLIENT_HOSTS)})",
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
        help="Consecutive failed probes before treating MDS as crashed (default: 2)",
    )
    p.add_argument(
        "--core-wait",
        type=float,
        default=30.0,
        help="Seconds to wait for a fresh core after crash detection (default: 30)",
    )
    p.add_argument(
        "--no-wait-alive",
        action="store_true",
        help="Do not wait for the asok to be healthy before watching",
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
    mds_log = (
        args.mds_log.expanduser()
        if args.mds_log is not None
        else derive_mds_log(asok, args.log_dir.expanduser())
    )
    settings = args.mds_settings.expanduser() if args.mds_settings else None
    client_hosts = args.client_host if args.client_host else list(DEFAULT_CLIENT_HOSTS)

    output_dir.mkdir(parents=True, exist_ok=True)

    if not stall_debug.is_file():
        log(f"error: stall-debug script not found: {stall_debug}")
        return 2
    if not ceph_mds.is_file():
        log(f"error: ceph-mds binary not found: {ceph_mds}")
        return 2
    if not gdb_macro.is_file():
        log(f"warning: gdb macro not found (gdb source may fail): {gdb_macro}")

    if not args.no_wait_alive:
        wait_until_alive(asok, args.interval, args.probe_timeout)

    # Record wall time just before watch so we can prefer cores written after.
    watch_started_wall = time.time()
    watch_until_dead(
        asok,
        args.interval,
        args.probe_timeout,
        max(1, args.fail_threshold),
    )
    log("MDS appears down; collecting crash artifacts")

    # Prefer cores written around/after we started watching (minus a small
    # skew), not ancient dumps left in the crash dir.
    not_before = watch_started_wall - 5.0

    stall_dir = run_stall_debug(
        cwd=output_dir,
        stall_debug=stall_debug,
        asok=asok,
        client_hosts=client_hosts,
        grafana_url=args.grafana_url,
        metrics_step=args.metrics_step,
        extra_args=args.stall_debug_arg,
    )

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
        # Also record which core we used.
        (stall_dir / "core_path.txt").write_text(str(core.resolve()) + "\n", encoding="utf-8")
    else:
        log("skipping gdb capture (no core)")

    copy_artifacts(stall_dir, mds_log=mds_log, settings=settings)
    log(f"done: artifacts in {stall_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
