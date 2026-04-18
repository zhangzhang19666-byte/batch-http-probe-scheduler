#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FOR TESTING PURPOSES ONLY. DO NOT USE IN PRODUCTION.

Asynchronous HTTP Metadata Probe Scheduler
- Commit every single record (DELETE journal, no WAL side-files)
- --max-minutes: self-terminates gracefully before GitHub Actions job timeout
- SIGTERM handler: flushes pending write before exit
- Quota-aware retry logic (up to N rounds)
"""

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── runtime config ───────────────────────────────────────────────────────
_ENDPOINT   = os.environ.get("PROBE_ENDPOINT", "")
_BATCH      = 200
COMMIT_EVERY = 1            # commit after every single record
STATS_EVERY  = 50           # print progress every N records
_RETRY_WAITS = [90, 90, 90]

# ── logging ──────────────────────────────────────────────────────────────

def _init_log(log_file: Path):
    fmt     = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


log = logging.getLogger(__name__)

# ── db layer ─────────────────────────────────────────────────────────────

_conn: sqlite3.Connection | None = None

def _open_db(path: Path) -> sqlite3.Connection:
    global _conn
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS magnet_results (
            magnet_url  TEXT PRIMARY KEY,
            status      TEXT DEFAULT 'pending',
            raw_json    TEXT,
            checked_at  TEXT,
            created_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_status ON magnet_results(status);
    """)
    conn.commit()
    _conn = conn
    return conn


def _pending(conn, n, row_start=0, row_end=0):
    if row_end > 0:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results "
            "WHERE status='pending' AND rowid >= ? AND rowid <= ? LIMIT ?",
            (row_start, row_end, n)
        ).fetchall()]
    elif row_start > 0:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results "
            "WHERE status='pending' AND rowid >= ? LIMIT ?",
            (row_start, n)
        ).fetchall()]
    else:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results WHERE status='pending' LIMIT ?", (n,)
        ).fetchall()]


def _quota_limited(conn, row_start=0, row_end=0):
    if row_end > 0:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results "
            "WHERE status='quota_limited' AND rowid >= ? AND rowid <= ?",
            (row_start, row_end)
        ).fetchall()]
    elif row_start > 0:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results "
            "WHERE status='quota_limited' AND rowid >= ?",
            (row_start,)
        ).fetchall()]
    else:
        return [r[0] for r in conn.execute(
            "SELECT magnet_url FROM magnet_results WHERE status='quota_limited'"
        ).fetchall()]


def _bulk_save(conn, rows: list[tuple]):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn.executemany(
        "UPDATE magnet_results SET status=?, raw_json=?, checked_at=? WHERE magnet_url=?",
        [(status, json.dumps(payload, ensure_ascii=False), ts, key)
         for key, status, payload in rows],
    )
    conn.commit()
    log.debug("committed %d rows to disk", len(rows))


def _counts(conn):
    return {r[0]: r[1] for r in conn.execute(
        "SELECT status, COUNT(*) FROM magnet_results GROUP BY status"
    ).fetchall()}

# ── probe logic ──────────────────────────────────────────────────────────

def _probe(target: str) -> tuple[str, dict]:
    try:
        r = requests.get(_ENDPOINT, params={"url": target}, timeout=30)
        d = r.json()
        if d.get("error") == "quota_limited":
            return "quota_limited", d
        if d.get("screenshots"):
            return "success", d
        return "failed", d
    except requests.exceptions.Timeout:
        return "failed", {"error": "timeout"}
    except requests.exceptions.ConnectionError as e:
        return "failed", {"error": str(e)}
    except Exception as e:
        return "failed", {"error": str(e)}

# ── time budget ───────────────────────────────────────────────────────────

_deadline: float = float("inf")   # epoch seconds, set in run()

def _time_left() -> float:
    return _deadline - time.monotonic()

def _over_budget() -> bool:
    return time.monotonic() >= _deadline

# ── batch runner ─────────────────────────────────────────────────────────

def _run_batch(conn, items: list[str], delay: float, tag: str = "") -> tuple[list[str], bool]:
    """
    Returns (quota_urls, timed_out).
    timed_out=True means deadline was reached mid-batch; caller should stop.
    """
    quota: list[str] = []
    pending_rows: list[tuple] = []

    def _flush():
        if pending_rows:
            _bulk_save(conn, pending_rows)
            pending_rows.clear()

    for i, item in enumerate(items, 1):
        if _over_budget():
            _flush()
            log.warning("time budget exhausted at %s[%d/%d] – saving and exiting",
                        tag, i, len(items))
            return quota, True

        lbl = f"{tag}[{i:>5}/{len(items)}]"
        status, data = _probe(item)
        pending_rows.append((item, status, data))

        if status == "success":
            log.info("%s  ok", lbl)
        elif status == "quota_limited":
            log.warning("%s  quota – queued for retry", lbl)
            quota.append(item)
        else:
            log.info("%s  failed  err=%s", lbl, data.get("error", "-"))

        if i % COMMIT_EVERY == 0:
            _flush()
        if i % STATS_EVERY == 0:
            log.info("time left: %.1f min", _time_left() / 60)
            _print_stats(conn, f"subtotal {i}/{len(items)}")

        time.sleep(delay)

    _flush()
    return quota, False


def _print_stats(conn, label="stats"):
    c = _counts(conn)
    log.info(
        "-- %s -- total=%d  ok=%d  fail=%d  quota=%d  pending=%d",
        label, sum(c.values()),
        c.get("success", 0), c.get("failed", 0),
        c.get("quota_limited", 0), c.get("pending", 0),
    )

# ── signal handler ────────────────────────────────────────────────────────

def _handle_sigterm(signum, frame):
    log.warning("SIGTERM received – flushing DB and exiting")
    if _conn:
        try:
            _conn.commit()
        except Exception:
            pass
        _conn.close()
    sys.exit(0)

# ── entry point ───────────────────────────────────────────────────────────

def run(db_path: Path, delay: float, status_only: bool, max_minutes: float,
        row_start: int = 0, row_end: int = 0):
    global _deadline

    if not _ENDPOINT:
        log.error("PROBE_ENDPOINT env var not set")
        sys.exit(1)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    # set deadline: exit max_minutes from now so Sync dataset has time to run
    _deadline = time.monotonic() + max_minutes * 60
    log.info("time budget: %.0f min (deadline in %.0f min)", max_minutes, max_minutes)
    if row_start or row_end:
        log.info("row range: %d – %s", row_start, row_end if row_end else "end")

    conn = _open_db(db_path)
    _print_stats(conn, "startup")

    if status_only:
        conn.close()
        return

    log.info("=" * 60)
    log.info("phase 1: pending  (delay=%.1fs  rows=%d-%s)",
             delay, row_start, row_end if row_end else "end")
    log.info("=" * 60)
    done = 0
    while not _over_budget():
        batch = _pending(conn, _BATCH, row_start, row_end)
        if not batch:
            log.info("all pending processed in range (%d total)", done)
            break
        _, timed_out = _run_batch(conn, batch, delay, tag="P")
        done += len(batch)
        _print_stats(conn, f"after {done} records")
        if timed_out:
            break

    if _over_budget():
        log.warning("time budget reached – progress saved, next run resumes here")
        _print_stats(conn, "exit snapshot")
        conn.close()
        return

    for rnd, wait in enumerate(_RETRY_WAITS, 1):
        limited = _quota_limited(conn, row_start, row_end)
        if not limited:
            log.info("no quota_limited – skip retry")
            break
        if _time_left() < wait + 60:
            log.warning("not enough time for retry round %d – deferring to next run", rnd)
            break
        log.info("=" * 60)
        log.info("phase 2 round %d/%d: %d items, waiting %ds", rnd, len(_RETRY_WAITS), len(limited), wait)
        log.info("=" * 60)
        time.sleep(wait)
        remaining, timed_out = _run_batch(conn, limited, delay, tag=f"R{rnd}")  # noqa: E501
        log.info("round %d: resolved=%d  still_limited=%d", rnd, len(limited) - len(remaining), len(remaining))
        _print_stats(conn, f"after retry round {rnd}")
        if not remaining:
            log.info("all quota_limited resolved")
            break
        if timed_out:
            break
    else:
        leftover = len(_quota_limited(conn))
        if leftover:
            log.warning("%d still quota_limited – resume next run", leftover)

    _print_stats(conn, "final")
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Async HTTP Metadata Probe Scheduler")
    p.add_argument("--db",          "-d", required=True)
    p.add_argument("--delay",       "-D", type=float, default=2.4)
    p.add_argument("--status",      "-s", action="store_true")
    p.add_argument("--max-minutes", "-m", type=float, default=300,
                   help="stop processing after N minutes so Sync has time to run (default 300)")
    p.add_argument("--row-start", type=int, default=0,
                   help="process only rows with rowid >= N (0 = from beginning)")
    p.add_argument("--row-end",   type=int, default=0,
                   help="process only rows with rowid <= N (0 = no upper limit)")
    args = p.parse_args()

    db_path  = Path(args.db)
    log_file = db_path.parent / "probe_scheduler.log"
    _init_log(log_file)

    log.info("db=%s  delay=%.1fs  max_minutes=%.0f", db_path, args.delay, args.max_minutes)

    if not db_path.exists():
        log.error("db not found: %s", db_path)
        sys.exit(1)

    try:
        run(db_path, args.delay, args.status, args.max_minutes,
            args.row_start, args.row_end)
    except KeyboardInterrupt:
        log.warning("interrupted – progress saved")
        if _conn:
            _conn.commit()
            _conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
