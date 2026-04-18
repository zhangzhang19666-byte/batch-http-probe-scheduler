#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FOR TESTING PURPOSES ONLY. DO NOT USE IN PRODUCTION.

Asynchronous HTTP Metadata Probe Scheduler
- Batch-commit SQLite writes (every COMMIT_EVERY records)
- DELETE journal mode: writes go directly to the main file, no WAL
- SIGTERM handler: flushes pending batch before exit (safe for GitHub Actions cancel)
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
_ENDPOINT    = os.environ.get("PROBE_ENDPOINT", "")
_BATCH       = 200          # rows fetched per DB query
COMMIT_EVERY = 1            # flush to disk after every single record
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

_conn: sqlite3.Connection | None = None   # module-level for signal handler

def _open_db(path: Path) -> sqlite3.Connection:
    global _conn
    conn = sqlite3.connect(path)
    # DELETE journal: every commit writes directly to the main file.
    # No WAL side-files, so cp results_all.db always captures the full state.
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


def _pending(conn: sqlite3.Connection, n: int) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT magnet_url FROM magnet_results WHERE status='pending' LIMIT ?", (n,)
    ).fetchall()]


def _quota_limited(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT magnet_url FROM magnet_results WHERE status='quota_limited'"
    ).fetchall()]


def _bulk_save(conn: sqlite3.Connection, rows: list[tuple]):
    """Write a batch of (status, raw_json, checked_at, magnet_url) rows in one transaction."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn.executemany(
        "UPDATE magnet_results SET status=?, raw_json=?, checked_at=? WHERE magnet_url=?",
        [(status, json.dumps(payload, ensure_ascii=False), ts, key)
         for key, status, payload in rows],
    )
    conn.commit()
    log.debug("committed %d rows to disk", len(rows))


def _counts(conn: sqlite3.Connection) -> dict:
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

# ── batch runner ─────────────────────────────────────────────────────────

def _run_batch(conn, items: list[str], delay: float, tag: str = "") -> list[str]:
    """
    Process items one by one.
    Accumulate results in memory, flush to DB every COMMIT_EVERY records.
    Returns list of still-quota-limited URLs.
    """
    quota   = []
    pending_rows: list[tuple] = []   # (key, status, payload)

    def _flush():
        if pending_rows:
            _bulk_save(conn, pending_rows)
            pending_rows.clear()

    for i, item in enumerate(items, 1):
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

        # batch commit
        if i % COMMIT_EVERY == 0:
            _flush()
            _print_stats(conn, f"subtotal {i}/{len(items)}")

        time.sleep(delay)

    _flush()   # commit any remaining rows
    return quota


def _print_stats(conn, label="stats"):
    c = _counts(conn)
    log.info(
        "-- %s -- total=%d  ok=%d  fail=%d  quota=%d  pending=%d",
        label, sum(c.values()),
        c.get("success", 0), c.get("failed", 0),
        c.get("quota_limited", 0), c.get("pending", 0),
    )

# ── signal handler (GitHub Actions cancel = SIGTERM) ─────────────────────

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

def run(db_path: Path, delay: float, status_only: bool):
    if not _ENDPOINT:
        log.error("PROBE_ENDPOINT env var not set")
        sys.exit(1)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    conn = _open_db(db_path)
    _print_stats(conn, "startup")

    if status_only:
        conn.close()
        return

    # phase 1 – pending
    log.info("=" * 60)
    log.info("phase 1: pending records  (delay=%.1fs  commit_every=%d)",
             delay, COMMIT_EVERY)
    log.info("=" * 60)
    done = 0
    while True:
        batch = _pending(conn, _BATCH)
        if not batch:
            log.info("all pending processed (%d total)", done)
            break
        _run_batch(conn, batch, delay, tag="P")
        done += len(batch)
        _print_stats(conn, f"after {done} records")

    # phase 2 – quota retry
    for rnd, wait in enumerate(_RETRY_WAITS, 1):
        limited = _quota_limited(conn)
        if not limited:
            log.info("no quota_limited records – skip retry")
            break
        log.info("=" * 60)
        log.info("phase 2 round %d/%d: %d items, waiting %ds",
                 rnd, len(_RETRY_WAITS), len(limited), wait)
        log.info("=" * 60)
        time.sleep(wait)
        remaining = _run_batch(conn, limited, delay, tag=f"R{rnd}")
        log.info("round %d done: resolved=%d  still_limited=%d",
                 rnd, len(limited) - len(remaining), len(remaining))
        _print_stats(conn, f"after retry round {rnd}")
        if not remaining:
            log.info("all quota_limited resolved")
            break
    else:
        leftover = len(_quota_limited(conn))
        if leftover:
            log.warning("%d items still quota_limited after %d rounds – resume next run",
                        leftover, len(_RETRY_WAITS))

    _print_stats(conn, "final")
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Async HTTP Metadata Probe Scheduler")
    p.add_argument("--db",    "-d", required=True, help="path to SQLite database")
    p.add_argument("--delay", "-D", type=float, default=1.5,
                   help="seconds between requests (default 1.5)")
    p.add_argument("--status", "-s", action="store_true",
                   help="print progress then exit")
    args = p.parse_args()

    db_path  = Path(args.db)
    log_file = db_path.parent / "probe_scheduler.log"
    _init_log(log_file)

    log.info("db=%s  delay=%.1fs  commit_every=%d", db_path, args.delay, COMMIT_EVERY)

    if not db_path.exists():
        log.error("db not found: %s", db_path)
        sys.exit(1)

    try:
        run(db_path, args.delay, args.status)
    except KeyboardInterrupt:
        log.warning("interrupted – progress saved, resume by restarting")
        if _conn:
            _conn.commit()
            _conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
