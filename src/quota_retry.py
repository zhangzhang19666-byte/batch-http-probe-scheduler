#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FOR TESTING PURPOSES ONLY.

Quota Retry Processor
- Fetches all quota_limited records from DB
- Re-probes each one and writes result back immediately
- Stops gracefully before GitHub Actions job timeout
- Auto-saves on SIGTERM / KeyboardInterrupt
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

# ── config ───────────────────────────────────────────────────────────────
_ENDPOINT   = os.environ.get("PROBE_ENDPOINT", "")
STATS_EVERY = 50

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

# ── db ────────────────────────────────────────────────────────────────────

_conn: sqlite3.Connection | None = None


def _open_db(path: Path) -> sqlite3.Connection:
    global _conn
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.commit()
    _conn = conn
    return conn


def _fetch_quota_limited(conn) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT magnet_url FROM magnet_results WHERE status='quota_limited'"
    ).fetchall()]


def _save(conn, key: str, status: str, payload: dict):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn.execute(
        "UPDATE magnet_results SET status=?, raw_json=?, checked_at=? WHERE magnet_url=?",
        (status, json.dumps(payload, ensure_ascii=False), ts, key),
    )
    conn.commit()
    log.debug("committed 1 row to disk  [%s]", status)


def _counts(conn) -> dict:
    return {r[0]: r[1] for r in conn.execute(
        "SELECT status, COUNT(*) FROM magnet_results GROUP BY status"
    ).fetchall()}


def _print_stats(conn, label="stats"):
    c = _counts(conn)
    log.info(
        "-- %s -- total=%d  ok=%d  fail=%d  quota=%d  pending=%d",
        label, sum(c.values()),
        c.get("success", 0), c.get("failed", 0),
        c.get("quota_limited", 0), c.get("pending", 0),
    )

# ── probe ─────────────────────────────────────────────────────────────────

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

_deadline: float = float("inf")


def _over_budget() -> bool:
    return time.monotonic() >= _deadline


def _time_left() -> float:
    return _deadline - time.monotonic()

# ── signal handler ────────────────────────────────────────────────────────

def _handle_sigterm(signum, frame):
    log.warning("SIGTERM received – flushing DB and exiting")
    if _conn:
        try:
            _conn.commit()
            _conn.close()
        except Exception:
            pass
    sys.exit(0)

# ── main logic ────────────────────────────────────────────────────────────

def run(db_path: Path, delay: float, max_minutes: float):
    global _deadline

    if not _ENDPOINT:
        log.error("PROBE_ENDPOINT env var not set")
        sys.exit(1)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    _deadline = time.monotonic() + max_minutes * 60
    log.info("time budget: %.0f min", max_minutes)

    conn = _open_db(db_path)
    _print_stats(conn, "startup")

    items = _fetch_quota_limited(conn)
    total = len(items)

    if total == 0:
        log.info("no quota_limited records found – nothing to do")
        conn.close()
        return

    log.info("=" * 60)
    log.info("retrying %d quota_limited records  (delay=%.1fs)", total, delay)
    log.info("=" * 60)

    resolved = 0
    still_limited = 0

    for i, item in enumerate(items, 1):
        if _over_budget():
            log.warning("time budget exhausted at [%d/%d] – progress saved", i, total)
            break

        lbl = f"[{i:>6}/{total}]"
        status, data = _probe(item)
        _save(conn, item, status, data)

        if status == "success":
            log.info("%s  ok", lbl)
            resolved += 1
        elif status == "quota_limited":
            log.warning("%s  still quota_limited", lbl)
            still_limited += 1
        else:
            log.info("%s  failed  err=%s", lbl, data.get("error", "-"))
            resolved += 1   # failed is still "resolved" from quota perspective

        if i % STATS_EVERY == 0:
            log.info("time left: %.1f min", _time_left() / 60)
            _print_stats(conn, f"subtotal {i}/{total}")

        time.sleep(delay)

    log.info("done: resolved=%d  still_limited=%d", resolved, still_limited)
    _print_stats(conn, "final")
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Quota Retry Processor")
    p.add_argument("--db",          "-d", required=True, help="path to SQLite database")
    p.add_argument("--delay",       "-D", type=float, default=2.4,
                   help="seconds between requests (default 2.4)")
    p.add_argument("--max-minutes", "-m", type=float, default=300,
                   help="stop before job timeout (default 300)")
    args = p.parse_args()

    db_path  = Path(args.db)
    log_file = db_path.parent / "quota_retry.log"
    _init_log(log_file)

    log.info("db=%s  delay=%.1fs  max_minutes=%.0f", db_path, args.delay, args.max_minutes)

    if not db_path.exists():
        log.error("db not found: %s", db_path)
        sys.exit(1)

    try:
        run(db_path, args.delay, args.max_minutes)
    except KeyboardInterrupt:
        log.warning("interrupted – progress saved")
        if _conn:
            try:
                _conn.commit()
                _conn.close()
            except Exception:
                pass
        sys.exit(0)


if __name__ == "__main__":
    main()
