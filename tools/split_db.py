#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Split results_all.db pending rows into N smaller DBs for multi-account parallel processing.

Usage:
  python tools/split_db.py --src E:/r2/.../results_all.db --parts 10
  python tools/split_db.py --src E:/r2/.../results_all.db --parts 10 --out-dir E:/splits

Output:
  splits/part_00.db  (~160k rows each)
  splits/part_01.db
  ...
  splits/part_09.db

Each output DB:
  - Same schema as source
  - Only contains its slice of pending rows (status reset to 'pending')
  - Already-processed rows (success/failed) are NOT included
"""

import argparse
import sqlite3
from pathlib import Path


def split(src: Path, n_parts: int, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    src_conn = sqlite3.connect(src)
    rows = src_conn.execute(
        "SELECT magnet_url, created_at FROM magnet_results WHERE status='pending'"
    ).fetchall()
    src_conn.close()

    total = len(rows)
    chunk = (total + n_parts - 1) // n_parts  # ceiling div → last part may be smaller

    print(f"pending rows : {total:,}")
    print(f"parts        : {n_parts}")
    print(f"rows/part    : ~{chunk:,}")
    print()

    for i in range(n_parts):
        part_rows = rows[i * chunk: (i + 1) * chunk]
        if not part_rows:
            print(f"  part_{i:02d}  skipped (no rows)")
            continue

        out_path = out_dir / f"part_{i:02d}.db"
        out_path.unlink(missing_ok=True)

        conn = sqlite3.connect(out_path)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript("""
            CREATE TABLE magnet_results (
                magnet_url  TEXT PRIMARY KEY,
                status      TEXT DEFAULT 'pending',
                raw_json    TEXT,
                checked_at  TEXT,
                created_at  TEXT
            );
            CREATE INDEX idx_status ON magnet_results(status);
        """)
        conn.executemany(
            "INSERT INTO magnet_results (magnet_url, status, created_at) "
            "VALUES (?, 'pending', ?)",
            part_rows,
        )
        conn.commit()
        conn.close()

        size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"  part_{i:02d}.db  {len(part_rows):>7,} rows  {size_mb:5.1f} MB  → {out_path}")

    print(f"\nDone. {n_parts} files in {out_dir}")


def main():
    p = argparse.ArgumentParser(description="Split DB into N parts for parallel workers")
    p.add_argument("--src",      required=True, help="source results_all.db path")
    p.add_argument("--parts",    type=int, default=10, help="number of output parts (default 10)")
    p.add_argument("--out-dir",  default="splits", help="output directory (default ./splits)")
    args = p.parse_args()

    src = Path(args.src)
    if not src.exists():
        print(f"ERROR: {src} not found")
        raise SystemExit(1)

    split(src, args.parts, Path(args.out_dir))


if __name__ == "__main__":
    main()
