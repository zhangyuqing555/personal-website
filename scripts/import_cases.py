#!/usr/bin/env python3
"""
Import case metadata from a browser-extracted JSON file into the SQLite database.

Usage:
    python3 scripts/import_cases.py ~/Downloads/spr_cases_2024.json
    python3 scripts/import_cases.py ~/Downloads/spr_cases_*.json   (multiple years)

Workflow:
    1. On sec.state.ma.us/appealsweb/appealsstatus.aspx, set Case Type=Appeal
       and pick a Year, click Search, wait for all results to load.
    2. Open DevTools console, paste and run scripts/browser_extract.js
    3. A spr_cases_YEAR.json file downloads.
    4. Run this script to import it.
"""

import json
import os
import re
import sqlite3
import sys
from glob import glob

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "database", "decisions.db")


def norm_date(s):
    if not s: return None
    m = re.match(r"(\d{2})-(\d{2})-(\d{4})", s.strip())
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else s.strip()


def make_spr(cn):
    s = str(cn)
    if len(s) >= 6:
        yr  = s[2:4]
        seq = s[4:].lstrip("0") or "0"
        return f"SPR{yr}/{seq}"
    return None


def migrate_schema(conn):
    """Add new columns if they don't exist (idempotent)."""
    new_cols = [
        ("case_number",    "TEXT"),
        ("opened_date",    "TEXT"),
        ("closed_date",    "TEXT"),
        ("appeal_no",      "TEXT"),
        ("scraped",        "INTEGER DEFAULT 0"),
        ("pdf_local_path", "TEXT"),
    ]
    for col, dtype in new_cols:
        try:
            conn.execute(f"ALTER TABLE decisions ADD COLUMN {col} {dtype}")
        except Exception:
            pass  # already exists
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_case_number ON decisions(case_number)")
    except Exception:
        pass
    conn.commit()


UPSERT_SQL = """
INSERT INTO decisions
    (spr_number, case_number, year, date, opened_date, closed_date,
     agency, petitioner, appeal_no, pdf_source, scraped)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'sec.state.ma.us', 1)
ON CONFLICT(case_number) DO UPDATE SET
    appeal_no   = COALESCE(excluded.appeal_no,   appeal_no),
    agency      = COALESCE(excluded.agency,       agency),
    petitioner  = COALESCE(excluded.petitioner,   petitioner),
    opened_date = COALESCE(excluded.opened_date,  opened_date),
    closed_date = COALESCE(excluded.closed_date,  closed_date),
    scraped     = 1
"""


def import_file(path, conn):
    with open(path) as f:
        cases = json.load(f)

    print(f"\n  Importing {len(cases)} cases from {os.path.basename(path)}...")

    before = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    conn.execute("BEGIN")
    for c in cases:
        cn = c.get("cn", "")
        if not cn:
            continue
        yr = int(str(cn)[:4]) if len(str(cn)) >= 4 else None
        conn.execute(UPSERT_SQL, (
            make_spr(cn), cn, yr,
            norm_date(c.get("cl") or c.get("op")),
            norm_date(c.get("op")),
            norm_date(c.get("cl")),
            c.get("cu"),
            c.get("rq"),
            c.get("an"),
        ))
    conn.execute("COMMIT")
    after = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    print(f"  New rows added: {after - before}  |  Total: {after}")
    return after - before


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/import_cases.py <path/to/spr_cases_YEAR.json> [...]")
        sys.exit(1)

    # Support glob patterns
    paths = []
    for arg in sys.argv[1:]:
        expanded = glob(os.path.expanduser(arg))
        if expanded:
            paths.extend(sorted(expanded))
        else:
            paths.append(os.path.expanduser(arg))

    if not paths:
        print("No files found.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    migrate_schema(conn)

    total_new = 0
    for path in paths:
        if not os.path.exists(path):
            print(f"  WARNING: File not found: {path}")
            continue
        total_new += import_file(path, conn)

    # Final stats
    total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    by_year = conn.execute(
        "SELECT year, COUNT(*) n FROM decisions WHERE year IS NOT NULL GROUP BY year ORDER BY year"
    ).fetchall()

    print(f"\n{'='*40}")
    print(f"Total in database: {total}")
    print("By year:")
    for yr, n in by_year:
        print(f"  {yr}: {n:>4} decisions")
    conn.close()


if __name__ == "__main__":
    main()
