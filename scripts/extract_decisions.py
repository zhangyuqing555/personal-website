#!/usr/bin/env python3
"""
Extract Massachusetts SPR decisions from PDFs and store in SQLite with FTS5.
Usage: python3 scripts/extract_decisions.py
"""

import pdfplumber
import sqlite3
import re
import os
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "database", "decisions.db")
PDF_DIR = os.path.join(BASE_DIR, "Public Records Appeals")

PDF_FILES = [
    "Combined SPR Decisions.pdf",
    "Combined SPR Decisions File 2.pdf",
]

# Pattern: start of a new decision letter
DECISION_START = re.compile(
    r"The Commonwealth of Massachusetts",
    re.IGNORECASE,
)

SPR_PATTERN = re.compile(r"SPR(\d{2,4})/(\d+)", re.IGNORECASE)

DATE_PATTERN = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s*(20\d{2}|19\d{2})",
    re.IGNORECASE,
)

# Continuation page header: "Recipient SPR##/#### \nPage N\nDate"
CONTINUATION_PATTERN = re.compile(
    r"^.{0,60}SPR\d{2,4}/\d+\s*\nPage \d+",
    re.MULTILINE | re.IGNORECASE,
)


def normalize_text(text):
    """Clean up extracted PDF text."""
    if not text:
        return ""
    # Normalize whitespace and line endings
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\r", "\n", text)
    # Remove repeated header/footer lines (phone numbers, addresses)
    text = re.sub(r"One Ashburton Place.*?\n", "", text)
    text = re.sub(r"\(617\) 727.*?\n", "", text)
    text = re.sub(r"sec\.state\.ma\.us.*?\n", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_spr_number(text):
    m = SPR_PATTERN.search(text)
    if m:
        return f"SPR{m.group(1)}/{m.group(2)}", int("20" + m.group(1)) if len(m.group(1)) == 2 else int(m.group(1))
    return None, None


def extract_date(text):
    """Extract the primary date (first date near the top of a decision)."""
    m = DATE_PATTERN.search(text[:500])
    if m:
        try:
            raw = f"{m.group(1)} {m.group(2)}, {m.group(3)}"
            dt = datetime.strptime(raw, "%B %d, %Y")
            return dt.strftime("%Y-%m-%d"), dt.year
        except ValueError:
            pass
    return None, None


def extract_agency(text):
    """Extract the agency name from the decision body."""
    # Pattern: "appealing the [non]response of [Agency Name] (Short)"
    m = re.search(
        r"appealing the (?:non)?response of ([^(.\n]+?)(?:\s*\([^)]+\))?\s+to",
        text,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    # Fallback: look for "Dear [Title] [Name]:" and use the agency on prior lines
    m2 = re.search(r"Dear [^:]+:\n\nI have received.*?of ([^(.\n]+?)(?:\s*\()", text, re.DOTALL)
    if m2:
        return m2.group(1).strip()
    return None


def extract_petitioner(text):
    """Extract petitioner name."""
    m = re.search(
        r"petition of ([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-']+(?:\s+[A-Z][a-zA-Z\-']+)?)"
        r"(?:\s+appealing|\s+and\s)",
        text,
    )
    if m:
        return m.group(1).strip()
    # Try simpler pattern
    m2 = re.search(r"petition of ([A-Z][^\n]+?) appealing", text)
    if m2:
        name = m2.group(1).strip()
        # Cap at 60 chars to avoid runaway matches
        if len(name) < 60:
            return name
    return None


def extract_recipient_agency_block(text):
    """Extract recipient name + agency from the top address block."""
    # Lines after the date / SPR number, before 'Dear'
    lines = text.split("\n")
    # Find 'Dear' line position
    dear_idx = next((i for i, l in enumerate(lines) if l.strip().startswith("Dear ")), None)
    if dear_idx and dear_idx > 2:
        # The agency is typically 2-3 lines before Dear
        block = lines[max(0, dear_idx - 6):dear_idx]
        # Agency name is usually the first non-empty line in that block
        for line in block:
            line = line.strip()
            if line and not re.match(r"^\d", line) and "Street" not in line and "MA" not in line:
                return line
    return None


def extract_outcome(text):
    """Classify outcome: ordered, closed, fee, denied, or other."""
    lower = text.lower()
    if "is ordered to provide" in lower or "must provide" in lower:
        return "ordered_to_provide"
    if "appeal closed" in lower or "closed" in lower and "administrative" in lower:
        return "closed"
    if "fee" in lower and ("waive" in lower or "estimate" in lower):
        return "fee_dispute"
    if "denied" in lower or "withheld" in lower:
        return "records_withheld"
    if "exemption" in lower:
        return "exemption_claimed"
    return "other"


def extract_request_text(text):
    """Extract the description of the public records request."""
    # Usually quoted or follows "requested"
    m = re.search(
        r'requested[,:]?\s*(?:the following[:\s]*|["\u201c])(.{30,600}?)(?:["\u201d]|\.\s+(?:On|In|The|I |Mr\.|Ms\.|Upon))',
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    # Fallback: look for bracketed request text
    m2 = re.search(r"requested[^.]{0,100}[:\n]\s*(.{30,400}?)\.\s*\n", text, re.DOTALL)
    if m2:
        return re.sub(r"\s+", " ", m2.group(1)).strip()
    return None


def is_continuation_page(text):
    """Return True if this page is a continuation of a prior decision."""
    first_lines = "\n".join(text.strip().split("\n")[:4])
    # A continuation starts with something like "RecipientName SPR##/####\nPage N"
    if re.match(r"^[^\n]{5,80}\s+SPR\d+/\d+\s*\nPage \d+", first_lines):
        return True
    # Or just "Page N" as first non-empty content after a name
    if re.match(r"^[^\n]{5,80}\s*\nPage \d+\s*\n", first_lines):
        if SPR_PATTERN.search(first_lines):
            return True
    return False


def split_pages_into_decisions(pages_text):
    """
    Group consecutive pages into individual decisions.
    A new decision starts when a page begins with the Commonwealth header.
    """
    decisions = []
    current_pages = []

    for page_num, text in enumerate(pages_text):
        if not text:
            continue
        first_500 = text[:500]
        is_new_decision = bool(DECISION_START.search(first_500))

        if is_new_decision:
            if current_pages:
                decisions.append(current_pages)
            current_pages = [(page_num, text)]
        else:
            # Continuation page — attach to current decision
            current_pages.append((page_num, text))

    if current_pages:
        decisions.append(current_pages)

    return decisions


def parse_decision(pages):
    """Parse a list of (page_num, text) tuples into a decision dict."""
    full_text = "\n\n".join(normalize_text(t) for _, t in pages)
    page_start = pages[0][0] + 1  # 1-based

    spr_number, spr_year = extract_spr_number(full_text)
    date_str, date_year = extract_date(full_text)

    year = date_year or spr_year

    agency = extract_agency(full_text) or extract_recipient_agency_block(full_text)
    petitioner = extract_petitioner(full_text)
    request_text = extract_request_text(full_text)
    outcome = extract_outcome(full_text)

    return {
        "spr_number": spr_number,
        "year": year,
        "date": date_str,
        "agency": agency,
        "petitioner": petitioner,
        "request_summary": request_text,
        "full_text": full_text,
        "outcome": outcome,
        "page_start": page_start,
    }


def setup_database(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS decisions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            spr_number  TEXT,
            year        INTEGER,
            date        TEXT,
            agency      TEXT,
            petitioner  TEXT,
            request_summary TEXT,
            full_text   TEXT,
            outcome     TEXT,
            pdf_source  TEXT,
            page_start  INTEGER
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
            spr_number,
            agency,
            petitioner,
            request_summary,
            full_text,
            content=decisions,
            content_rowid=id,
            tokenize="unicode61"
        );

        CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
            INSERT INTO decisions_fts(rowid, spr_number, agency, petitioner, request_summary, full_text)
            VALUES (new.id, new.spr_number, new.agency, new.petitioner, new.request_summary, new.full_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts, rowid, spr_number, agency, petitioner, request_summary, full_text)
            VALUES ('delete', old.id, old.spr_number, old.agency, old.petitioner, old.request_summary, old.full_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts, rowid, spr_number, agency, petitioner, request_summary, full_text)
            VALUES ('delete', old.id, old.spr_number, old.agency, old.petitioner, old.request_summary, old.full_text);
            INSERT INTO decisions_fts(rowid, spr_number, agency, petitioner, request_summary, full_text)
            VALUES (new.id, new.spr_number, new.agency, new.petitioner, new.request_summary, new.full_text);
        END;
    """)
    conn.commit()


def insert_decision(conn, decision, pdf_source):
    conn.execute(
        """INSERT INTO decisions
           (spr_number, year, date, agency, petitioner, request_summary,
            full_text, outcome, pdf_source, page_start)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            decision["spr_number"],
            decision["year"],
            decision["date"],
            decision["agency"],
            decision["petitioner"],
            decision["request_summary"],
            decision["full_text"],
            decision["outcome"],
            pdf_source,
            decision["page_start"],
        ),
    )


def process_pdf(pdf_path, conn, pdf_name):
    print(f"\nProcessing: {pdf_name}")
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  Pages: {total}")
        pages_text = []
        for i, page in enumerate(pdf.pages):
            if i % 100 == 0:
                print(f"  Extracting page {i+1}/{total}...")
            pages_text.append(page.extract_text())

    print("  Splitting into decisions...")
    decision_groups = split_pages_into_decisions(pages_text)
    print(f"  Found {len(decision_groups)} decisions")

    inserted = 0
    skipped = 0
    for group in decision_groups:
        d = parse_decision(group)
        if not d["spr_number"]:
            skipped += 1
            continue
        # Skip duplicates
        existing = conn.execute(
            "SELECT id FROM decisions WHERE spr_number = ?", (d["spr_number"],)
        ).fetchone()
        if existing:
            skipped += 1
            continue
        insert_decision(conn, d, pdf_name)
        inserted += 1

    conn.commit()
    print(f"  Inserted: {inserted} | Skipped/duplicates: {skipped}")
    return inserted


def main():
    print("=== SPR Decisions Extraction ===")
    print(f"Database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    setup_database(conn)

    total_inserted = 0
    for pdf_name in PDF_FILES:
        pdf_path = os.path.join(PDF_DIR, pdf_name)
        if not os.path.exists(pdf_path):
            print(f"  WARNING: {pdf_name} not found, skipping.")
            continue
        inserted = process_pdf(pdf_path, conn, pdf_name)
        total_inserted += inserted

    # Print summary stats
    count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    years = conn.execute(
        "SELECT year, COUNT(*) FROM decisions WHERE year IS NOT NULL GROUP BY year ORDER BY year"
    ).fetchall()

    print(f"\n=== COMPLETE ===")
    print(f"Total decisions in database: {count}")
    print("By year:")
    for year, cnt in years:
        print(f"  {year}: {cnt} decisions")

    conn.close()


if __name__ == "__main__":
    main()
