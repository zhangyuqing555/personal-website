#!/usr/bin/env python3
"""
Scraper for Massachusetts Public Records Appeals decisions.

Uses Playwright (real browser) to bypass Incapsula bot protection.
Fetches all Appeal cases from sec.state.ma.us/appealsweb/,
downloads determination letter PDFs, extracts text, and stores in SQLite.

Usage:
    python3 scripts/scrape_decisions.py                  # all years 2014-2026
    python3 scripts/scrape_decisions.py --year 2025      # single year
    python3 scripts/scrape_decisions.py --year 2025 --no-pdf   # metadata only
    python3 scripts/scrape_decisions.py --resume         # skip already-indexed cases
    python3 scripts/scrape_decisions.py --headless false # show browser window
"""

import argparse, asyncio, io, json, os, re, sqlite3, sys, time
from datetime import datetime

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "database", "decisions.db")
PDF_DIR  = os.path.join(BASE_DIR, "database", "pdfs")
os.makedirs(PDF_DIR, exist_ok=True)

SEARCH_URL = "https://www.sec.state.ma.us/appealsweb/appealsstatus.aspx"
DETAIL_URL = "https://www.sec.state.ma.us/appealsweb/AppealStatusDetail.aspx"

DELAY = 0.8   # polite delay between requests (seconds)

# ─── Database ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS decisions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            spr_number       TEXT,
            case_number      TEXT UNIQUE,
            year             INTEGER,
            date             TEXT,
            opened_date      TEXT,
            closed_date      TEXT,
            agency           TEXT,
            petitioner       TEXT,
            request_summary  TEXT,
            full_text        TEXT,
            outcome          TEXT,
            pdf_source       TEXT,
            pdf_local_path   TEXT,
            appeal_no        TEXT,
            page_start       INTEGER,
            scraped          INTEGER DEFAULT 0
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
            spr_number, agency, petitioner, request_summary, full_text,
            content=decisions, content_rowid=id, tokenize="unicode61"
        );
        CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
            INSERT INTO decisions_fts(rowid,spr_number,agency,petitioner,request_summary,full_text)
            VALUES(new.id,new.spr_number,new.agency,new.petitioner,new.request_summary,new.full_text);
        END;
        CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts,rowid,spr_number,agency,petitioner,request_summary,full_text)
            VALUES('delete',old.id,old.spr_number,old.agency,old.petitioner,old.request_summary,old.full_text);
        END;
        CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
            INSERT INTO decisions_fts(decisions_fts,rowid,spr_number,agency,petitioner,request_summary,full_text)
            VALUES('delete',old.id,old.spr_number,old.agency,old.petitioner,old.request_summary,old.full_text);
            INSERT INTO decisions_fts(rowid,spr_number,agency,petitioner,request_summary,full_text)
            VALUES(new.id,new.spr_number,new.agency,new.petitioner,new.request_summary,new.full_text);
        END;
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER, started_at TEXT, finished_at TEXT,
            total_cases INTEGER, pdfs_downloaded INTEGER, errors TEXT
        );
    """)
    conn.commit()
    return conn


# ─── Helpers ──────────────────────────────────────────────────────────────────
def normalize_date(s):
    if not s: return None
    m = re.match(r"(\d{2})-(\d{2})-(\d{4})", s.strip())
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else s.strip()

def infer_year(case_no):
    try: return int(str(case_no)[:4])
    except: return None

def make_spr(case_no):
    s = str(case_no)
    if len(s) >= 6:
        yr = s[2:4]
        seq = s[4:].lstrip("0") or "0"
        return f"SPR{yr}/{seq}"
    return None

def extract_outcome(text):
    t = (text or "").lower()
    if "is ordered to provide" in t or "must provide" in t: return "ordered_to_provide"
    if "appeal closed" in t or ("closed" in t and "administrative" in t): return "closed"
    if "fee" in t and ("waive" in t or "estimate" in t): return "fee_dispute"
    if "denied" in t or "withheld" in t: return "records_withheld"
    if "exemption" in t: return "exemption_claimed"
    return "other"

def extract_spr(text):
    m = re.search(r"(SPR\d{2,4}/\d+)", text or "", re.IGNORECASE)
    return m.group(1) if m else None

def extract_text_from_pdf(path_or_bytes):
    try:
        import pdfplumber
        src = io.BytesIO(path_or_bytes) if isinstance(path_or_bytes, bytes) else path_or_bytes
        with pdfplumber.open(src) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages).strip()
    except Exception as e:
        print(f"    PDF extract error: {e}")
        return ""

def upsert(conn, d):
    existing = conn.execute(
        "SELECT id FROM decisions WHERE case_number=?", (d.get("case_number"),)
    ).fetchone()
    if existing:
        conn.execute("""
            UPDATE decisions SET
                full_text=COALESCE(?,full_text), pdf_local_path=COALESCE(?,pdf_local_path),
                spr_number=COALESCE(?,spr_number), outcome=COALESCE(?,outcome),
                petitioner=COALESCE(?,petitioner), agency=COALESCE(?,agency),
                opened_date=COALESCE(?,opened_date), closed_date=COALESCE(?,closed_date),
                appeal_no=COALESCE(?,appeal_no), scraped=1
            WHERE id=?
        """, (d.get("full_text"), d.get("pdf_local_path"), d.get("spr_number"),
              d.get("outcome"), d.get("petitioner"), d.get("agency"),
              d.get("opened_date"), d.get("closed_date"), d.get("appeal_no"),
              existing["id"]))
        return existing["id"], False
    conn.execute("""
        INSERT INTO decisions
            (spr_number,case_number,year,date,opened_date,closed_date,
             agency,petitioner,full_text,outcome,pdf_source,pdf_local_path,appeal_no,scraped)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)
    """, (d.get("spr_number"), d.get("case_number"), d.get("year"), d.get("date"),
          d.get("opened_date"), d.get("closed_date"), d.get("agency"), d.get("petitioner"),
          d.get("full_text"), d.get("outcome"), "sec.state.ma.us",
          d.get("pdf_local_path"), d.get("appeal_no")))
    return conn.lastrowid, True


# ─── Playwright scraper ────────────────────────────────────────────────────────
async def scrape_year_playwright(year, conn, download_pdfs=True, resume=True, headless=True):
    from playwright.async_api import async_playwright

    print(f"\n{'='*60}")
    print(f"  Year {year}  |  PDFs: {'yes' if download_pdfs else 'no'}  |  Resume: {resume}")
    print(f"{'='*60}")

    pdfs_downloaded = 0
    errors = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        # ── Step 1: Load search page ──────────────────────────────────────────
        print("  Loading search page...")
        await page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(DELAY)

        # ── Step 2: Set filters ───────────────────────────────────────────────
        await page.select_option("#ddlCaseType", "1")       # Appeal
        await page.select_option("#DdlYear",     str(year))
        await page.select_option("#ddlPerPage",  "30000")   # View all
        await asyncio.sleep(0.3)

        # ── Step 3: Submit search ─────────────────────────────────────────────
        print(f"  Searching for year {year}...")
        await page.click("#BtnSearchAppeal")
        await page.wait_for_load_state("networkidle", timeout=60000)
        await asyncio.sleep(1)

        # ── Step 4: Count results ─────────────────────────────────────────────
        count_text = await page.inner_text("body")
        m = re.search(r"Records found:\s*([\d,]+)", count_text)
        total = int(m.group(1).replace(",", "")) if m else "?"
        print(f"  Found {total} records")

        # ── Step 5: Extract grid rows ─────────────────────────────────────────
        rows_data = await page.evaluate("""
        () => {
            const grid = document.getElementById('GrdWebStatusReport');
            if (!grid) return [];
            const rows = grid.querySelectorAll('tr');
            const results = [];
            for (let i = 1; i < rows.length; i++) {
                const cells = rows[i].querySelectorAll('td');
                if (cells.length < 9) continue;
                const link = cells[0].querySelector('a');
                const pdfBtn = cells[cells.length-1].querySelector('input[type="image"]');
                const href = link ? link.getAttribute('href') : '';
                const m = href ? href.match(/AppealNo=([^&]+)/) : null;
                results.push({
                    case_number: cells[0].innerText.trim(),
                    opened:      cells[1].innerText.trim(),
                    closed:      cells[3].innerText.trim(),
                    status:      cells[6].innerText.trim(),
                    requester:   cells[7].innerText.trim(),
                    custodian:   cells[8].innerText.trim(),
                    has_pdf:     !!pdfBtn,
                    appeal_no:   m ? m[1] : ''
                });
            }
            return results;
        }
        """)

        print(f"  Parsed {len(rows_data)} rows")
        processed = 0

        # ── Step 6: Process each case ─────────────────────────────────────────
        for i, row in enumerate(rows_data):
            case_no = row.get("case_number", "")
            if not case_no:
                continue

            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(rows_data)}] Processing... (PDFs: {pdfs_downloaded})")

            # Skip if already indexed with text
            if resume:
                existing = conn.execute(
                    "SELECT id, full_text FROM decisions WHERE case_number=?",
                    (case_no,)
                ).fetchone()
                if existing and existing["full_text"]:
                    continue

            yr = infer_year(case_no) or year
            case_data = {
                "case_number":  case_no,
                "year":         yr,
                "date":         normalize_date(row.get("closed") or row.get("opened")),
                "opened_date":  normalize_date(row.get("opened")),
                "closed_date":  normalize_date(row.get("closed")),
                "agency":       row.get("custodian"),
                "petitioner":   row.get("requester"),
                "appeal_no":    row.get("appeal_no"),
                "spr_number":   make_spr(case_no),
            }

            # ── Download PDF ──────────────────────────────────────────────────
            pdf_text = ""
            if download_pdfs and row.get("has_pdf") and row.get("appeal_no"):
                pdf_path = os.path.join(PDF_DIR, f"{case_no}.pdf")

                if os.path.exists(pdf_path):
                    # Already downloaded — just extract text
                    pdf_text = extract_text_from_pdf(pdf_path)
                else:
                    try:
                        detail_url = f"{DETAIL_URL}?AppealNo={row['appeal_no']}"
                        detail_page = await context.new_page()
                        await detail_page.goto(detail_url, wait_until="networkidle", timeout=20000)
                        await asyncio.sleep(DELAY)

                        # Click PDF icon and capture download
                        pdf_btn = detail_page.locator('input[type="image"][id*="btnDetermination"]')
                        btn_count = await pdf_btn.count()

                        if btn_count > 0:
                            async with detail_page.expect_download(timeout=20000) as dl_info:
                                await pdf_btn.first.click()
                            download = await dl_info.value
                            await download.save_as(pdf_path)
                            pdf_text = extract_text_from_pdf(pdf_path)
                            case_data["pdf_local_path"] = pdf_path
                            pdfs_downloaded += 1

                        await detail_page.close()
                        await asyncio.sleep(DELAY)

                    except Exception as e:
                        err = f"Case {case_no}: {e}"
                        errors.append(err)
                        if len(errors) <= 5:
                            print(f"    ERROR: {err}")
                        try:
                            await detail_page.close()
                        except:
                            pass

            # Enrich from PDF text
            if pdf_text:
                case_data["full_text"] = pdf_text
                spr = extract_spr(pdf_text)
                if spr:
                    case_data["spr_number"] = spr
                case_data["outcome"] = extract_outcome(pdf_text)

            # Save to DB
            try:
                upsert(conn, case_data)
                conn.commit()
                processed += 1
            except Exception as e:
                errors.append(f"DB {case_no}: {e}")

        await browser.close()

    print(f"\n  Year {year} done:")
    print(f"    Processed : {processed}")
    print(f"    PDFs DL   : {pdfs_downloaded}")
    print(f"    Errors    : {len(errors)}")

    conn.execute(
        "INSERT INTO scrape_log(year,started_at,finished_at,total_cases,pdfs_downloaded,errors) VALUES(?,?,?,?,?,?)",
        (year, "", datetime.now().isoformat(), processed, pdfs_downloaded, json.dumps(errors[:20]))
    )
    conn.commit()
    return processed, pdfs_downloaded, len(errors)


# ─── Entry point ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",      type=int)
    parser.add_argument("--year-from", type=int, default=2014)
    parser.add_argument("--year-to",   type=int, default=2026)
    parser.add_argument("--no-pdf",    action="store_true")
    parser.add_argument("--resume",    action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--headless",  type=lambda x: x.lower() != "false", default=True)
    args = parser.parse_args()

    years = [args.year] if args.year else list(range(args.year_from, args.year_to + 1))
    print(f"=== MA SPR Scraper ===")
    print(f"Years: {years[0]}–{years[-1]}  |  PDFs: {'no' if args.no_pdf else 'yes'}  |  Headless: {args.headless}")

    conn = get_db()
    total_cases = total_pdfs = 0

    for year in years:
        try:
            n, p, _ = asyncio.run(scrape_year_playwright(
                year, conn,
                download_pdfs=not args.no_pdf,
                resume=args.resume,
                headless=args.headless,
            ))
            total_cases += n
            total_pdfs  += p
        except KeyboardInterrupt:
            print("\nInterrupted. Progress saved.")
            break
        except Exception as e:
            print(f"FATAL year {year}: {e}")
            import traceback; traceback.print_exc()

    db_count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    print(f"\n=== DONE === Total in DB: {db_count} | PDFs DL: {total_pdfs}")
    conn.close()


if __name__ == "__main__":
    main()
