'use strict';

const express = require('express');
const path    = require('path');
const { spawn } = require('child_process');
const Database = require('better-sqlite3');

const router  = express.Router();
const DB_PATH = path.join(__dirname, '..', 'database', 'decisions.db');

// Allow browser scripts from any origin (e.g. sec.state.ma.us) to POST here
// Includes Private Network Access header for Chrome 98+ localhost requests
router.use('/api/admin/ingest-cases', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Private-Network', 'true');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function getDb() {
  return new Database(DB_PATH, { readonly: true });
}

// In-memory scraper job state
let scraperJob = null;   // { proc, year, startedAt, logs, done, error }

// GET /admin  — admin dashboard page
router.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'views', 'admin.html'));
});

// GET /api/admin/status  — database stats + scraper state
router.get('/api/admin/status', (req, res) => {
  let stats = { total: 0, byYear: [], scraped: 0, withPdf: 0, lastScrape: null };
  try {
    const db = getDb();
    stats.total   = db.prepare('SELECT COUNT(*) AS n FROM decisions').get().n;
    stats.scraped  = db.prepare('SELECT COUNT(*) AS n FROM decisions WHERE scraped=1').get().n;
    stats.withPdf  = db.prepare('SELECT COUNT(*) AS n FROM decisions WHERE pdf_local_path IS NOT NULL').get().n;
    stats.byYear   = db.prepare('SELECT year, COUNT(*) AS n FROM decisions WHERE year IS NOT NULL GROUP BY year ORDER BY year').all();
    stats.lastScrape = db.prepare('SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1').get() || null;
    db.close();
  } catch {}

  const jobStatus = scraperJob ? {
    running:   !scraperJob.done,
    year:      scraperJob.year,
    startedAt: scraperJob.startedAt,
    done:      scraperJob.done,
    error:     scraperJob.error,
    logTail:   scraperJob.logs.slice(-30),
  } : null;

  res.json({ stats, job: jobStatus });
});

// POST /api/admin/scrape  — start a scrape job
router.post('/api/admin/scrape', (req, res) => {
  if (scraperJob && !scraperJob.done) {
    return res.status(409).json({ error: 'A scrape job is already running.' });
  }

  const { year, noPdf, resume = true } = req.body || {};
  const scriptPath = path.join(__dirname, '..', 'scripts', 'scrape_decisions.py');

  const args = ['python3', scriptPath];
  if (year) { args.push('--year', String(year)); }
  if (noPdf) { args.push('--no-pdf'); }
  if (!resume) { args.push('--no-resume'); }

  const proc = spawn(args[0], args.slice(1), {
    cwd: path.join(__dirname, '..'),
    env: { ...process.env },
  });

  scraperJob = {
    proc,
    year: year || 'all',
    startedAt: new Date().toISOString(),
    logs: [],
    done: false,
    error: null,
  };

  const appendLog = (data) => {
    const lines = data.toString().split('\n').filter(Boolean);
    scraperJob.logs.push(...lines);
    if (scraperJob.logs.length > 500) scraperJob.logs = scraperJob.logs.slice(-500);
  };

  proc.stdout.on('data', appendLog);
  proc.stderr.on('data', appendLog);

  proc.on('close', (code) => {
    scraperJob.done = true;
    if (code !== 0) scraperJob.error = `Exited with code ${code}`;
  });

  proc.on('error', (err) => {
    scraperJob.done = true;
    scraperJob.error = err.message;
  });

  res.json({ started: true, year: year || 'all' });
});

// POST /api/admin/scrape/stop
router.post('/api/admin/scrape/stop', (req, res) => {
  if (!scraperJob || scraperJob.done) {
    return res.json({ stopped: false, message: 'No job running.' });
  }
  scraperJob.proc.kill('SIGTERM');
  scraperJob.done = true;
  scraperJob.error = 'Stopped by user';
  res.json({ stopped: true });
});

// GET /api/admin/logs  — streaming log tail
router.get('/api/admin/logs', (req, res) => {
  const logs = scraperJob ? scraperJob.logs.slice(-100) : [];
  res.json({ logs, running: !!(scraperJob && !scraperJob.done) });
});

// POST /api/admin/ingest-cases  — receive case metadata from browser script
// Accepts { cases: [{cn, op, cl, st, rq, cu, pdf, an}, ...] }
router.post('/api/admin/ingest-cases', (req, res) => {
  const { cases = [] } = req.body || {};
  if (!cases.length) return res.json({ inserted: 0, updated: 0 });

  let db;
  try {
    db = new Database(DB_PATH);
  } catch {
    return res.status(503).json({ error: 'Database not ready.' });
  }

  const normalize = (s) => {
    if (!s) return null;
    const m = s.match(/^(\d{2})-(\d{2})-(\d{4})$/);
    return m ? `${m[3]}-${m[1]}-${m[2]}` : s;
  };

  const makeSpr = (cn) => {
    const s = String(cn);
    if (s.length >= 6) {
      const yr  = s.slice(2, 4);
      const seq = s.slice(4).replace(/^0+/, '') || '0';
      return `SPR${yr}/${seq}`;
    }
    return null;
  };

  let inserted = 0, updated = 0;

  const upsertStmt = db.prepare(`
    INSERT INTO decisions (spr_number, case_number, year, date, opened_date, closed_date,
      agency, petitioner, appeal_no, pdf_source, scraped)
    VALUES (@spr, @cn, @yr, @date, @op, @cl, @cu, @rq, @an, 'sec.state.ma.us', 1)
    ON CONFLICT(case_number) DO UPDATE SET
      appeal_no   = COALESCE(excluded.appeal_no, appeal_no),
      agency      = COALESCE(excluded.agency, agency),
      petitioner  = COALESCE(excluded.petitioner, petitioner),
      opened_date = COALESCE(excluded.opened_date, opened_date),
      closed_date = COALESCE(excluded.closed_date, closed_date),
      scraped     = 1
  `);

  const runBatch = db.transaction((rows) => {
    for (const c of rows) {
      const yrNum = c.cn ? parseInt(String(c.cn).slice(0, 4), 10) : null;
      const dateVal = normalize(c.cl) || normalize(c.op);
      const result = upsertStmt.run({
        spr:  makeSpr(c.cn),
        cn:   c.cn,
        yr:   yrNum,
        date: dateVal,
        op:   normalize(c.op),
        cl:   normalize(c.cl),
        cu:   c.cu || null,
        rq:   c.rq || null,
        an:   c.an || null,
      });
      if (result.changes > 0) {
        if (result.lastInsertRowid > (db.prepare('SELECT MAX(id) AS m FROM decisions').get().m - cases.length)) {
          inserted++;
        } else {
          updated++;
        }
      }
    }
  });

  try {
    runBatch(cases);
    const total = db.prepare('SELECT COUNT(*) AS n FROM decisions').get().n;
    res.json({ inserted, updated, total });
  } catch (err) {
    console.error('Ingest error:', err);
    res.status(500).json({ error: err.message });
  } finally {
    db.close();
  }
});

module.exports = router;
