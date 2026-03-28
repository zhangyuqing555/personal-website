'use strict';

const express = require('express');
const path = require('path');
const Database = require('better-sqlite3');

const router = express.Router();
const DB_PATH = path.join(__dirname, '..', 'database', 'decisions.db');

function getDb() {
  return new Database(DB_PATH, { readonly: true });
}

/**
 * Convert a user-facing Boolean query into an FTS5 query string.
 *
 * Supported syntax (case-insensitive):
 *   AND  →  AND  (FTS5 default when implicit)
 *   OR   →  OR
 *   NOT  →  NOT
 *   "phrase search" → "phrase search"
 *   /N   →  proximity: terms within N words  (post-filter)
 *   /S   →  proximity: same sentence         (post-filter)
 *   /P   →  proximity: same paragraph        (post-filter)
 *
 * Returns { ftsQuery, proximityOps } where proximityOps is an array of
 * { terms: [a, b], type: 'word'|'sentence'|'paragraph', n?: number }
 */
function parseQuery(raw) {
  const proximityOps = [];

  // Extract proximity expressions: term1 /N term2, term1 /S term2, term1 /P term2
  let query = raw;
  const proxPattern = /(\S+)\s+\/(\d+|[SP])\s+(\S+)/gi;
  query = query.replace(proxPattern, (match, t1, spec, t2) => {
    const type = isNaN(spec) ? (spec.toUpperCase() === 'S' ? 'sentence' : 'paragraph') : 'word';
    const n = isNaN(spec) ? null : parseInt(spec, 10);
    proximityOps.push({ terms: [t1, t2], type, n });
    // Replace with AND for FTS5 so both terms must exist
    return `${t1} AND ${t2}`;
  });

  // Normalize boolean operators to uppercase for FTS5
  query = query.replace(/\bAND\b/gi, 'AND');
  query = query.replace(/\bOR\b/gi, 'OR');
  query = query.replace(/\bNOT\b/gi, 'NOT');

  // Escape unquoted special chars that would break FTS5 (except operators and quotes)
  // FTS5 uses: AND OR NOT ( ) " * ^
  // We allow those through; sanitize anything else weird
  query = query.replace(/[{}[\]|\\]/g, ' ').trim();

  return { ftsQuery: query, proximityOps };
}

/**
 * Post-filter results for proximity constraints.
 */
function checkProximity(text, terms, type, n) {
  if (!text) return false;
  const lower = text.toLowerCase();
  const t0 = terms[0].toLowerCase().replace(/"/g, '');
  const t1 = terms[1].toLowerCase().replace(/"/g, '');

  if (type === 'word') {
    const words = lower.split(/\s+/);
    const idx0 = words.findIndex(w => w.includes(t0));
    const idx1 = words.findIndex(w => w.includes(t1));
    if (idx0 === -1 || idx1 === -1) return false;
    return Math.abs(idx0 - idx1) <= n;
  }

  if (type === 'sentence') {
    const sentences = lower.split(/[.!?]+/);
    return sentences.some(s => s.includes(t0) && s.includes(t1));
  }

  if (type === 'paragraph') {
    const paragraphs = lower.split(/\n{2,}/);
    return paragraphs.some(p => p.includes(t0) && p.includes(t1));
  }

  return true;
}

/**
 * Generate a text snippet with the query terms highlighted.
 */
function makeSnippet(text, rawQuery, maxLen = 300) {
  if (!text) return '';
  // Extract plain terms (strip operators and quotes)
  const terms = rawQuery
    .replace(/\b(AND|OR|NOT)\b/gi, ' ')
    .replace(/["()]/g, ' ')
    .split(/\s+/)
    .filter(t => t.length > 2);

  // Find the first occurrence of any term
  const lower = text.toLowerCase();
  let bestPos = text.length;
  for (const term of terms) {
    const pos = lower.indexOf(term.toLowerCase());
    if (pos !== -1 && pos < bestPos) bestPos = pos;
  }

  const start = Math.max(0, bestPos - 80);
  let snippet = text.slice(start, start + maxLen);
  if (start > 0) snippet = '…' + snippet;
  if (start + maxLen < text.length) snippet += '…';

  // Bold each matching term in the snippet
  for (const term of terms) {
    const re = new RegExp(`(${term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    snippet = snippet.replace(re, '**$1**');
  }

  return snippet;
}

// GET /api/search
router.get('/api/search', (req, res) => {
  const {
    q = '',
    year_from,
    year_to,
    agency,
    outcome,
    page = 1,
    per_page = 20,
  } = req.query;

  if (!q.trim()) {
    return res.json({ results: [], total: 0, page: 1, pages: 0 });
  }

  let db;
  try {
    db = getDb();
  } catch {
    return res.status(503).json({ error: 'Database not ready. Run the extraction script first.' });
  }

  try {
    const { ftsQuery, proximityOps } = parseQuery(q.trim());
    const pageNum = Math.max(1, parseInt(page, 10));
    const limit = Math.min(50, Math.max(1, parseInt(per_page, 10)));
    const offset = (pageNum - 1) * limit;

    // Build the SQL query
    let sql = `
      SELECT d.id, d.spr_number, d.year, d.date, d.agency,
             d.petitioner, d.request_summary, d.outcome, d.pdf_source,
             d.full_text
      FROM decisions_fts
      JOIN decisions d ON decisions_fts.rowid = d.id
      WHERE decisions_fts MATCH ?
    `;
    const params = [ftsQuery];

    if (year_from) { sql += ' AND d.year >= ?'; params.push(parseInt(year_from, 10)); }
    if (year_to)   { sql += ' AND d.year <= ?'; params.push(parseInt(year_to, 10)); }
    if (agency)    { sql += ' AND LOWER(d.agency) LIKE ?'; params.push(`%${agency.toLowerCase()}%`); }
    if (outcome)   { sql += ' AND d.outcome = ?'; params.push(outcome); }

    sql += ' ORDER BY d.date DESC';

    // Fetch all matching for proximity post-filter (with cap)
    const allRows = db.prepare(sql).all(...params);

    // Apply proximity post-filter
    const filtered = proximityOps.length === 0
      ? allRows
      : allRows.filter(row =>
          proximityOps.every(op =>
            checkProximity(row.full_text, op.terms, op.type, op.n)
          )
        );

    const total = filtered.length;
    const pages = Math.ceil(total / limit);
    const pageRows = filtered.slice(offset, offset + limit);

    const results = pageRows.map(row => ({
      id: row.id,
      spr_number: row.spr_number,
      year: row.year,
      date: row.date,
      agency: row.agency,
      petitioner: row.petitioner,
      outcome: row.outcome,
      snippet: makeSnippet(row.full_text, q),
      request_summary: row.request_summary,
    }));

    res.json({ results, total, page: pageNum, pages, query: q });
  } catch (err) {
    // FTS5 syntax errors return a user-friendly message
    if (err.message && err.message.includes('fts5')) {
      return res.status(400).json({ error: 'Invalid search syntax. Check your Boolean operators.' });
    }
    console.error('Search error:', err);
    res.status(500).json({ error: 'Search failed.' });
  } finally {
    if (db) db.close();
  }
});

// GET /api/decisions/:id  — full decision text
router.get('/api/decisions/:id', (req, res) => {
  let db;
  try {
    db = getDb();
    const row = db.prepare('SELECT * FROM decisions WHERE id = ?').get(req.params.id);
    if (!row) return res.status(404).json({ error: 'Not found' });
    res.json(row);
  } catch {
    res.status(503).json({ error: 'Database not ready.' });
  } finally {
    if (db) db.close();
  }
});

// GET /api/stats  — summary counts for the UI
router.get('/api/stats', (req, res) => {
  let db;
  try {
    db = getDb();
    const total = db.prepare('SELECT COUNT(*) AS n FROM decisions').get().n;
    const byYear = db.prepare(
      'SELECT year, COUNT(*) AS n FROM decisions WHERE year IS NOT NULL GROUP BY year ORDER BY year'
    ).all();
    const byOutcome = db.prepare(
      'SELECT outcome, COUNT(*) AS n FROM decisions GROUP BY outcome ORDER BY n DESC'
    ).all();
    const agencies = db.prepare(
      'SELECT agency, COUNT(*) AS n FROM decisions WHERE agency IS NOT NULL GROUP BY agency ORDER BY n DESC LIMIT 20'
    ).all();
    res.json({ total, byYear, byOutcome, agencies });
  } catch {
    res.status(503).json({ error: 'Database not ready.' });
  } finally {
    if (db) db.close();
  }
});

module.exports = router;
