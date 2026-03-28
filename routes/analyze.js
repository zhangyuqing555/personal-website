'use strict';

const express = require('express');
const path = require('path');
const Database = require('better-sqlite3');
const Anthropic = require('@anthropic-ai/sdk');

const router = express.Router();
const DB_PATH = path.join(__dirname, '..', 'database', 'decisions.db');

function getDb() {
  return new Database(DB_PATH, { readonly: true });
}

/**
 * Find similar past decisions from the database to use as context.
 */
function findSimilarDecisions(requestText, limit = 5) {
  let db;
  try {
    db = getDb();
    // Extract key terms from the request (simple keyword extraction)
    const terms = requestText
      .split(/\s+/)
      .filter(w => w.length > 4)
      .slice(0, 8)
      .join(' OR ');

    if (!terms) return [];

    const rows = db.prepare(`
      SELECT spr_number, date, agency, petitioner, outcome, request_summary,
             SUBSTR(full_text, 1, 800) AS excerpt
      FROM decisions_fts
      JOIN decisions d ON decisions_fts.rowid = d.id
      WHERE decisions_fts MATCH ?
      ORDER BY d.date DESC
      LIMIT ?
    `).all(terms, limit);

    return rows;
  } catch {
    return [];
  } finally {
    if (db) db.close();
  }
}

/**
 * Format similar decisions for Claude context.
 */
function formatPrecedents(decisions) {
  if (!decisions.length) return 'No closely matching precedents found in the database.';
  return decisions.map(d => (
    `• ${d.spr_number} (${d.date || 'undated'}) — Agency: ${d.agency || 'unknown'}\n` +
    `  Outcome: ${d.outcome || 'unknown'}\n` +
    `  Request: ${d.request_summary || d.excerpt || '(not summarized)'}`
  )).join('\n\n');
}

// POST /api/analyze
router.post('/api/analyze', async (req, res) => {
  const { request_text, agency_response, agency_name } = req.body || {};

  if (!request_text || !agency_response) {
    return res.status(400).json({ error: 'Both request_text and agency_response are required.' });
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    return res.status(503).json({
      error: 'Analysis requires an ANTHROPIC_API_KEY environment variable to be set.',
    });
  }

  const similarDecisions = findSimilarDecisions(request_text);
  const precedentsText = formatPrecedents(similarDecisions);

  const systemPrompt = `You are an expert legal analyst specializing in Massachusetts Public Records Law (G.L. c. 66, § 10 et seq.) and the regulations at 950 C.M.R. 32.00. You analyze public records request appeals decided by the Massachusetts Supervisor of Records.

Your role is to:
1. Assess the likely outcome if this matter were appealed to the Supervisor of Records
2. Identify the strongest arguments for the requester
3. Flag weaknesses in the requester's position
4. Recommend concrete steps to strengthen the appeal

Base your analysis on:
- The Public Records Law and 950 C.M.R. 32.00 regulations
- The pattern of past Supervisor of Records decisions
- The specific facts presented

Be direct, practical, and legally precise. Format your response with clear sections.`;

  const userMessage = `Please analyze this public records request situation:

**Public Records Request:**
${request_text}

${agency_name ? `**Responding Agency:** ${agency_name}\n\n` : ''}**Agency's Response:**
${agency_response}

**Relevant Past Decisions from Database:**
${precedentsText}

Provide:
1. **Outcome Prediction** — How likely is a successful appeal to the Supervisor of Records? (Strong / Moderate / Weak) and why.
2. **Key Legal Issues** — What are the core legal questions at stake?
3. **Requester's Strongest Arguments** — What arguments favor the requester?
4. **Weaknesses / Risks** — What could undermine the appeal?
5. **How to Strengthen Your Position** — Specific actionable steps before or during the appeal.
6. **Suggested Next Steps** — What should the requester do now?`;

  try {
    const client = new Anthropic();
    const message = await client.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
    });

    const analysis = message.content[0].text;

    res.json({
      analysis,
      precedents: similarDecisions.map(d => ({
        spr_number: d.spr_number,
        date: d.date,
        agency: d.agency,
        outcome: d.outcome,
        request_summary: d.request_summary,
      })),
    });
  } catch (err) {
    console.error('Anthropic API error:', err.message);
    if (err.status === 401) {
      return res.status(401).json({ error: 'Invalid API key. Check ANTHROPIC_API_KEY.' });
    }
    res.status(500).json({ error: 'Analysis failed. Please try again.' });
  }
});

module.exports = router;
