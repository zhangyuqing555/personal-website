'use strict';

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// ─── Middleware ───────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Routes ───────────────────────────────────────────────────────────────────

// Personal website
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// SPR Decisions platform
app.get('/decisions', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'decisions.html'));
});

app.get('/analyze', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'analyze.html'));
});

// API routes
app.use('/', require('./routes/search'));
app.use('/', require('./routes/analyze'));
app.use('/', require('./routes/admin'));

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  console.log(`  Personal site : http://localhost:${PORT}/`);
  console.log(`  SPR Search    : http://localhost:${PORT}/decisions`);
  console.log(`  Analyzer      : http://localhost:${PORT}/analyze`);
});
