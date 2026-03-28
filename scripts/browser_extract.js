/**
 * SPR Cases Browser Extractor
 * ─────────────────────────────────────────────────────────────
 * Run this in your Chrome DevTools console while on:
 *   https://www.sec.state.ma.us/appealsweb/appealsstatus.aspx
 *
 * Instructions:
 *   1. Go to the MA SoS appeals site in Chrome
 *   2. Set Case Type = Appeal, pick a Year, click "Search for case record"
 *   3. Wait for ALL results to load (can take 20-30 seconds for large years)
 *   4. Open DevTools (Cmd+Option+J), paste this script, press Enter
 *   5. A JSON file will download to your Downloads folder
 *   6. Run:  python3 scripts/import_cases.py ~/Downloads/spr_cases_YEAR.json
 *
 * For multiple years: repeat steps 2-6 for each year.
 */

(function extractAndDownload() {
  const grid = document.getElementById('GrdWebStatusReport');
  if (!grid) {
    alert('No results grid found. Make sure you have searched first and all results are loaded.');
    return;
  }

  const rows = grid.querySelectorAll('tr');
  const cases = [];

  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i].querySelectorAll('td');
    if (cells.length < 9) continue;

    const link = cells[0].querySelector('a');
    const href = link ? link.getAttribute('href') : '';
    const m    = href ? href.match(/AppealNo=([^&]+)/) : null;

    cases.push({
      cn:  cells[0].innerText.trim(),          // case number
      op:  cells[1].innerText.trim(),          // opened date
      cl:  cells[3].innerText.trim(),          // closed date
      st:  cells[6].innerText.trim(),          // status
      rq:  cells[7].innerText.trim(),          // requester
      cu:  cells[8].innerText.trim(),          // custodian/agency
      pdf: !!cells[cells.length-1].querySelector('input[type="image"]'),
      an:  m ? m[1] : ''                       // appeal_no (encoded)
    });
  }

  if (cases.length === 0) {
    alert('No cases found in grid. Results may still be loading — wait and try again.');
    return;
  }

  // Detect the year from the first case number (format: YYYYNNNNN)
  const year = cases[0].cn ? cases[0].cn.substring(0, 4) : 'unknown';
  const filename = `spr_cases_${year}.json`;

  // Download as JSON file
  const blob = new Blob([JSON.stringify(cases, null, 0)], { type: 'application/json' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  URL.revokeObjectURL(url);
  a.remove();

  console.log(`✅ Downloaded ${cases.length} cases as ${filename}`);
  console.log(`   Next: run  python3 scripts/import_cases.py ~/Downloads/${filename}`);
})();
