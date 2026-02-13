// src/scrapeFinanceTables.js
function buildExtractor() {
  return () => {
    const norm = (s) => (s || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();

    // On Hitta, the finance tables are: <table data-test="finance-table">
    const tables = Array.from(document.querySelectorAll('table[data-test="finance-table"]'));
    if (!tables.length) return null;

    const results = [];

    for (const table of tables) {
      // YEARS: Hitta often puts years in the FIRST ROW of TBODY as <th class="text-right">2024-12</th>
      // Sometimes it might be in THEAD; handle both.
      let yearCells = Array.from(table.querySelectorAll("thead th.text-right"));
      if (!yearCells.length) {
        const firstRow = table.querySelector("tbody tr");
        if (firstRow) yearCells = Array.from(firstRow.querySelectorAll("th.text-right"));
      }

      const years = yearCells
        .map((th) => norm(th.innerText).slice(0, 4))
        .filter((y) => /^\d{4}$/.test(y))
        .map((y) => parseInt(y, 10));

      // Require at least 2 years to treat as real finance table
      if (years.length < 2) continue;

      const rows = Array.from(table.querySelectorAll("tbody tr"));

      for (const tr of rows) {
        // Skip header-like rows that contain the years (they usually have THs, no TD values)
        const valueTds = Array.from(tr.querySelectorAll("td.text-right"));
        if (!valueTds.length) continue;

        const labelCell = tr.querySelector(".title") || tr.querySelector("th.title") || tr.querySelector("td.title");
        if (!labelCell) continue;

        const rawLabel = norm(labelCell.innerText);
        const values = valueTds.map((td) => norm(td.innerText));

        results.push({ years, rawLabel, values });
      }
    }

    return results.length ? results : null;
  };
}

async function scrapeFinanceTables(page) {
  const extractor = buildExtractor();
  return await page.evaluate(extractor);
}

module.exports = scrapeFinanceTables;
