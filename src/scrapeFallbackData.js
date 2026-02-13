async function scrapeFallbackData(page) {
  return page.evaluate(() => {
    const tables = Array.from(document.querySelectorAll("table"));
    const table = tables.find((t) => /kortfristiga\s+skulder/i.test(t.innerText));
    if (!table) return null;

    // Extract years from table text (prefer 2024 2023 2022)
    const yearsFound = Array.from(table.innerText.matchAll(/\b(20[0-3]\d)\b/g)).map((m) => m[1]);
    const years = Array.from(new Set(yearsFound)).sort((a, b) => parseInt(b) - parseInt(a)).slice(0, 3);
    if (years.length < 2) return null;

    const cleanCell = (s) =>
      (s || "")
        .toString()
        .replace(/\u00A0/g, " ")
        .replace(/\s/g, "")
        .replace(/[^0-9-]/g, "");

    const getRowValues = (labelRegex) => {
      const rows = Array.from(table.querySelectorAll("tr"));
      const row = rows.find((r) => labelRegex.test(r.innerText));
      if (!row) return null;

      const tds = Array.from(row.querySelectorAll("td"));
      // Usually first td is label, remaining are year values
      const vals = tds.map((td) => cleanCell(td.innerText)).filter((v) => v.length > 0);

      // Heuristic: drop the first cell if it contains letters in raw text
      if (tds.length && /[A-Za-zÅÄÖåäö]/.test(tds[0].innerText)) {
        return vals.slice(1, 1 + years.length);
      }
      return vals.slice(0, years.length);
    };

    return {
      years,
      shortDebt: getRowValues(/kortfristiga\s+skulder/i),
      longDebt: getRowValues(/långfristiga\s+skulder/i),
    };
  });
}

module.exports = scrapeFallbackData;
