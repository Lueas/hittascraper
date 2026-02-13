const config = require("./config");

const MONEY_GROUP_REGEX = /-?\d{1,3}(?:\s\d{3})+/g;
const YEAR_REGEX = /\b(20[0-3]\d)\b/g;

function normalizeSpaces(s) {
  return (s || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}

function parseMoneyStringToInt(raw) {
  if (!raw) return null;
  const s = normalizeSpaces(raw).replace(/[^\d\s-]/g, "");
  if (!s) return null;
  const compact = s.replace(/\s+/g, "");
  const v = parseInt(compact, 10);
  if (!Number.isFinite(v)) return null;
  if (Math.abs(v) > config.sanityLimit) return null;
  return v;
}

function extractYearsFromHeaderText(text) {
  const years = [...normalizeSpaces(text).matchAll(YEAR_REGEX)].map(m => parseInt(m[1], 10));
  const unique = [...new Set(years)];
  return unique.length >= 2 ? unique : null;
}

function normalizeLabel(rawLabel) {
  const s = normalizeSpaces(rawLabel).toLowerCase();
  for (const m of config.labelMap) {
    if (m.pattern.test(s)) return m.label;
  }
  return null;
}

function extractMoneyGroups(line) {
  const s = normalizeSpaces(line);
  const out = [];
  for (const m of s.matchAll(MONEY_GROUP_REGEX)) {
    const v = parseMoneyStringToInt(m[0]);
    if (v !== null) out.push(v);
  }
  return out;
}

function parseLineFromText(line, yearContext) {
  const raw = normalizeSpaces(line);
  if (!raw) return null;

  const label = normalizeLabel(raw);
  if (!label) return null;

  if (/avskrivningar|resultat före skatt/i.test(raw)) return null;

  if (!yearContext || yearContext.length < 2) return null;

  const values = extractMoneyGroups(raw);
  if (!values.length) return null;

  let mapped = values;
  if (values.length > yearContext.length) {
    mapped = values.slice(-yearContext.length);
  }

  if (mapped.length !== yearContext.length) return null;

  return {
    label,
    raw_line: raw,
    data: yearContext.map((y, i) => ({ year: y, value: mapped[i] }))
  };
}

module.exports = {
  normalizeSpaces,
  parseMoneyStringToInt,
  extractYearsFromHeaderText,
  normalizeLabel,
  parseLineFromText
};
