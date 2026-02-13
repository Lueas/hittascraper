#!/usr/bin/env node
// scripts/scan_finance_labels.js
// Scan output/finance_table_data.jsonl and report tables that would be
// excluded by the updated financing label filter.

const fs = require("fs");
const path = require("path");
const config = require("../src/config");

const FINANCE_RE =
    /\b(kortfrist|långfrist|långfristiga|långfristig|kortfristiga|kortfristig|skuld|skulder|lån|kredit|kreditinstitut|leasing)\b/i;
const EXCLUDE_RE = [
    /\beget kapital\b/i,
    /\bskulder och eget kapital\b/i,
    /\bbalans(r(a|ä)kning)?\b/i,
];

function wouldBeExcluded(label) {
    if (!label) return false;
    const s = label.toString();
    for (const re of EXCLUDE_RE) if (re.test(s)) return true;
    return false;
}

function isFinanceLabel(label) {
    if (!label) return false;
    return FINANCE_RE.test(label.toString());
}

const fp = config.paths.financeJsonl;

if (!fs.existsSync(fp)) {
    console.error(`File not found: ${fp}`);
    console.error("Run the scraper first or place the JSONL at that path.");
    process.exit(2);
}

const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter(Boolean);

let totalOrgs = 0;
let totalTables = 0;
let matchedBefore = 0;
let excludedByNew = 0;
const samples = [];
const excludeCounts = new Map();

for (const line of lines) {
    totalOrgs++;
    let obj = null;
    try {
        obj = JSON.parse(line);
    } catch (e) {
        continue;
    }
    const org = obj.org || obj.name || "<unknown>";
    const tables = Array.isArray(obj.tables) ? obj.tables : [];
    for (const t of tables) {
        totalTables++;
        const lbl = (t && (t.rawLabel || t.label || "")).toString();
        if (isFinanceLabel(lbl)) {
            matchedBefore++;
            if (wouldBeExcluded(lbl)) {
                excludedByNew++;
                samples.push({ org, label: lbl });
                excludeCounts.set(lbl, (excludeCounts.get(lbl) || 0) + 1);
            }
        }
    }
}

console.log("Scan results:");
console.log(`  Orgs scanned:       ${totalOrgs}`);
console.log(`  Tables scanned:     ${totalTables}`);
console.log(`  Financing matches:  ${matchedBefore}`);
console.log(`  Would be excluded:  ${excludedByNew}`);
console.log("");

if (excludedByNew) {
    console.log("Sample excluded labels (up to 20):");
    for (let i = 0; i < Math.min(20, samples.length); i++) {
        console.log(`  - ${samples[i].org}: ${samples[i].label}`);
    }
    console.log("");
    console.log("Top excluded label counts:");
    const sorted = Array.from(excludeCounts.entries()).sort(
        (a, b) => b[1] - a[1],
    );
    for (const [lbl, cnt] of sorted.slice(0, 10)) {
        console.log(`  ${cnt.toString().padStart(5)}  ${lbl}`);
    }
} else {
    console.log("No labels would be excluded by the new filter.");
}

process.exit(0);
