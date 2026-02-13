// config.js
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const OUTPUT = path.join(ROOT, "output");

module.exports = {
    paths: {
        inputCsv: path.join(ROOT, "input", "input.csv"),
        outputDir: OUTPUT,
        pdfDir: path.join(OUTPUT, "PDFs"),
        outputCsv: path.join(OUTPUT, "financial_data_atomic.csv"),
        contactsCsv: path.join(OUTPUT, "contacts.csv"),
        financeJsonl: path.join(OUTPUT, "finance_table_data.jsonl"),
        wideReportCsv: path.join(OUTPUT, "wide_report.csv"),
    },

    sources: {
        HTML_TABLE: 40,
        PDF_TEXT: 25,
        OCR: 10,
    },

    labelMap: [
        {
            pattern: /^kortfristiga skulder/i,
            label: "Summa Kortfristiga Skulder",
        },
        {
            pattern: /^långfristiga skulder/i,
            label: "Summa Långfristiga Skulder",
        },
    ],

    multiplier: 1000,
    ocrThresholdChars: 120,
    maxPdfPagesForOcr: 10,
    sanityLimit: 5_000_000_000,

    // Keywords to search for inside PDF text extractions (case-insensitive).
    // Used to flag potential credit providers / lenders mentioned in notes.
    pdfLenderKeywords: [
        "Kreditinstitut",
        "Qred",
        "Froda",
        "CapitalBox",
        "Capital Box",
        "Svea",
        "OPR",
        "Capcito",
    ],
};
