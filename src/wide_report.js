// src/wide_report_1year.js
const fs = require("fs-extra");
const path = require("path");
const { createObjectCsvWriter } = require("csv-writer");

const config = require("./config");
const { parseMoneyStringToInt, normalizeLabel } = require("./utils");
const { scanPdfLenderKeywords } = require("./pdfKeywordScan");

function canonicalKey(s) {
    return (s || "")
        .toString()
        .trim()
        .replace(/\s+/g, "")
        .replace(/[^0-9A-Za-zÅÄÖåäö]/g, "");
}

function extractNumericTokens(line, max) {
    const limit = Number(max || 4);
    const text = (line || "").toString().replace(/\u00A0/g, " ");
    const matches =
        text.match(
            /-?\d{1,4}(?:[ \u00A0.]\d{3})+(?:[.,]\d+)?|-?\d+(?:[.,]\d+)?/g,
        ) || [];
    const out = [];
    for (const m of matches) {
        const cleaned = m
            .replace(/\s+/g, "")
            .replace(/\./g, "")
            .replace(/,/g, "");
        if (!cleaned || cleaned === "-" || cleaned === "+") continue;
        if (cleaned.replace(/^-/, "").length > 18) continue;
        out.push(cleaned);
        if (out.length >= limit) break;
    }
    return out;
}

function buildStructuredLineColumns(prefix, items, keys, opts) {
    const options = opts || {};
    const maxLinesPerKey = Number(options.maxLinesPerKey || 10);
    const maxNumsPerLine = Number(options.maxNumsPerLine || 10);
    const out = {};

    const buckets = new Map();
    const add = (key, obj) => {
        const k = canonicalKey(key);
        if (!k) return;
        if (!buckets.has(k)) buckets.set(k, []);
        const arr = buckets.get(k);
        if (arr.length >= maxLinesPerKey) return;
        arr.push(obj);
    };

    for (const it of Array.isArray(items) ? items : []) {
        if (!it) continue;
        if (typeof it === "string") {
            add("Unknown", { key: "Unknown", line: it, lineIndex: null });
            continue;
        }
        const key = (it.key || it.keyword || "Unknown").toString();
        const line = (it.line || "").toString();
        if (!line.trim()) continue;
        add(key, {
            key,
            line,
            lineIndex: it.lineIndex || null,
            values: Array.isArray(it.values) ? it.values : it.values || null,
        });
    }

    const canonicalKeys = Array.from(
        new Set((keys || []).map((k) => canonicalKey(k)).filter(Boolean)),
    );

    for (const ck of canonicalKeys) {
        const arr = buckets.get(ck) || [];
        for (let i = 0; i < maxLinesPerKey; i++) {
            const row = arr[i] || null;
            const base = `${prefix}_${ck}_Line${i + 1}`;
            out[base] = row ? row.line : "";
            out[`${base}_Idx`] =
                row && row.lineIndex ? String(row.lineIndex) : "";
            let nums = [];
            if (row) {
                if (Array.isArray(row.values) && row.values.length) {
                    nums = row.values
                        .map((v) =>
                            (v || "")
                                .toString()
                                .replace(/\u00A0/g, " ")
                                .trim(),
                        )
                        .slice(0, maxNumsPerLine);
                } else {
                    nums = extractNumericTokens(row.line, maxNumsPerLine);
                }
            }
            for (let n = 0; n < maxNumsPerLine; n++) {
                out[`${base}_Num${n + 1}`] = nums[n] || "";
            }
        }
    }

    return out;
}

// NOTE: This report has been simplified to only track:
// - Email / Phone
// - Lender keywords (config.pdfLenderKeywords)
// - Kreditinstitut debts lines (obj.kreditinstitutSkulderLines)

async function exportWide1YearFromJsonl() {
    if (!fs.existsSync(config.paths.financeJsonl)) {
        throw new Error(
            `Missing finance JSONL: ${config.paths.financeJsonl}. Run: npm run scrape (or npm run pipeline) first.`,
        );
    }

    fs.ensureDirSync(config.paths.outputDir);

    const lines = fs
        .readFileSync(config.paths.financeJsonl, "utf8")
        .split(/\r?\n/)
        .filter(Boolean);

    let pdfKeywordMap = new Map();
    try {
        pdfKeywordMap = await scanPdfLenderKeywords();
    } catch {
        pdfKeywordMap = new Map();
    }

    // OrgNr -> row object
    const rowsByOrg = new Map();

    const lenderKeysCanonical = Array.from(
        new Set(
            (config.pdfLenderKeywords || [])
                .map((k) => canonicalKey(k))
                .filter(Boolean),
        ),
    );
    const structuredKeysCanonical = [canonicalKey("Kreditinstitut")].filter(
        Boolean,
    );

    for (const line of lines) {
        let obj;
        try {
            obj = JSON.parse(line);
        } catch {
            continue;
        }

        const orgNr = obj.org;
        const name = obj.name || "Unknown";
        if (!orgNr) continue;

        const jsonlKeywords = Array.isArray(obj.lenderKeywords)
            ? obj.lenderKeywords
            : null;

        let pdfLenderKeywords = "";
        if (jsonlKeywords && jsonlKeywords.length) {
            pdfLenderKeywords = Array.from(new Set(jsonlKeywords))
                .sort((a, b) => a.localeCompare(b, "sv"))
                .join(";");
        } else {
            const pdfKeywordsFound = pdfKeywordMap.get(orgNr);
            pdfLenderKeywords =
                pdfKeywordsFound && pdfKeywordsFound.size
                    ? Array.from(pdfKeywordsFound)
                          .sort((a, b) => a.localeCompare(b, "sv"))
                          .join(";")
                    : "";
        }

        if (!rowsByOrg.has(orgNr)) {
            const derivedKreditinstitutSkulderLines = Array.isArray(
                obj.kreditinstitutSkulderLines,
            )
                ? obj.kreditinstitutSkulderLines
                : Array.isArray(obj.lenderKeywordLines)
                  ? obj.lenderKeywordLines.filter((it) => {
                        try {
                            const key = (it?.key || it?.keyword || "")
                                .toString()
                                .toLowerCase();
                            if (key !== "kreditinstitut") return false;
                            const line = (it?.line || "").toString();
                            return /skuld/i.test(line);
                        } catch {
                            return false;
                        }
                    })
                  : [];

            rowsByOrg.set(orgNr, {
                OrgNr: orgNr,
                Name: name,
                Email: obj.email || "",
                Phone: obj.phone || "",
                Pdf_Lender_Keywords: pdfLenderKeywords,
                Pdf_Lender_Keyword_Lines: Array.isArray(obj.lenderKeywordLines)
                    ? obj.lenderKeywordLines
                          .map((x) =>
                              typeof x === "string"
                                  ? x
                                  : `${x.key || x.keyword || ""}: ${x.line || ""}`,
                          )
                          .filter(Boolean)
                          .join(" || ")
                    : "",
                Kreditinstitut_Skulder_Lines: derivedKreditinstitutSkulderLines
                    .map((x) =>
                        typeof x === "string"
                            ? x
                            : `${x.key || x.keyword || ""}: ${x.line || ""}`,
                    )
                    .filter(Boolean)
                    .join(" || "),
                ...buildStructuredLineColumns(
                    "Pdf_Lender",
                    Array.isArray(obj.lenderKeywordLines)
                        ? obj.lenderKeywordLines
                        : [],
                    structuredKeysCanonical,
                    { maxLinesPerKey: 10, maxNumsPerLine: 10 },
                ),
            });
        } else {
            const r = rowsByOrg.get(orgNr);
            if (!r.Name || r.Name === "Unknown") r.Name = name;
            if (!r.Email && obj.email) r.Email = obj.email;
            if (!r.Phone && obj.phone) r.Phone = obj.phone;
            if (!r.Pdf_Lender_Keywords && pdfLenderKeywords)
                r.Pdf_Lender_Keywords = pdfLenderKeywords;

            if (
                !r.Pdf_Lender_Keyword_Lines &&
                Array.isArray(obj.lenderKeywordLines)
            ) {
                r.Pdf_Lender_Keyword_Lines = obj.lenderKeywordLines
                    .map((x) =>
                        typeof x === "string"
                            ? x
                            : `${x.key || x.keyword || ""}: ${x.line || ""}`,
                    )
                    .filter(Boolean)
                    .join(" || ");
            }

            if (
                !r.Kreditinstitut_Skulder_Lines &&
                (Array.isArray(obj.kreditinstitutSkulderLines) ||
                    Array.isArray(obj.lenderKeywordLines))
            ) {
                const arr = Array.isArray(obj.kreditinstitutSkulderLines)
                    ? obj.kreditinstitutSkulderLines
                    : obj.lenderKeywordLines.filter((it) => {
                          try {
                              const key = (it?.key || it?.keyword || "")
                                  .toString()
                                  .toLowerCase();
                              if (key !== "kreditinstitut") return false;
                              const line = (it?.line || "").toString();
                              return /skuld/i.test(line);
                          } catch {
                              return false;
                          }
                      });
                r.Kreditinstitut_Skulder_Lines = arr
                    .map((x) =>
                        typeof x === "string"
                            ? x
                            : `${x.key || x.keyword || ""}: ${x.line || ""}`,
                    )
                    .filter(Boolean)
                    .join(" || ");
            }

            const lenderStruct = buildStructuredLineColumns(
                "Pdf_Lender",
                Array.isArray(obj.lenderKeywordLines)
                    ? obj.lenderKeywordLines
                    : [],
                structuredKeysCanonical,
                { maxLinesPerKey: 10, maxNumsPerLine: 10 },
            );
            for (const [k, v] of Object.entries(lenderStruct)) {
                if (!r[k] && v) r[k] = v;
            }
        }
    }

    const outRows = [...rowsByOrg.values()];

    const header = [
        { id: "OrgNr", title: "OrgNr" },
        { id: "Name", title: "Name" },
        { id: "Email", title: "Email" },
        { id: "Phone", title: "Phone" },
        { id: "Pdf_Lender_Keywords", title: "Pdf_Lender_Keywords" },
        {
            id: "Pdf_Lender_Keyword_Lines",
            title: "Pdf_Lender_Keyword_Lines",
        },
        {
            id: "Kreditinstitut_Skulder_Lines",
            title: "Kreditinstitut_Skulder_Lines",
        },
        ...(() => {
            const hdr = [];
            // Fewer columns and shorter titles for a cleaner CSV.
            const maxLinesPerKey = 2;
            const maxNumsPerLine = 2;

            const addKey = (prefix, ck) => {
                for (let i = 1; i <= maxLinesPerKey; i++) {
                    const base = `${prefix}_${ck}_Line${i}`;
                    const shortBase = `${ck}_L${i}`;
                    hdr.push({ id: base, title: shortBase });
                    hdr.push({ id: `${base}_Idx`, title: `${shortBase}_Idx` });
                    for (let n = 1; n <= maxNumsPerLine; n++) {
                        hdr.push({
                            id: `${base}_Num${n}`,
                            title: `${shortBase}_N${n}`,
                        });
                    }
                }
            };

            for (const ck of structuredKeysCanonical) addKey("Pdf_Lender", ck);
            return hdr;
        })(),
    ];

    const outPath = path.join(
        config.paths.outputDir,
        "financial_data_wide_1year.csv",
    );

    const writer = createObjectCsvWriter({
        path: outPath,
        header,
    });

    await writer.writeRecords(outRows);
    console.log("Saved", outRows.length, "rows to", outPath);
}

module.exports = exportWide1YearFromJsonl;

if (require.main === module) {
    exportWide1YearFromJsonl().catch((e) => {
        console.error(e);
        process.exit(1);
    });
}
