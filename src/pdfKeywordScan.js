const fs = require("fs-extra");
const path = require("path");
const pdfParse = require("pdf-parse");

const config = require("./config");

function normalizeOrgFromPdfFilename(fileName) {
    const base = path.basename(fileName);
    const m = base.match(/^(\d{10})_/);
    return m ? m[1] : null;
}

function buildKeywordMatchers(keywords) {
    return (keywords || [])
        .map((k) => (k || "").toString().trim())
        .filter(Boolean)
        .map((k) => ({
            keyword: k,
            re: new RegExp(k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"),
        }));
}

async function scanPdfLenderKeywords() {
    const out = new Map(); // orgNr -> Set(keywords)

    if (!fs.existsSync(config.paths.pdfDir)) return out;

    const matchers = buildKeywordMatchers(config.pdfLenderKeywords);
    if (!matchers.length) return out;

    const files = (await fs.readdir(config.paths.pdfDir)).filter((f) =>
        f.toLowerCase().endsWith(".pdf"),
    );

    for (const f of files) {
        const orgNr = normalizeOrgFromPdfFilename(f);
        if (!orgNr) continue;

        const fullPath = path.join(config.paths.pdfDir, f);

        let text = "";
        try {
            const buf = await fs.readFile(fullPath);
            const parsed = await pdfParse(buf);
            text = parsed && parsed.text ? parsed.text : "";
        } catch {
            continue;
        }

        if (!text) continue;

        for (const { keyword, re } of matchers) {
            if (re.test(text)) {
                if (!out.has(orgNr)) out.set(orgNr, new Set());
                out.get(orgNr).add(keyword);
            }
        }
    }

    return out;
}

module.exports = {
    scanPdfLenderKeywords,
};
