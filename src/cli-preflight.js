const fs = require("fs-extra");
const path = require("path");

const config = require("./config");

function getCsvHeaderColumns(csvPath) {
    const raw = fs.readFileSync(csvPath, "utf8");
    const firstLine = raw.split(/\r?\n/)[0] || "";
    return firstLine
        .split(",")
        .map((c) => c.trim().replace(/^\uFEFF/, ""))
        .filter(Boolean);
}

function hasColumn(cols, name) {
    const target = name.toLowerCase();
    return cols.some((c) => c.toLowerCase() === target);
}

(async () => {
    const mode = (process.argv[2] || "online").toLowerCase();

    // Input CSV checks
    if (!fs.existsSync(config.paths.inputCsv)) {
        console.error(`Missing input CSV: ${config.paths.inputCsv}`);
        process.exit(2);
    }

    const cols = getCsvHeaderColumns(config.paths.inputCsv);
    if (!cols.length) {
        console.error(`Input CSV has no header row: ${config.paths.inputCsv}`);
        process.exit(2);
    }

    if (
        !hasColumn(cols, "orgnr") &&
        !hasColumn(cols, "OrgNr") &&
        !hasColumn(cols, "org")
    ) {
        console.error(
            `Input CSV header must include 'orgnr' (or 'OrgNr'/'org'). Found: ${cols.join(", ")}`,
        );
        process.exit(2);
    }

    if (!hasColumn(cols, "name") && !hasColumn(cols, "Name")) {
        console.error(
            `Input CSV header must include 'name' (or 'Name'). Found: ${cols.join(", ")}`,
        );
        process.exit(2);
    }

    // Offline build requires JSONL already present
    if (mode === "offline") {
        if (!fs.existsSync(config.paths.financeJsonl)) {
            console.error(
                `Missing finance JSONL: ${config.paths.financeJsonl}\n` +
                    `Run: npm run scrape (or npm run pipeline) to generate it first.`,
            );
            process.exit(3);
        }

        const stat = fs.statSync(config.paths.financeJsonl);
        if (!stat.size) {
            console.error(
                `Finance JSONL is empty: ${config.paths.financeJsonl}\n` +
                    `Run: npm run scrape to populate it.`,
            );
            process.exit(3);
        }
    }

    // Success
    const rel = (p) => path.relative(process.cwd(), p);
    console.log("Preflight OK:");
    console.log("- input:", rel(config.paths.inputCsv));
    if (mode === "offline")
        console.log("- financeJsonl:", rel(config.paths.financeJsonl));
    process.exit(0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
