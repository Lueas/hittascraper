// src/scraper.js
const fs = require("fs-extra");
const csv = require("csv-parser");
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
const pdfParse = require("pdf-parse");
const config = require("./config");
const scrapeFinanceTables = require("./scrapeFinanceTables");
const { extractMatchedLinesFromPdfBufferXY } = require("./pdfXYExtract");
const os = require("os");
const path = require("path");

puppeteer.use(Stealth());

// Keep a reference to the active browser so external code (signal handlers)
// can ask the scraper to close it cleanly on SIGINT/SIGTERM.
let _activeBrowser = null;
// Flag to request a graceful stop between work items
let _abortRequested = false;

function requestAbort() {
    _abortRequested = true;
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function setupFastPage(page) {
    const fastSetupTimeoutMs = Number.parseInt(
        (process.env.FAST_PAGE_SETUP_TIMEOUT_MS || "8000").toString(),
        10,
    );

    try {
        await withTimeout(
            page.setCacheEnabled(true),
            fastSetupTimeoutMs,
            "setCacheEnabled",
        );
    } catch {}

    // Abort heavy resources (images/fonts/styles) to speed up.
    try {
        // setRequestInterception can occasionally hang on some Chromium builds.
        await withTimeout(
            page.setRequestInterception(true),
            fastSetupTimeoutMs,
            "setRequestInterception",
        );
        page.on("request", (req) => {
            try {
                const rt = req.resourceType();
                if (
                    rt === "image" ||
                    rt === "stylesheet" ||
                    rt === "font" ||
                    rt === "media"
                ) {
                    req.abort();
                } else {
                    req.continue();
                }
            } catch {
                try {
                    req.continue();
                } catch {}
            }
        });
    } catch {}
}

function jitterDelay(baseMs) {
    const base = Number(baseMs || 0);
    if (!base) return 0;
    const jitter = Math.floor(Math.random() * Math.min(400, base));
    return base + jitter;
}

function withTimeout(promise, ms, label) {
    const timeoutMs = Number(ms || 0);
    if (!timeoutMs) return promise;
    return Promise.race([
        promise,
        new Promise((_, reject) => {
            const t = setTimeout(() => {
                clearTimeout(t);
                reject(
                    new Error(
                        `Timeout after ${timeoutMs}ms${label ? ` (${label})` : ""}`,
                    ),
                );
            }, timeoutMs);
        }),
    ]);
}

// Robust click helper that avoids detached element handles by clicking via
// page.evaluate and re-trying common CMP/cookie interference between attempts.
async function safeClick(page, selector, opts) {
    const options = opts || {};
    const retries = Number(options.retries || 3);
    const waitTimeout = Number(options.waitTimeout || 2000);
    const postDelay = Number(options.postDelay || 300);

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            // Wait for selector to appear briefly
            try {
                await page.waitForSelector(selector, { timeout: waitTimeout });
            } catch {}

            const clicked = await page.evaluate((sel) => {
                const el = document.querySelector(sel);
                if (!el) return false;
                try {
                    el.scrollIntoView({ block: "center", inline: "center" });
                } catch {}
                try {
                    el.click();
                    return true;
                } catch {
                    // fallback to dispatching MouseEvent
                    try {
                        const ev = new MouseEvent("click", {
                            bubbles: true,
                            cancelable: true,
                        });
                        el.dispatchEvent(ev);
                        return true;
                    } catch {
                        return false;
                    }
                }
            }, selector);

            if (clicked) {
                await sleep(postDelay);
                return true;
            }

            // If element not found or click failed, try accepting cookies then retry
            await tryAcceptCookies(page);
            await sleep(250);
        } catch (err) {
            if (attempt >= retries) throw err;
            await tryAcceptCookies(page);
            await sleep(300 + attempt * 100);
        }
    }
    return false;
}

// Load already scraped orgs from jsonl so we can resume safely
function loadAlreadyScrapedSet() {
    const set = new Set();
    if (!fs.existsSync(config.paths.financeJsonl)) return set;

    const lines = fs
        .readFileSync(config.paths.financeJsonl, "utf8")
        .split(/\r?\n/)
        .filter(Boolean);
    for (const line of lines) {
        try {
            const obj = JSON.parse(line);
            if (obj.org) set.add(obj.org);
        } catch {}
    }
    return set;
}

async function tryAcceptCookies(page) {
    // Common Hitta cookie button is #modalConfirmBtn (you used it earlier)
    try {
        const btn = await page.$("#modalConfirmBtn");
        if (btn) {
            await btn.click();
            await sleep(600);
        }
    } catch {}

    // Some sessions use OneTrust
    try {
        const btn = await page.$("#onetrust-accept-btn-handler");
        if (btn) {
            await btn.click();
            await sleep(600);
        }
    } catch {}

    // Fallback: click any visible button that looks like accept/approve
    try {
        // Specific Gravito/gravitoTCFCPM modal buttons seen on Hitta
        const gravitoSelectors = [
            "#gravitoTCFCPM-layer1-accept-all",
            "#gravitoTCFCPM-layer1-accept",
            "#gravitoCMP-accept-all",
            '[id^="gravito"][id$="accept-all"]',
            '[data-test="accept-all"]',
        ];

        let clicked = false;
        for (const sel of gravitoSelectors) {
            try {
                const b = await page.$(sel);
                if (b) {
                    try {
                        await b.evaluate((el) =>
                            el.scrollIntoView({ block: "center" }),
                        );
                    } catch {}
                    await b.click();
                    await sleep(500);
                    clicked = true;
                    break;
                }
            } catch {}
        }

        // If not found, try generic text-based accept (handles different CMPs/languages)
        if (!clicked) {
            await page.evaluate(() => {
                const buttons = Array.from(document.querySelectorAll("button"));
                const matcher =
                    /(accept all|accept|agree|godkän(n)? alla|godkänn|tillåt|ok)/i;
                const btn = buttons.find((b) =>
                    matcher.test((b.innerText || "").trim()),
                );
                if (btn) btn.click();
            });
            await sleep(400);
        }

        // If Gravito lives inside a modal container, try to click internal buttons by querying the container
        try {
            const root = await page.$("#gravitoCMPRoot");
            if (root) {
                await page.evaluate(() => {
                    const rootEl = document.querySelector("#gravitoCMPRoot");
                    if (!rootEl) return;
                    const btn =
                        rootEl.querySelector('button[id*="accept"]') ||
                        rootEl.querySelector('button[aria-label*="accept"]');
                    if (btn) btn.click();
                });
                await sleep(300);
            }
        } catch {}

        // As a last resort, try frames (some CMPs render into an iframe)
        try {
            for (const f of page.frames()) {
                try {
                    const btn = await f.$(
                        'button[id*="gravito"], button[id*="accept"], button[title*="accept"], button[aria-label*="accept"]',
                    );
                    if (btn) {
                        try {
                            await btn.click();
                        } catch {
                            await f.evaluate((b) => b.click(), btn);
                        }
                        await sleep(300);
                        break;
                    }
                } catch {}
            }
        } catch {}
    } catch {}
}

async function waitForFinanceTables(page) {
    // Wait for finance section/tables to appear (they are rendered client-side)
    // If it never appears, we still proceed and let the extractor return null.
    try {
        await page.waitForSelector('table[data-test="finance-table"]', {
            timeout: 8000,
        });
    } catch {}
}

function buildContactExtractor() {
    return () => {
        const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));

        const norm = (s) =>
            (s || "")
                .toString()
                .replace(/\u00A0/g, " ")
                .replace(/\s+/g, " ")
                .trim();

        const emailRe = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
        const phoneRe = /^\+?\d[\d\s\-()]{6,}\d$/;

        const censusVals = Array.from(
            document.querySelectorAll("[data-census-details]"),
        ).map((el) => norm(el.getAttribute("data-census-details")));

        const emailsFromCensus = censusVals.filter((v) => emailRe.test(v));
        const phonesFromCensus = censusVals
            .map((v) => v.replace(/\s+/g, ""))
            .filter((v) => phoneRe.test(v));

        // Extra fallbacks: mailto: links, tel: links, and visible text
        const mailtoEmails = Array.from(
            document.querySelectorAll('a[href^="mailto:"]'),
        )
            .map(
                (a) =>
                    norm(a.getAttribute("href"))
                        .replace(/^mailto:/i, "")
                        .split("?")[0],
            )
            .filter((v) => emailRe.test(v));

        const telPhones = Array.from(
            document.querySelectorAll('a[href^="tel:"]'),
        )
            .map((a) => norm(a.getAttribute("href")).replace(/^tel:/i, ""))
            .map((v) => v.replace(/\s+/g, ""))
            .filter((v) => phoneRe.test(v));

        // If the site renders the number into the DOM after reveal,
        // it often appears as a +46... token in text.
        const textPhones = uniq(
            (document.body?.innerText || "")
                .split(/\r?\n/)
                .flatMap((line) => line.match(/\+\d[\d\s\-()]{6,}\d/g) || [])
                .map((v) => v.replace(/\s+/g, ""))
                .filter((v) => phoneRe.test(v)),
        );

        const emails = uniq([...emailsFromCensus, ...mailtoEmails]);
        const phones = uniq([...phonesFromCensus, ...telPhones, ...textPhones]);

        return {
            email: emails[0] || null,
            phone: phones[0] || null,
            emails,
            phones,
        };
    };
}

async function scrapeContactDetails(page) {
    // Some org pages keep contact details behind a "Kontakt" tab.
    try {
        const tab = await page.$('[data-test="tab-company-contact"]');
        if (tab) {
            try {
                await safeClick(page, '[data-test="tab-company-contact"]', {
                    retries: 3,
                    waitTimeout: 1500,
                    postDelay: 600,
                });
            } catch {}
        }
    } catch {}

    // Some org pages show email via an explicit email button.
    try {
        await safeClick(page, '[data-test="company-email-button"]', {
            retries: 3,
            waitTimeout: 1200,
            postDelay: 500,
        });
    } catch {}

    // Give client-side rendering a moment and wait for common contact indicators.
    try {
        await page.waitForFunction(
            () =>
                Boolean(
                    document.querySelector("[data-census-details]") ||
                    document.querySelector('a[href^="mailto:"]') ||
                    document.querySelector('a[href^="tel:"]') ||
                    document.querySelector('[data-test="show-numbers-button"]'),
                ),
            { timeout: 5000 },
        );
    } catch {}

    // Best-effort: sometimes the phone number is only revealed after clicking.
    try {
        await safeClick(page, '[data-test="show-numbers-button"]', {
            retries: 2,
            waitTimeout: 1000,
            postDelay: 500,
        });
    } catch {}

    const extractor = buildContactExtractor();
    return await page.evaluate(extractor);
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

function buildLoanMatchers() {
    // Narrow set of loan/financing terms to avoid noisy matches (exclude earnings/etc).
    // Only include terms we want in the final outputs: Kortfristiga, Långfristiga,
    // Kreditinstitut and general "Skuld/Skulder" mentions.
    return [
        { key: "Kortfristiga", re: /\bkortfristiga\b|\bkortfristig\b/i },
        {
            key: "Långfristiga",
            re: /\blångfristiga\b|\blångfristig\b|\blangfristiga\b|\blangfristig\b/i,
        },
        { key: "Kreditinstitut", re: /\bkreditinstitut\b/i },
    ];
}

function normalizeScanText(text) {
    return (text || "")
        .toString()
        .replace(/\u0000/g, " ")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n");
}

// Extract numeric tokens from a line. Matches grouped thousands like "22 875 000"
// and plain numbers. Returns array of matched strings (preserves grouping spaces).
function splitGroupedNumberRun(run, opts) {
    const options = opts || {};
    const preferredCount = Number(options.preferredCount || 0);
    const cleaned = (run || "").replace(/[\u00A0\s]+/g, " ").trim();
    if (!cleaned) return [];

    const tokens = cleaned.split(" ").filter(Boolean);
    if (!tokens.length) return [];
    if (tokens.length === 1) return [cleaned];

    const stripSign = (s) => s.replace(/^[+-]/, "");

    const expandMergedDigitToken = (token) => {
        const t = (token || "").toString();
        const sign = t.startsWith("-") || t.startsWith("+") ? t[0] : "";
        const d = stripSign(t);
        if (!/^\d+$/.test(d)) return [t];
        if (d.length <= 3) return [t];

        // Keep likely years intact.
        if (d.length === 4 && /^20\d{2}$/.test(d)) return [t];

        if (d.length === 4) return [`${sign}${d.slice(0, 3)}`, d.slice(3)];
        if (d.length === 5) return [`${sign}${d.slice(0, 2)}`, d.slice(2)];
        if (d.length === 6) return [`${sign}${d.slice(0, 3)}`, d.slice(3)];

        return [t];
    };

    // OCR repair heuristic for 2-year value pairs occasionally extracted as
    // "9 1330" (intended: "9133" and "0").
    if (preferredCount === 2 && tokens.length === 2) {
        const a = stripSign(tokens[0] || "");
        const b = stripSign(tokens[1] || "");
        if (/^\d{1,3}$/.test(a) && /^\d{4,6}$/.test(b)) {
            const left = `${a}${b.slice(0, 3)}`;
            const right = b.slice(3);
            if (left && right) return [left, right];
        }
    }

    const workingTokens = [];
    for (const token of tokens) {
        const expanded =
            preferredCount > 0 ? expandMergedDigitToken(token) : [token];
        for (const p of expanded) {
            if (p) workingTokens.push(p);
        }
    }

    if (!workingTokens.length) return [];

    // If tokens are plain long numbers (e.g. years), keep them separate.
    if (workingTokens.some((t) => !/^[+-]?\d+$/.test(t))) return [cleaned];
    if (workingTokens.some((t) => t.replace(/^[+-]/, "").length > 3)) {
        return workingTokens;
    }

    // Heuristics for 2-column (year-pair) lines.
    // These lines often contain footnote refs (e.g. "3, 4") and two values.
    // Prefer splitting "4 990 429 295 000" -> ["4 990 429", "295 000"].
    if (preferredCount === 2) {
        const strip = (x) => stripSign((x || "").toString());
        const t = workingTokens.slice();
        const allDigits = t.every((x) => /^[-+]?\d+$/.test(x));

        if (allDigits) {
            const lens = t.map((x) => strip(x).length);
            const isGroup3 = (i) => lens[i] === 3;
            const isLead = (i) => lens[i] >= 1 && lens[i] <= 3;

            // Fix: duplicated single-digit note + leading digit ("4 4 990 429") => "4 990 429".
            if (
                t.length === 4 &&
                strip(t[0]).length === 1 &&
                strip(t[1]).length === 1 &&
                strip(t[0]) === strip(t[1]) &&
                isGroup3(2) &&
                isGroup3(3)
            ) {
                return [`${t[1]} ${t[2]} ${t[3]}`];
            }

            // Fix: merged leading zero in first group: "04 965 842" => ["0", "4 965 842"].
            if (
                t.length === 3 &&
                /^0\d$/.test(strip(t[0])) &&
                isGroup3(1) &&
                isGroup3(2)
            ) {
                const first = strip(t[0]);
                return ["0", `${first.slice(1)} ${t[1]} ${t[2]}`];
            }

            // Fix: footnote digit merged into a 3-digit first group: "404 965 842" likely means
            // note "4" + value "0" + value "4 965 842" => ["0", "4 965 842"].
            if (
                t.length === 3 &&
                /^\d{3}$/.test(strip(t[0])) &&
                isGroup3(1) &&
                isGroup3(2)
            ) {
                const g0 = strip(t[0]);
                if (/^[1-9]0\d$/.test(g0)) {
                    return ["0", `${g0.slice(2)} ${t[1]} ${t[2]}`];
                }
            }

            // Fix: duplicated leading digit from footnote: "44 990 429" => "4 990 429".
            if (
                t.length === 3 &&
                /^\d{2}$/.test(strip(t[0])) &&
                isGroup3(1) &&
                isGroup3(2)
            ) {
                const g0 = strip(t[0]);
                if (g0[0] === g0[1]) {
                    return [`${g0[0]} ${t[1]} ${t[2]}`];
                }
            }

            // Common 5-token pattern: 1-3 digits + 4x 3-digit groups.
            // Choose 3+2 split (not 2+3) to avoid "4 990" + "429 295 000".
            if (
                t.length === 5 &&
                isLead(0) &&
                isGroup3(1) &&
                isGroup3(2) &&
                isGroup3(3) &&
                isGroup3(4)
            ) {
                return [t.slice(0, 3).join(" "), t.slice(3).join(" ")];
            }

            // Another common case from noisy extraction: footnote digit + "0" + grouped number.
            // Example: "4 0 4 965 842" should become ["0", "4 965 842"].
            if (t.length >= 4) {
                const last3 = t.slice(-3);
                const last3Lens = last3.map((x) => strip(x).length);
                const last3LooksGrouped =
                    last3Lens[0] >= 1 &&
                    last3Lens[0] <= 3 &&
                    last3Lens[1] === 3 &&
                    last3Lens[2] === 3;

                const hasZero =
                    t.includes("0") || t.includes("+0") || t.includes("-0");
                if (hasZero && last3LooksGrouped) {
                    // Keep first zero we see, and the grouped number at the end.
                    const z = t.find((x) => strip(x) === "0") || "0";
                    return [stripSign(z), last3.join(" ")];
                }
            }
        }
    }

    const segCost = (start, len) => {
        let cost = 0;
        if (len === 1) cost += 1.2;
        else if (len === 2) cost += 0;
        else if (len === 3) cost += 0.1;
        else if (len === 4) cost += 0.6;
        else cost += 2 + (len - 4) * 1.5;

        const first = stripSign(workingTokens[start] || "");
        if (!/^\d{1,3}$/.test(first)) cost += 4;
        if (/^0\d+$/.test(first) || first === "000") cost += 2;

        for (let i = 1; i < len; i++) {
            const part = stripSign(workingTokens[start + i] || "");
            if (!/^\d{3}$/.test(part)) cost += 4;
        }

        // Prefer solutions that produce expected columns (commonly 2-3 values).
        return cost;
    };

    const n = workingTokens.length;
    const dp = Array.from({ length: n + 1 }, () => null);
    dp[0] = { cost: 0, parts: [] };

    for (let i = 0; i < n; i++) {
        if (!dp[i]) continue;
        for (const len of [2, 3, 1, 4]) {
            const j = i + len;
            if (j > n) continue;
            const nextParts = dp[i].parts.length + 1;
            let nextCost = dp[i].cost + segCost(i, len);

            if (preferredCount > 0) {
                nextCost += Math.abs(nextParts - preferredCount) * 0.15;
            }

            if (!dp[j] || nextCost < dp[j].cost) {
                dp[j] = {
                    cost: nextCost,
                    parts: [...dp[i].parts, [i, j]],
                };
            }
        }
    }

    const best = dp[n];
    if (!best || !best.parts || !best.parts.length) return [cleaned];

    return best.parts.map(([a, b]) => workingTokens.slice(a, b).join(" "));
}

function extractNumbersFromLine(line, opts) {
    const options = opts || {};
    const preferredCount = Number(options.preferredCount || 0);
    if (!line) return [];
    const s = line
        .replace(/[\u00A0]+/g, " ")
        .replace(/[−–—]/g, "-")
        .replace(/[\t]+/g, " ");

    // Match numeric runs that may include grouped thousands and signs.
    const re = /[+-]?\d[\d ]*/g;
    const matches = [];
    let m;
    while ((m = re.exec(s)) !== null) {
        const run = (m[0] || "").replace(/[ ]+/g, " ").trim();
        if (!run) continue;
        const parts = splitGroupedNumberRun(run, { preferredCount });
        for (const p of parts) {
            const v = (p || "").replace(/[ ]+/g, " ").trim();
            if (v) matches.push(v);
        }
    }
    return matches;
}

function stripNumbersFromLine(line) {
    if (!line) return "";
    const s = line
        .toString()
        .replace(/[\u00A0]+/g, " ")
        .replace(/[−–—]/g, "-")
        .replace(/[+-]?\d[\d ]*/g, " ")
        .replace(/\s+/g, " ")
        .trim();

    const cleaned = s.replace(/[,:;\-]+$/g, "").trim();

    // Collapse exact repeated phrases caused by OCR duplication
    // e.g. "Kortfristiga fordringarKortfristiga fordringar".
    const dup = cleaned.match(/^(.{4,}?)\1$/u);
    if (dup && dup[1]) return dup[1].trim();
    return cleaned;
}

function normalizeTableValues(values, expectedCount) {
    const arr = Array.isArray(values)
        ? values.map((v) => (v || "").toString().replace(/\s+/g, " ").trim())
        : [];
    if (!arr.length) return [];
    if (!expectedCount || arr.length >= expectedCount) return arr;

    // If table values got merged into fewer cells, split numerics to expected columns.
    const flattened = [];
    for (const item of arr) {
        const split = extractNumbersFromLine(item, {
            preferredCount: expectedCount,
        });
        if (split.length > 1) flattened.push(...split);
        else flattened.push(item);
    }

    if (flattened.length === expectedCount) return flattened;
    return arr;
}

// Return true if a table label looks like a financing/liability row
function isFinancingTableLabel(label) {
    if (!label) return false;
    const s = label.toString().toLowerCase();

    // Exclude combined sections that include equity or are full-balance lines
    // (e.g. "Skulder och eget kapital") since they are not pure financing rows.
    const exclude = [
        /\beget kapital\b/i,
        /\bskulder och eget kapital\b/i,
        /\bbalans(r(a|ä)kning)?\b/i,
    ];
    for (const re of exclude) {
        if (re.test(s)) return false;
    }

    // include common financing terms
    return /\b(kortfrist|långfrist|långfristiga|långfristig|kortfristiga|kortfristig|skuld|skulder|lån|kredit|kreditinstitut|leasing)\b/i.test(
        s,
    );
}

function extractMatchedLines(text, matchers, opts) {
    const options = opts || {};
    const maxLinesPerKey = Number(options.maxLinesPerKey || 10);
    const maxTotalLines = Number(options.maxTotalLines || 60);

    const out = [];
    const normalized = normalizeScanText(text);
    const lines = normalized
        .split("\n")
        .map((l) => l.replace(/\s+/g, " ").trim())
        .filter(Boolean);

    const counts = new Map();
    const seen = new Set();

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        for (const m of matchers) {
            const key = m.keyword || m.key || "";
            if (!key) continue;
            if (m.re && !m.re.test(line)) continue;

            const cnt = counts.get(key) || 0;
            if (cnt >= maxLinesPerKey) continue;

            const dedupeKey = `${key}|${line}`;
            if (seen.has(dedupeKey)) continue;

            out.push({ key, line, lineIndex: i + 1 });
            seen.add(dedupeKey);
            counts.set(key, cnt + 1);

            if (out.length >= maxTotalLines) return out;
        }
    }

    return out;
}

async function findAnnualReportPdfUrls(page) {
    return await page.evaluate(() => {
        const abs = (href) => {
            try {
                return new URL(href, window.location.href).toString();
            } catch {
                return null;
            }
        };

        const anchors = Array.from(document.querySelectorAll("a[href]"));

        const candidates = anchors
            .map((a) => {
                const href = abs(a.getAttribute("href"));
                if (!href) return null;
                const text = (a.innerText || "").toString().trim();
                const aria = (a.getAttribute("aria-label") || "")
                    .toString()
                    .trim();
                return { href, text, aria };
            })
            .filter(Boolean);

        const score = (c) => {
            const hay = `${c.text} ${c.aria} ${c.href}`.toLowerCase();
            let s = 0;
            if (hay.includes("årsredovis")) s += 20;
            if (hay.includes("arsredovis")) s += 20;
            if (hay.includes("annual")) s += 3;
            if (hay.includes("report")) s += 2;
            if (hay.includes("bokslut")) s += 1;
            if (hay.includes("pdf")) s += 3;
            // prefer direct files over e.g. viewer wrappers
            if (/\.pdf(\?|#|$)/i.test(c.href)) s += 8;
            return s;
        };

        const sorted = candidates
            .map((c) => ({ ...c, score: score(c) }))
            .filter((c) => c.score > 0)
            .sort((a, b) => b.score - a.score);

        // return unique hrefs in best-first order
        const out = [];
        const seen = new Set();
        for (const c of sorted) {
            if (!seen.has(c.href)) {
                seen.add(c.href);
                out.push(c.href);
            }
            if (out.length >= 8) break;
        }
        return out;
    });
}

async function scanKeywordsInText(text, matchers) {
    const found = new Set();
    if (!text) return found;
    for (const { keyword, re } of matchers) {
        if (re.test(text)) found.add(keyword);
    }
    return found;
}

function buildTimeoutPromise(ms) {
    return new Promise((resolve) => setTimeout(() => resolve(null), ms));
}

async function waitForPdfResponseBuffer(page, timeoutMs) {
    return await new Promise((resolve) => {
        let settled = false;

        const done = (buf) => {
            if (settled) return;
            settled = true;
            try {
                page.off("response", onResponse);
            } catch {}
            resolve(buf || null);
        };

        const onResponse = async (resp) => {
            if (settled) return;
            try {
                const headers = resp.headers() || {};
                const ct = (headers["content-type"] || "").toString();
                const cd = (headers["content-disposition"] || "").toString();
                const url = (resp.url() || "").toString();

                const looksLikePdf =
                    /application\/pdf/i.test(ct) ||
                    /\bpdf\b/i.test(ct) ||
                    /filename=.*\.pdf/i.test(cd) ||
                    /\.pdf(\?|#|$)/i.test(url);

                if (!looksLikePdf) return;

                const buf = await resp.buffer();
                const head = buf.slice(0, 8).toString("latin1");
                if (!head.startsWith("%PDF-")) return;

                done(buf);
            } catch {
                // ignore
            }
        };

        page.on("response", onResponse);

        setTimeout(() => done(null), timeoutMs);
    });
}

async function tryDownloadAnnualReportPdfViaButtons(page) {
    const selector =
        'button[data-test="download-report-button"],button[data-test^="download-report-button"]';
    const buttons = await page.$$(selector);
    if (!buttons.length) return { tried: false, buf: null };

    // Try a few buttons (usually newest year is first)
    const maxTries = Math.min(4, buttons.length);

    // Prepare a temporary download directory so browser-initiated downloads
    // go into a controlled location we can clean up. Not all clicks trigger
    // network responses we can capture; this covers the cases where the
    // browser performs a direct file download to disk.
    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hitta_dl_"));
    let cdpClient = null;
    try {
        try {
            cdpClient = await page.target().createCDPSession();
            await cdpClient.send("Page.setDownloadBehavior", {
                behavior: "allow",
                downloadPath: tmpRoot,
            });
        } catch {
            cdpClient = null;
        }
    } catch {}

    for (let i = 0; i < maxTries; i++) {
        // Click the Nth matched button via page.evaluate to avoid detached handles.
        try {
            await tryAcceptCookies(page);
        } catch {}

        const pdfBufPromise = waitForPdfResponseBuffer(page, 20000);

        try {
            await page.evaluate(
                (sel, idx) => {
                    const els = Array.from(document.querySelectorAll(sel));
                    const el = els[idx];
                    if (!el) return false;
                    try {
                        el.scrollIntoView({
                            block: "center",
                            inline: "center",
                        });
                    } catch {}
                    try {
                        el.click();
                        return true;
                    } catch {
                        try {
                            const ev = new MouseEvent("click", {
                                bubbles: true,
                                cancelable: true,
                            });
                            el.dispatchEvent(ev);
                            return true;
                        } catch {
                            return false;
                        }
                    }
                },
                selector,
                i,
            );
        } catch {}

        const buf = await Promise.race([
            pdfBufPromise,
            buildTimeoutPromise(21000),
        ]);
        if (buf) {
            // Clean up any temp files if present
            try {
                const files = fs.readdirSync(tmpRoot || "") || [];
                for (const f of files) {
                    try {
                        fs.unlinkSync(path.join(tmpRoot, f));
                    } catch {}
                }
            } catch {}
            try {
                fs.rmSync(tmpRoot, { recursive: true, force: true });
            } catch {}
            return { tried: true, buf };
        }

        // If we didn't capture the PDF via network response, check the
        // temporary download directory for any files the browser saved.
        try {
            const deadline = Date.now() + 6000;
            while (Date.now() < deadline) {
                const files = fs.readdirSync(tmpRoot || "") || [];
                if (files && files.length) {
                    // Prefer the newest file
                    files.sort((a, b) => {
                        const sa =
                            fs.statSync(path.join(tmpRoot, a)).mtimeMs || 0;
                        const sb =
                            fs.statSync(path.join(tmpRoot, b)).mtimeMs || 0;
                        return sb - sa;
                    });
                    for (const f of files) {
                        const fp = path.join(tmpRoot, f);
                        try {
                            const fileBuf = fs.readFileSync(fp);
                            if (
                                fileBuf &&
                                fileBuf
                                    .slice(0, 8)
                                    .toString("latin1")
                                    .startsWith("%PDF-")
                            ) {
                                // remove file then return buffer
                                try {
                                    fs.unlinkSync(fp);
                                } catch {}
                                try {
                                    fs.rmSync(tmpRoot, {
                                        recursive: true,
                                        force: true,
                                    });
                                } catch {}
                                return { tried: true, buf: fileBuf };
                            } else {
                                try {
                                    fs.unlinkSync(fp);
                                } catch {}
                            }
                        } catch {}
                    }
                }
                await sleep(200);
            }
        } catch {}
    }

    try {
        fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch {}
    return { tried: true, buf: null };
}

async function scrapeLenderKeywordsPdfFirst(browser, page) {
    const matchers = buildKeywordMatchers(config.pdfLenderKeywords);
    const USE_XY_COORDS = /^1|true$/i.test(
        (process.env.USE_XY_COORDS || "").toString(),
    );
    if (!matchers.length) {
        return {
            keywords: [],
            keywordLines: [],
            source: "NONE",
            pdfUrl: null,
        };
    }

    const blendXyAndTextLines = (xyLines, textLines) => {
        const x = Array.isArray(xyLines) ? xyLines : [];
        const t = Array.isArray(textLines) ? textLines : [];
        if (!x.length) return t;
        if (!t.length) return x;

        const withValues = (arr) =>
            arr.map((it) => {
                const base = it || {};
                const line = (base.line || "").toString();
                const values =
                    Array.isArray(base.values) && base.values.length
                        ? base.values
                        : extractNumbersFromLine(line, { preferredCount: 2 });
                return { ...base, values: Array.isArray(values) ? values : [] };
            });

        const xy = withValues(x);
        const tx = withValues(t);

        const byKey = (arr) => {
            const m = new Map();
            for (const it of arr) {
                const k = (it.key || it.keyword || "").toString();
                if (!m.has(k)) m.set(k, []);
                m.get(k).push(it);
            }
            return m;
        };

        const xyMap = byKey(xy);
        const txMap = byKey(tx);
        const keys = new Set([...xyMap.keys(), ...txMap.keys()]);
        const out = [];

        for (const key of keys) {
            const xa = xyMap.get(key) || [];
            const ta = txMap.get(key) || [];
            const n = Math.max(xa.length, ta.length);
            for (let i = 0; i < n; i++) {
                const xv = xa[i] || null;
                const tv = ta[i] || null;
                if (!xv && tv) {
                    out.push(tv);
                    continue;
                }
                if (xv && !tv) {
                    out.push(xv);
                    continue;
                }

                const xs =
                    (xv.values || []).length >= 2
                        ? 2
                        : (xv.values || []).length;
                const ts =
                    (tv.values || []).length >= 2
                        ? 2
                        : (tv.values || []).length;
                out.push(xs >= ts ? xv : tv);
            }
        }

        return out;
    };

    // 1) Try annual report PDFs first
    let pdfUrls = [];
    try {
        pdfUrls = await findAnnualReportPdfUrls(page);
    } catch {
        pdfUrls = [];
    }

    let pdfPage = null;
    try {
        if (pdfUrls.length) {
            pdfPage = await browser.newPage();
            await setupFastPage(pdfPage);
        }

        for (const url of pdfUrls) {
            const resp = await pdfPage.goto(url, {
                waitUntil: "domcontentloaded",
                timeout:
                    Number.parseInt(
                        (process.env.NAV_TIMEOUT_MS || "30000").toString(),
                        10,
                    ) || 30000,
            });

            if (!resp || !resp.ok()) {
                continue;
            }

            const buf = await resp.buffer();

            // Some links are to HTML viewers/download endpoints. Confirm PDF bytes.
            const head = buf.slice(0, 8).toString("latin1");
            if (!head.startsWith("%PDF-")) {
                continue;
            }

            const parsed = await pdfParse(buf);
            const text = parsed && parsed.text ? parsed.text : "";
            const hits = await scanKeywordsInText(text, matchers);
            let keywordLines = extractMatchedLines(text, matchers, {
                maxLinesPerKey: 10,
                maxTotalLines: 60,
            });
            let source = "PDF";

            if (USE_XY_COORDS) {
                try {
                    const keywordLinesXY =
                        await extractMatchedLinesFromPdfBufferXY(
                            buf,
                            matchers,
                            {
                                maxLinesPerKey: 10,
                                maxTotalLines: 60,
                                preferredCount: 2,
                                maxPages: 12,
                            },
                        );

                    if (keywordLinesXY.length) {
                        keywordLines = blendXyAndTextLines(
                            keywordLinesXY,
                            keywordLines,
                        );
                    }
                    if (keywordLinesXY.length) {
                        source = "PDF_XY";
                    }
                } catch {}
            }

            return {
                keywords: Array.from(hits),
                keywordLines,
                source,
                pdfUrl: url,
            };
        }
    } catch {
        // ignore and fall through
    } finally {
        try {
            if (pdfPage) await pdfPage.close();
        } catch {}
    }

    // 1b) If the page uses download buttons instead of anchors, click and capture PDF bytes
    let buttonTried = false;
    try {
        const { tried, buf } = await tryDownloadAnnualReportPdfViaButtons(page);
        buttonTried = Boolean(tried);
        if (buf) {
            const parsed = await pdfParse(buf);
            const text = parsed && parsed.text ? parsed.text : "";
            const hits = await scanKeywordsInText(text, matchers);
            let keywordLines = extractMatchedLines(text, matchers, {
                maxLinesPerKey: 10,
                maxTotalLines: 60,
            });
            let source = "PDF";

            if (USE_XY_COORDS) {
                try {
                    const keywordLinesXY =
                        await extractMatchedLinesFromPdfBufferXY(
                            buf,
                            matchers,
                            {
                                maxLinesPerKey: 10,
                                maxTotalLines: 60,
                                preferredCount: 2,
                                maxPages: 12,
                            },
                        );

                    if (keywordLinesXY.length) {
                        keywordLines = blendXyAndTextLines(
                            keywordLinesXY,
                            keywordLines,
                        );
                    }
                    if (keywordLinesXY.length) {
                        source = "PDF_XY";
                    }
                } catch {}
            }

            return {
                keywords: Array.from(hits),
                keywordLines,
                source,
                pdfUrl: null,
            };
        }
    } catch {
        // ignore
    }

    // 2) Fallback: scan visible HTML text if no PDF exists / downloads fail
    try {
        const text = await page.evaluate(() => document.body?.innerText || "");
        const hits = await scanKeywordsInText(text, matchers);
        const keywordLines = extractMatchedLines(text, matchers, {
            maxLinesPerKey: 10,
            maxTotalLines: 60,
        });
        return {
            keywords: Array.from(hits),
            keywordLines,
            source: pdfUrls.length || buttonTried ? "HTML_FALLBACK" : "HTML",
            pdfUrl: null,
        };
    } catch {
        return {
            keywords: [],
            keywordLines: [],
            source: pdfUrls.length || buttonTried ? "HTML_FALLBACK" : "HTML",
            pdfUrl: null,
        };
    }
}

async function runScraper() {
    fs.ensureFileSync(config.paths.financeJsonl);

    const RESCAN_LENDER = /^1|true$/i.test(process.env.RESCAN_LENDER || "");
    const ONLY_ORG = (process.env.ORG || process.env.ORGNR || "")
        .toString()
        .replace(/-/g, "")
        .trim();
    const LIMIT = Number.parseInt(
        (process.env.LIMIT || process.env.SCRAPE_LIMIT || "0").toString(),
        10,
    );

    const HEADLESS = !/^0|false$/i.test(
        (process.env.HEADLESS || "").toString(),
    );
    const CONCURRENCY = Math.max(
        1,
        Math.min(
            6,
            Number.parseInt((process.env.CONCURRENCY || "1").toString(), 10) ||
                3,
        ),
    );
    const DELAY_MS =
        Number.parseInt((process.env.DELAY_MS || "500").toString(), 10) || 0;

    const ORG_TIMEOUT_MS =
        Number.parseInt(
            (process.env.ORG_TIMEOUT_MS || "180000").toString(),
            10,
        ) || 0;

    const MAX_ORG_RETRIES =
        Number.parseInt((process.env.ORG_RETRIES || "3").toString(), 10) || 3;

    const PROTOCOL_TIMEOUT_MS =
        Number.parseInt(
            (process.env.PROTOCOL_TIMEOUT_MS || "120000").toString(),
            10,
        ) || 120000;

    const NAV_TIMEOUT_MS =
        Number.parseInt(
            (process.env.NAV_TIMEOUT_MS || "30000").toString(),
            10,
        ) || 30000;
    const LOG_STAGES = /^1|true$/i.test(
        (process.env.LOG_STAGES || "").toString(),
    );

    const BROWSER_START_TIMEOUT_MS =
        Number.parseInt(
            (process.env.BROWSER_START_TIMEOUT_MS || "90000").toString(),
            10,
        ) || 90000;

    const BROWSER_NEW_PAGE_TIMEOUT_MS =
        Number.parseInt(
            (process.env.BROWSER_NEW_PAGE_TIMEOUT_MS || "30000").toString(),
            10,
        ) || 30000;

    const DUMPIO = /^1|true$/i.test((process.env.DUMPIO || "").toString());

    // Load input CSV (supports your column names)
    const targets = [];
    await new Promise((resolve) => {
        fs.createReadStream(config.paths.inputCsv)
            .pipe(
                csv({
                    mapHeaders: ({ header }) =>
                        (header || "")
                            .toString()
                            .replace(/^\uFEFF/, "")
                            .trim(),
                }),
            )
            .on("data", (d) => {
                const org = (d.orgnr || d.OrgNr || d.org || "")
                    .toString()
                    .replace(/-/g, "")
                    .trim();
                const name = d.name || d.Name || "";
                if (org && /^\d{10}$/.test(org)) targets.push({ org, name });
            })
            .on("end", resolve);
    });

    // Debug convenience: if ORG is provided but not in input.csv, run it anyway.
    if (ONLY_ORG && !targets.some((t) => t.org === ONLY_ORG)) {
        targets.push({ org: ONLY_ORG, name: "" });
    }

    console.log(`Loaded ${targets.length} orgs from input CSV`);

    const done = loadAlreadyScrapedSet();
    console.log(`Already scraped: ${done.size} orgs`);

    const userDataDir = (process.env.USER_DATA_DIR || "").toString().trim();

    const baseArgs = [
        "--start-maximized",
        // Windows stability: reduce GPU / background throttling flakiness.
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
    ];

    const launchOptsBase = {
        headless: HEADLESS,
        defaultViewport: null,
        args: baseArgs,
        protocolTimeout: PROTOCOL_TIMEOUT_MS,
        dumpio: DUMPIO,
        // If USER_DATA_DIR is not provided, use an isolated profile per run to
        // avoid profile corruption after forced termination.
        userDataDir:
            userDataDir ||
            path.join(
                os.tmpdir(),
                `hittascraper-profile-${Date.now()}-${process.pid}`,
            ),
    };

    const tryLaunch = async (extra) => {
        return puppeteer.launch({ ...launchOptsBase, ...(extra || {}) });
    };

    const closeBrowserHard = async (br) => {
        if (!br) return;
        try {
            await br.close();
        } catch {}
        try {
            const p = typeof br.process === "function" ? br.process() : null;
            if (p && typeof p.kill === "function") p.kill();
        } catch {}
    };

    const launchBrowserAndPage = async () => {
        const launchPromise = (async () => {
            try {
                return await tryLaunch();
            } catch (e1) {
                // Fallback: use system-installed Chrome if available.
                // You can force this with PUPPETEER_CHANNEL=chrome.
                const forcedChannel = (process.env.PUPPETEER_CHANNEL || "")
                    .toString()
                    .trim();
                if (
                    forcedChannel ||
                    /3221225477/.test((e1 && e1.message) || "")
                ) {
                    const channel = forcedChannel || "chrome";
                    return await tryLaunch({ channel });
                }
                throw e1;
            }
        })();

        let browser = null;
        try {
            browser = await withTimeout(
                launchPromise,
                BROWSER_START_TIMEOUT_MS,
                "puppeteer.launch",
            );
        } catch (err) {
            // If launch eventually succeeds after our timeout, make sure we close it.
            launchPromise.then((br) => closeBrowserHard(br)).catch(() => {});
            throw err;
        }

        _activeBrowser = browser;

        const pagePromise = browser.newPage();
        let page = null;
        try {
            page = await withTimeout(
                pagePromise,
                BROWSER_NEW_PAGE_TIMEOUT_MS,
                "browser.newPage",
            );
        } catch (err) {
            pagePromise.then(() => {}).catch(() => {});
            await closeBrowserHard(browser);
            _activeBrowser = null;
            throw err;
        }

        // Set sensible per-page timeouts so slow targets don't hang forever.
        try {
            page.setDefaultTimeout(ORG_TIMEOUT_MS || 0);
            page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS || 30000);
        } catch {}

        try {
            await withTimeout(setupFastPage(page), 15000, "setupFastPage");
        } catch {
            // If request interception setup is flaky, continue without it.
        }

        return { browser, page };
    };

    let browser = null;
    let page = null;
    ({ browser, page } = await launchBrowserAndPage());

    // Run single-threaded to avoid concurrency issues
    console.log(
        `Settings: HEADLESS=${HEADLESS ? 1 : 0} RUN_MODE=single CONCURRENCY=${CONCURRENCY} DELAY_MS=${DELAY_MS} RESCAN_LENDER=${RESCAN_LENDER ? 1 : 0} ORG_TIMEOUT_MS=${ORG_TIMEOUT_MS}`,
    );

    // page is created in launchBrowserAndPage()

    let processed = 0;
    let logIdx = 0;

    const scrapeOne = async (page, org, name, workerId) => {
        const alreadyDone = done.has(org);
        const rescanOnly = alreadyDone && RESCAN_LENDER;
        if (alreadyDone && !RESCAN_LENDER) return { skipped: true };

        logIdx++;
        const prefix = `[${logIdx}]`;
        console.log(
            `${prefix} Scraping ${org} ${name}${rescanOnly ? " (rescan lender)" : ""}`,
        );

        let contact = null;
        let lender = { keywords: [], source: "NONE", pdfUrl: null };

        for (let attempt = 1; attempt <= 2; attempt++) {
            try {
                if (LOG_STAGES) console.log(`${prefix}   -> base page`);
                await page.goto(
                    `https://www.hitta.se/företagsinformation/${org}`,
                    {
                        waitUntil: "domcontentloaded",
                        timeout: NAV_TIMEOUT_MS,
                    },
                );
                await sleep(600);
                await tryAcceptCookies(page);
                contact = await scrapeContactDetails(page);

                if (LOG_STAGES) console.log(`${prefix}   -> reports page`);
                await page.goto(
                    `https://www.hitta.se/företagsinformation/${org}#reports`,
                    {
                        waitUntil: "domcontentloaded",
                        timeout: NAV_TIMEOUT_MS,
                    },
                );
                await sleep(500);
                await tryAcceptCookies(page);

                if (LOG_STAGES) console.log(`${prefix}   -> pdf scan`);
                lender = await scrapeLenderKeywordsPdfFirst(browser, page);

                break;
            } catch (err) {
                console.log(`  -> Attempt ${attempt} failed: ${err.message}`);
                await sleep(900);
            }
        }

        // Post-process matched lines: keep only relevant loan/financing lines and
        // extract numeric tokens into a `values` array for each matched line.
        const lenderKeywords = Array.isArray(lender?.keywords)
            ? lender.keywords
            : [];

        const rawKeywordLines = Array.isArray(lender?.keywordLines)
            ? lender.keywordLines
            : [];

        const normalizeMatched = (item) => {
            try {
                const rawLine = (item && item.line ? item.line : "").toString();
                const lineHasDigits = /\d/.test(rawLine);

                let values = [];
                if (lineHasDigits) {
                    values = extractNumbersFromLine(rawLine, {
                        preferredCount: 2,
                    });
                } else if (Array.isArray(item?.values)) {
                    const existing = item.values
                        .map((v) =>
                            (v || "").toString().replace(/\s+/g, " ").trim(),
                        )
                        .filter((v) => v.length > 0);

                    const looksCleanPair =
                        existing.length === 2 &&
                        existing.every((v) => /^[-+]?\d+(?: \d{3})*$/.test(v));

                    if (looksCleanPair) {
                        values = existing;
                    } else {
                        const repaired = extractNumbersFromLine(
                            existing.join(" "),
                            {
                                preferredCount: 2,
                            },
                        );
                        values = repaired.length === 2 ? repaired : existing;
                    }
                }
                const line = stripNumbersFromLine(rawLine);

                // Cleanup for Kreditinstitut debt rows: strip note/footnote-only digits
                // and keep the last two values (typically 2 year columns).
                const keyLower = (item?.key || item?.keyword || "")
                    .toString()
                    .toLowerCase();
                const looksLikeSkuldLine = /skuld/i.test(rawLine);
                if (
                    keyLower === "kreditinstitut" &&
                    looksLikeSkuldLine &&
                    Array.isArray(values) &&
                    values.length > 2
                ) {
                    const filtered = values.filter((v) => {
                        const s = (v || "")
                            .toString()
                            .replace(/\s+/g, " ")
                            .trim();
                        if (!s) return false;
                        if (s === "0" || s === "+0" || s === "-0") return true;
                        const digitsOnly = s.replace(/[^0-9]/g, "");
                        // Drop tiny tokens like "3" / "4" that are almost always note refs.
                        return digitsOnly.length > 2;
                    });
                    if (filtered.length >= 2) {
                        values = filtered.slice(-2);
                    }
                }

                return {
                    ...item,
                    line: line || rawLine,
                    values,
                };
            } catch {
                return { ...item, values: [] };
            }
        };

        const lenderKeywordLines = rawKeywordLines.map(normalizeMatched);

        // Kreditinstitut debts (skulder till kreditinstitut) are the primary
        // numeric signal we care about.
        const kreditinstitutSkulderLines = lenderKeywordLines.filter((it) => {
            try {
                const key = (
                    it && (it.key || it.keyword) ? it.key || it.keyword : ""
                )
                    .toString()
                    .toLowerCase();
                if (key !== "kreditinstitut") return false;
                const line = (it && it.line ? it.line : "").toString();
                return /skuld/i.test(line);
            } catch {
                return false;
            }
        });

        const dedupeKreditinstitutValueVariants = (arr) => {
            const lines = Array.isArray(arr) ? arr : [];
            if (lines.length < 2) return lines;

            const keyOf = (it) => {
                const v = Array.isArray(it?.values) ? it.values : [];
                return `${it?.key || it?.keyword || ""}|${(it?.line || "").toString().trim()}|${v.join("|")}`;
            };

            const hasCleanVariant = (badFirst, second, all) => {
                // badFirst like "44 990 429" => clean "4 990 429"
                const m = (badFirst || "").match(/^(\d)\1 (\d{3} \d{3})$/);
                if (!m) return null;
                const cleanFirst = `${m[1]} ${m[2]}`;
                return all.some((it) => {
                    const v = Array.isArray(it?.values) ? it.values : [];
                    return (
                        v.length === 2 && v[0] === cleanFirst && v[1] === second
                    );
                });
            };

            const out = [];
            const seen = new Set();
            for (const it of lines) {
                const v = Array.isArray(it?.values) ? it.values : [];
                if (v.length === 2 && hasCleanVariant(v[0], v[1], lines)) {
                    // Skip the "duplicated-leading-digit" variant when a clean one exists.
                    if (/^(\d)\1 \d{3} \d{3}$/.test(v[0] || "")) {
                        continue;
                    }
                }

                const k = keyOf(it);
                if (seen.has(k)) continue;
                seen.add(k);
                out.push(it);
            }
            return out;
        };

        const cleanedKreditinstitutSkulderLines =
            dedupeKreditinstitutValueVariants(kreditinstitutSkulderLines);

        const hasAnyData =
            Boolean(contact?.email) ||
            Boolean(contact?.phone) ||
            (Array.isArray(lenderKeywords) && lenderKeywords.length) ||
            (Array.isArray(lenderKeywordLines) && lenderKeywordLines.length);

        if (!hasAnyData && (lender?.source || "NONE") === "NONE") {
            console.log(`${prefix}   -> No data (scan failed) — not saved`);
            return { skipped: false };
        }

        fs.appendFileSync(
            config.paths.financeJsonl,
            JSON.stringify({
                org,
                name,
                email: contact?.email || null,
                phone: contact?.phone || null,
                lenderKeywords: lenderKeywords,
                lenderKeywordLines: lenderKeywordLines,
                kreditinstitutSkulderLines: cleanedKreditinstitutSkulderLines,
                lenderKeywordsSource: lender?.source || null,
                tables: [],
            }) + "\n",
        );

        console.log(`${prefix}   -> Saved keyword scan`);

        return { skipped: false };
    };

    for (let i = 0; i < targets.length; i++) {
        const t = targets[i];
        if (!t) continue;
        // If an external signal requested abort, stop before starting next org
        if (_abortRequested) {
            console.log("Abort requested — stopping before next org.");
            break;
        }
        const { org, name } = t;
        if (ONLY_ORG && org !== ONLY_ORG) continue;

        let attemptTarget = 0;
        let ok = false;
        let wasSkipped = false;
        while (attemptTarget < MAX_ORG_RETRIES && !ok) {
            attemptTarget++;
            try {
                const res = await withTimeout(
                    scrapeOne(page, org, name, 1),
                    ORG_TIMEOUT_MS,
                    `org ${org}`,
                );
                if (res && res.skipped) {
                    ok = true;
                    wasSkipped = true;
                    break;
                }
                ok = true;
                break;
            } catch (err) {
                console.log(
                    `[single]   -> Failed ${org} (attempt ${attemptTarget}): ${err && err.message ? err.message : err}`,
                );

                // Try to recover by closing/ restarting page and browser, then retry.
                try {
                    await page.close();
                } catch {}
                try {
                    await browser.close();
                } catch {}
                _activeBrowser = null;

                try {
                    ({ browser, page } = await launchBrowserAndPage());
                } catch (e2) {
                    console.log(
                        `[single]   -> Failed to restart browser: ${e2 && e2.message ? e2.message : e2}`,
                    );
                }

                if (attemptTarget >= MAX_ORG_RETRIES) {
                    console.log(
                        `[single]   -> Giving up on ${org} after ${attemptTarget} attempts`,
                    );
                    break;
                }

                // Back off a bit before retrying
                await sleep(1000 * attemptTarget);
            }
        }
        if (!ok) continue;

        // IMPORTANT: when we skip an already-scraped org we should not apply
        // per-org delay. Otherwise startup can look "stuck" for minutes while
        // silently skipping a block of pre-scraped orgs.
        if (wasSkipped) {
            continue;
        }

        processed++;
        if (LIMIT && processed >= LIMIT) {
            console.log(`Reached LIMIT=${LIMIT}, stopping early.`);
            break;
        }

        const d = jitterDelay(DELAY_MS);
        if (d) await sleep(d);
    }

    await browser.close();
    _activeBrowser = null;
    console.log("Scraping finished.");
}

async function shutdownScraper() {
    // Idempotent shutdown: mark abort and close browser if present
    _abortRequested = true;
    if (!_activeBrowser) return;
    try {
        console.log("Shutting down browser...");
        await _activeBrowser.close();
    } catch (e) {
        // ignore
    } finally {
        _activeBrowser = null;
    }
}

module.exports = runScraper;
module.exports.shutdown = shutdownScraper;
module.exports.requestAbort = requestAbort;
