const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
const config = require("../src/config");
const { extractMatchedLinesFromPdfBufferXY } = require("../src/pdfXYExtract");
const fs = require("fs");
const os = require("os");
const path = require("path");

puppeteer.use(Stealth());

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
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
    return [
        { key: "Kortfristiga", re: /\bkortfristiga\b|\bkortfristig\b/i },
        {
            key: "Långfristiga",
            re: /\blångfristiga\b|\blångfristig\b|\blangfristiga\b|\blangfristig\b/i,
        },
        { key: "Kreditinstitut", re: /\bkreditinstitut\b/i },
    ];
}

async function tryAcceptCookies(page) {
    try {
        const btn = await page.$("#modalConfirmBtn");
        if (btn) {
            await btn.click();
            await sleep(500);
        }
    } catch {}
    try {
        const btn = await page.$("#onetrust-accept-btn-handler");
        if (btn) {
            await btn.click();
            await sleep(500);
        }
    } catch {}
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
            if (/\.pdf(\?|#|$)/i.test(c.href)) s += 8;
            return s;
        };

        const sorted = candidates
            .map((c) => ({ ...c, score: score(c) }))
            .filter((c) => c.score > 0)
            .sort((a, b) => b.score - a.score);

        const out = [];
        const seen = new Set();
        for (const c of sorted) {
            if (!seen.has(c.href)) {
                seen.add(c.href);
                out.push(c.href);
            }
            if (out.length >= 5) break;
        }
        return out;
    });
}

async function fetchPdfBuffer(browser, url) {
    const p = await browser.newPage();
    try {
        const resp = await p.goto(url, {
            waitUntil: "domcontentloaded",
            timeout: 45000,
        });
        if (!resp || !resp.ok()) return null;
        const buf = await resp.buffer();
        const head = buf.slice(0, 8).toString("latin1");
        return head.startsWith("%PDF-") ? buf : null;
    } catch {
        return null;
    } finally {
        try {
            await p.close();
        } catch {}
    }
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
            } catch {}
        };

        page.on("response", onResponse);
        setTimeout(() => done(null), timeoutMs);
    });
}

async function tryDownloadAnnualReportPdfViaButtons(page) {
    const selector =
        'button[data-test="download-report-button"],button[data-test^="download-report-button"]';
    const buttons = await page.$$(selector);
    if (!buttons.length) return null;

    const maxTries = Math.min(4, buttons.length);
    for (let i = 0; i < maxTries; i++) {
        const pdfBufPromise = waitForPdfResponseBuffer(page, 20000);

        // Create a temporary download dir and set the Page download behavior
        // so that click-initiated downloads go to a known location we control.
        const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "hitta_dl_"));
        let cdp = null;
        try {
            cdp = await page.target().createCDPSession();
            await cdp.send("Page.setDownloadBehavior", {
                behavior: "allow",
                downloadPath: tmpRoot,
            });
        } catch {
            cdp = null;
        }
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
                        return false;
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
            return buf;
        }

        // If no network-captured buffer, check the tmp download dir for files
        try {
            const deadline = Date.now() + 6000;
            while (Date.now() < deadline) {
                const files = fs.readdirSync(tmpRoot || "") || [];
                if (files && files.length) {
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
                                try {
                                    fs.unlinkSync(fp);
                                } catch {}
                                try {
                                    fs.rmSync(tmpRoot, {
                                        recursive: true,
                                        force: true,
                                    });
                                } catch {}
                                return fileBuf;
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

        try {
            fs.rmSync(tmpRoot, { recursive: true, force: true });
        } catch {}
    }
    return null;
}

async function run() {
    const orgs = process.argv.slice(2);
    const testOrgs = orgs.length
        ? orgs
        : ["5560254731", "5560310590", "5560397530", "5560069840"];

    const keywordMatchers = buildKeywordMatchers(config.pdfLenderKeywords);
    const loanMatchers = buildLoanMatchers();

    const browser = await puppeteer.launch({
        headless: true,
        defaultViewport: { width: 1400, height: 900 },
    });

    try {
        for (const org of testOrgs) {
            const page = await browser.newPage();
            let urls = [];
            try {
                await page.goto(
                    `https://www.hitta.se/företagsinformation/${org}#reports`,
                    {
                        waitUntil: "domcontentloaded",
                        timeout: 45000,
                    },
                );
                await sleep(700);
                await tryAcceptCookies(page);
                await sleep(600);
                urls = await findAnnualReportPdfUrls(page);
            } catch {
                urls = [];
            } finally {
                try {
                    await page.close();
                } catch {}
            }

            let buf = null;
            let pickedUrl = null;
            for (const u of urls) {
                buf = await fetchPdfBuffer(browser, u);
                if (buf) {
                    pickedUrl = u;
                    break;
                }
            }

            if (!buf) {
                const p2 = await browser.newPage();
                try {
                    await p2.goto(
                        `https://www.hitta.se/företagsinformation/${org}#reports`,
                        {
                            waitUntil: "domcontentloaded",
                            timeout: 45000,
                        },
                    );
                    await sleep(700);
                    await tryAcceptCookies(p2);
                    await sleep(500);
                    buf = await tryDownloadAnnualReportPdfViaButtons(p2);
                    if (buf) pickedUrl = "<download-button>";
                } catch {}
                try {
                    await p2.close();
                } catch {}
            }

            if (!buf) {
                console.log(`ORG ${org}: no PDF buffer found`);
                continue;
            }

            let k = [];
            let l = [];
            let err = null;
            try {
                k = await extractMatchedLinesFromPdfBufferXY(
                    buf,
                    keywordMatchers,
                    {
                        maxLinesPerKey: 10,
                        maxTotalLines: 60,
                        preferredCount: 2,
                    },
                );
                l = await extractMatchedLinesFromPdfBufferXY(
                    buf,
                    loanMatchers,
                    {
                        maxLinesPerKey: 10,
                        maxTotalLines: 60,
                        preferredCount: 2,
                    },
                );
            } catch (e) {
                err = e && e.message ? e.message : String(e);
            }

            console.log(
                `ORG ${org}: pdf=${pickedUrl ? "yes" : "no"} k=${k.length} l=${l.length}${err ? ` err=${err}` : ""}`,
            );
            if (k[0]) console.log(`  first keyword: ${JSON.stringify(k[0])}`);
            if (l[0]) console.log(`  first loan: ${JSON.stringify(l[0])}`);
        }
    } finally {
        await browser.close();
    }
}

run().catch((e) => {
    console.error(e);
    process.exit(1);
});
