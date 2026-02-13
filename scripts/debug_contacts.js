// scripts/debug_contacts.js
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");

puppeteer.use(Stealth());

const ORG = (process.env.ORG || process.env.ORGNR || "")
    .toString()
    .replace(/-/g, "")
    .trim();
if (!ORG) {
    console.error("Set ORG=5560008301 (or another orgnr)");
    process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function tryAcceptCookies(page) {
    const selectors = ["#modalConfirmBtn", "#onetrust-accept-btn-handler"];
    for (const sel of selectors) {
        try {
            const btn = await page.$(sel);
            if (btn) {
                await btn.click();
                await sleep(600);
            }
        } catch {}
    }
}

async function dump(page, label) {
    const info = await page.evaluate(() => {
        const uniq = (arr) => Array.from(new Set(arr));
        const hasCensus = document.querySelectorAll(
            "[data-census-details]",
        ).length;
        const mailto = document.querySelectorAll('a[href^="mailto:"]').length;
        const tel = document.querySelectorAll('a[href^="tel:"]').length;
        const showBtn = document.querySelectorAll(
            '[data-test="show-numbers-button"]',
        ).length;

        const dataTests = uniq(
            Array.from(document.querySelectorAll("[data-test]"))
                .map((el) => (el.getAttribute("data-test") || "").trim())
                .filter(Boolean),
        );

        const interesting = dataTests.filter((v) =>
            /mail|email|phone|number|contact|kontakt|census|show/i.test(v),
        );

        const bodyText = (document.body?.innerText || "").slice(0, 2000);

        return {
            url: window.location.href,
            hasCensus,
            mailto,
            tel,
            showBtn,
            interestingDataTests: interesting.slice(0, 50),
            bodyTextSample: bodyText,
        };
    });

    console.log("\n==", label, "==");
    console.log(JSON.stringify(info, null, 2));
}

(async () => {
    const browser = await puppeteer.launch({
        headless: false,
        defaultViewport: null,
        args: ["--start-maximized"],
    });

    const page = await browser.newPage();

    const base = `https://www.hitta.se/f\u00f6retagsinformation/${ORG}`;

    await page.goto(base, { waitUntil: "domcontentloaded", timeout: 30000 });
    await sleep(1200);
    await tryAcceptCookies(page);
    await sleep(800);
    await dump(page, "BASE");

    await page.goto(base + "#reports", {
        waitUntil: "domcontentloaded",
        timeout: 30000,
    });
    await sleep(1200);
    await tryAcceptCookies(page);
    await sleep(800);
    await dump(page, "REPORTS");

    console.log("Keeping browser open for 20s...");
    await sleep(20000);
    await browser.close();
})();
