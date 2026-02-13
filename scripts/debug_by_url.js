// scripts/debug_by_url.js
const puppeteer = require("puppeteer-extra");
const Stealth = require("puppeteer-extra-plugin-stealth");
puppeteer.use(Stealth());

const URL = process.argv[2];
if (!URL) {
    console.error("Usage: node scripts/debug_by_url.js <url>");
    process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function tryAcceptCookies(page) {
    try {
        const b = await page.$("#modalConfirmBtn");
        if (b) {
            await b.click();
            await sleep(600);
        }
    } catch {}
    try {
        const b = await page.$("#onetrust-accept-btn-handler");
        if (b) {
            await b.click();
            await sleep(600);
        }
    } catch {}
}

(async () => {
    const browser = await puppeteer.launch({
        headless: false,
        defaultViewport: null,
        args: ["--start-maximized"],
    });
    const page = await browser.newPage();
    try {
        await page.goto(URL, { waitUntil: "domcontentloaded", timeout: 30000 });
        await sleep(1200);
        await tryAcceptCookies(page);
        await sleep(800);

        const info = await page.evaluate(() => {
            const census = Array.from(
                document.querySelectorAll("[data-census-details]"),
            ).map((e) => e.getAttribute("data-census-details"));
            const mailto = Array.from(
                document.querySelectorAll('a[href^="mailto:"]'),
            ).map((a) => a.getAttribute("href"));
            const tel = Array.from(
                document.querySelectorAll('a[href^="tel:"]'),
            ).map((a) => a.getAttribute("href"));
            const showBtnCount = document.querySelectorAll(
                '[data-test="show-numbers-button"]',
            ).length;
            const companyEmailBtn = document.querySelectorAll(
                '[data-test="company-email-button"]',
            ).length;
            const dataTests = Array.from(
                document.querySelectorAll("[data-test]"),
            ).map((e) => e.getAttribute("data-test"));
            const textSample = (document.body?.innerText || "").slice(0, 2000);
            return {
                census,
                mailto,
                tel,
                showBtnCount,
                companyEmailBtn,
                dataTests: dataTests.slice(0, 50),
                textSample,
            };
        });

        console.log(JSON.stringify(info, null, 2));
    } catch (err) {
        console.error("Error:", err.message);
    }
    // keep browser open briefly so you can inspect if needed
    await sleep(6000);
    await browser.close();
    process.exit(0);
})();
