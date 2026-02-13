const scraper = require("./src/scraper");
const processAll = require("./src/processor");
const buildWideReport = require("./src/wide_report");

const args = process.argv.slice(2);

(async () => {
    if (!args.includes("--offline")) {
        console.log("Running scraper...");
        await scraper();
    } else {
        console.log("Offline mode.");
    }

    console.log("Building atomic dataset...");
    await processAll();

    console.log("Building wide 1-year report...");
    await buildWideReport();
})();

// Graceful shutdown on Ctrl+C / SIGTERM
let _shuttingDown = false;
const doShutdown = async (signal) => {
    if (_shuttingDown) {
        console.log(`Second ${signal} received — forcing exit.`);
        process.exit(signal === "SIGINT" ? 130 : 0);
    }
    _shuttingDown = true;
    console.log(`Received ${signal}, requesting graceful shutdown...`);
    try {
        if (scraper.requestAbort) scraper.requestAbort();

        // Ask scraper to shutdown and wait briefly for it to finish.
        if (scraper.shutdown) {
            const p = scraper.shutdown();
            await Promise.race([p, new Promise((r) => setTimeout(r, 5000))]);
        }
    } catch (e) {}
    process.exit(signal === "SIGINT" ? 130 : 0);
};

process.on("SIGINT", () => doShutdown("SIGINT"));
process.on("SIGTERM", () => doShutdown("SIGTERM"));
