const scraper = require("./scraper");

let shuttingDown = false;
async function shutdown(code) {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
        if (typeof scraper.requestAbort === "function") scraper.requestAbort();
    } catch {}
    try {
        if (typeof scraper.shutdown === "function") await scraper.shutdown();
    } catch {}
    process.exitCode = code;
}

process.on("SIGINT", () => {
    // Ctrl+C: request abort, close browser, exit with 130.
    shutdown(130);
});

process.on("SIGTERM", () => {
    shutdown(143);
});

process.on("unhandledRejection", (err) => {
    console.error("Unhandled rejection:", err);
    shutdown(1);
});

process.on("uncaughtException", (err) => {
    console.error("Uncaught exception:", err);
    shutdown(1);
});

(async () => {
    try {
        await scraper();
        process.exitCode = 0;
    } catch (err) {
        console.error(err);
        await shutdown(1);
    }
})();
