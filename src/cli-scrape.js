const scraper = require("./scraper");

let terminating = false;
let restartRequested = false;
let lastFatal = null;

async function cleanupBrowserOnly() {
    try {
        if (typeof scraper.requestAbort === "function") scraper.requestAbort();
    } catch {}
    try {
        if (typeof scraper.shutdown === "function") await scraper.shutdown();
    } catch {}
}

async function shutdown(code) {
    if (terminating) return;
    terminating = true;
    try {
        await cleanupBrowserOnly();
    } catch {}
    process.exitCode = code;
}

async function requestRestart(err) {
    if (terminating) return;
    restartRequested = true;
    lastFatal = err || new Error("Unknown fatal error");
    try {
        await cleanupBrowserOnly();
    } catch {}
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
    requestRestart(err);
});

process.on("uncaughtException", (err) => {
    console.error("Uncaught exception:", err);
    requestRestart(err);
});

(async () => {
    const EXIT_ON_ERROR = /^1|true$/i.test(
        (process.env.EXIT_ON_ERROR || "").toString(),
    );

    let backoffMs = 2000;
    const backoffMaxMs = 60000;

    while (!terminating) {
        restartRequested = false;
        lastFatal = null;

        try {
            await scraper();
            process.exitCode = 0;
            return;
        } catch (err) {
            console.error("Scraper crashed:", err);
            await requestRestart(err);
        }

        if (terminating) return;
        if (EXIT_ON_ERROR) {
            await shutdown(1);
            return;
        }

        if (!restartRequested) {
            // Defensive: if we get here without a restart request, exit.
            await shutdown(1);
            return;
        }

        const msg =
            lastFatal && lastFatal.message
                ? lastFatal.message
                : String(lastFatal);
        console.error(
            `Restarting after fatal error in ${Math.round(backoffMs / 1000)}s: ${msg}`,
        );
        await new Promise((r) => setTimeout(r, backoffMs));
        backoffMs = Math.min(backoffMaxMs, Math.floor(backoffMs * 1.6));
    }
})();
