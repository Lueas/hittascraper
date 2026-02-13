const processAll = require("./processor");

(async () => {
    try {
        await processAll();
        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
})();
