// Checks for lib/adaptive-pipelines.js: simulated AEM servers, one window per step.
const { createController, RETRY_WINDOWS } = require('../lib/adaptive-pipelines');

const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };

/** Runs windows against a server whose files/s for n pipelines is serverRate(n); returns the targets seen. */
function simulate(serverRate, windows, opts = {}) {
    const c = createController({ start: 8, max: 24, ...opts });
    const seen = [c.target];
    for (let w = 0; w < windows; w++) {
        const ctx = typeof opts.window === 'function' ? opts.window(w, c.target) : {};
        c.next({ rate: serverRate(c.target), errors: 0, canGrow: true, ...ctx });
        seen.push(c.target);
    }
    return seen;
}

// A server that answers in the same time however many files come at once: grows to the max and stays
let seen = simulate((n) => n * 0.8, 8);
record('fast server: grows 8, 12, 16, 20, 24 and stays at the max', seen.join(',') === '8,12,16,20,24,24,24,24,24', seen.join(','));

// A server that tops out at 10 files/s: tries 12 (faster), 16 (no faster) and goes back to 12
seen = simulate((n) => Math.min(n, 10), 6);
record('busy server: stops where more pipelines no longer help', seen.join(',') === '8,12,16,12,12,12,12', seen.join(','));

// ... and tries again after a while, in case AEM has less to do by then
seen = simulate((n) => Math.min(n, 10), 4 + RETRY_WINDOWS + 2);
const retried = seen.slice(4).includes(16);
record(`after a plateau it tries again (every ${RETRY_WINDOWS} windows) and comes back`, retried && seen[seen.length - 1] === 12, seen.join(','));

// Failed files: halve at once, then wait before growing again
let c = createController({ start: 8, max: 24 });
c.next({ rate: 8 }); c.next({ rate: 12 });
const before = c.target;
c.next({ rate: 3, errors: 5 });
const afterErr = c.target;
const held = [1, 2, 3].map(() => { c.next({ rate: 8 }); return c.target; });
c.next({ rate: 8 });
record('failed files: halves at once and holds before growing again', before === 16 && afterErr === 8 && held.every((t) => t === 8) && c.target === 12,
    `before=${before} after errors=${afterErr} held=${held.join(',')} then=${c.target}`);

// After failures at 16 it stays below 16 for a while instead of climbing back into them
c = createController({ start: 8, max: 24 });
c.next({ rate: 8 }); c.next({ rate: 12 });
c.next({ rate: 3, errors: 2 });
const after = [];
for (let i = 0; i < RETRY_WINDOWS - 1; i++) { c.next({ rate: c.target }); after.push(c.target); }
record('after failures at 16: grows back only to 15 until the retry time', Math.max(...after) === 15, after.join(','));

// Another upload to the same AEM server: back to the start setting, no growth
c = createController({ start: 8, max: 24 });
c.next({ rate: 8 }); c.next({ rate: 12 });
const grown = c.target;
c.next({ rate: 16, canGrow: false });
const shared = c.target;
c.next({ rate: 8, canGrow: false });
record('second upload to the same server: back to 8 and stays', grown === 16 && shared === 8 && c.target === 8, `grown=${grown} shared=${shared} then=${c.target}`);

// Never below 1, never above the max; a start above the max is capped
c = createController({ start: 1, max: 24 });
for (let i = 0; i < 4; i++) c.next({ rate: 1, errors: 1 });
const low = c.target;
const capped = createController({ start: 40, max: 24 }).target;
record('limits: never below 1, start capped at the max', low === 1 && capped === 24, `low=${low} capped=${capped}`);

console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
process.exit(results.every(Boolean) ? 0 : 1);
