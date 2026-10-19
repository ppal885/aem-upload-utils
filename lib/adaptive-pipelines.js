/**
 * How many upload pipelines to run. Each pipeline confirms its files with AEM one at a time, so for many
 * small files the speed is roughly pipelines / AEM's answer time. Measured every window:
 * - more pipelines made it faster: add STEP more (up to max), while growing is allowed;
 * - more pipelines did not make it faster: AEM is the limit, go back and stay there;
 * - files failed in the window: halve (never below min) and do not grow for a while;
 * - another upload to the same AEM server started: back to the start setting, so they share it fairly.
 * After a plateau it tries again every RETRY_WINDOWS windows (AEM may have less to do by then).
 */
const STEP = 4;
const BETTER = 1.1;
const HOLD_AFTER_ERRORS = 3;
const RETRY_WINDOWS = 10;

function createController({ start, min = 1, max = 24 }) {
    const base = Math.max(min, Math.min(max, start));
    let target = base;
    let lastRate = null;
    let grewFrom = null;
    let plateau = false;
    let hold = 0;
    let sincePlateau = 0;

    /** One window: rate in files/s, errors (failed files), canGrow (no other upload to the same AEM server). Returns { target, reason }. */
    function next({ rate, errors = 0, canGrow = true }) {
        const was = target;
        let reason = 'steady';
        if (errors > 0) {
            target = Math.max(min, Math.floor(target / 2));
            hold = HOLD_AFTER_ERRORS;
            grewFrom = null;
            reason = `${errors} failed file(s)`;
        } else if (!canGrow && target > base) {
            target = base;
            grewFrom = null;
            reason = 'another upload to the same server';
        } else if (grewFrom != null && lastRate != null && rate < lastRate * BETTER) {
            target = grewFrom;
            plateau = true;
            sincePlateau = 0;
            reason = `no gain from ${was} (${rate.toFixed(1)} vs ${lastRate.toFixed(1)} files/s)`;
            grewFrom = null;
        } else if (plateau && ++sincePlateau >= RETRY_WINDOWS) {
            plateau = false;
            grewFrom = null;
        } else if (hold > 0) {
            hold--;
            grewFrom = null;
        } else if (!plateau && canGrow && target < max) {
            grewFrom = target;
            target = Math.min(max, target + STEP);
            reason = `room to grow (${rate.toFixed(1)} files/s)`;
        } else {
            grewFrom = null;
        }
        lastRate = rate;
        return { target, changed: target !== was, reason };
    }

    return { next, get target() { return target; } };
}

module.exports = { createController, STEP, RETRY_WINDOWS };
