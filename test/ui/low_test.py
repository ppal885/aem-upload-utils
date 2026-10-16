"""Browser checks for the Low UI fixes #25-#30. usage: python test/ui/low_test.py <app url>"""
import asyncio
import sys

from playwright.async_api import async_playwright

APP = sys.argv[1]
results = []


def record(name, ok, detail):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
        page = await ctx.new_page()
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", 'http://127.0.0.1:4620')

        # #25
        escaped = await page.evaluate("""esc('a"b\\'c<d>')""")
        record('#25 esc() escapes quotes', escaped == 'a&quot;b&#39;c&lt;d&gt;', f"esc -> {escaped!r}")

        # #26 corrupted column order, and an OS file dropped on a header
        await page.evaluate("localStorage.setItem('histColOrder', '{not json')")
        await page.reload()
        await page.wait_for_load_state('networkidle')
        alive = await page.evaluate("typeof initiateUpload === 'function' && Array.isArray(histColOrder) && histColOrder.length === DEFAULT_COL_ORDER.length")
        before = len(errors)
        await page.evaluate("""() => {
            const th = document.querySelector('#historyHead th[data-col="target"]');
            const dt = new DataTransfer(); dt.items.add(new File(['x'], 'x.txt'));
            th.dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
        }""")
        await asyncio.sleep(0.3)
        record('#26 bad saved column order / file dropped on header', alive and len(errors) == before,
               f"page script alive={alive}, new page errors={errors[before:]}")

        # #27
        fmts = await page.evaluate("[fmtDuration(400), fmtDuration(45000), fmtDuration(7530000)]")
        timer_uses_fmt = await page.evaluate("startElapsedTimer.toString().includes('fmtDuration')") if await page.evaluate("typeof startElapsedTimer === 'function'") else None
        record('#27 one duration format', fmts == ['<1s', '45s', '2h 5m'], f"{fmts}, live timer uses fmtDuration={timer_uses_fmt}")

        # #28 non-OK health counts as lost; recovery toast; text available on phones
        async def fail_health(route):
            await route.fulfill(status=503, body='{}')
        await page.route('**/api/health', fail_health)
        await page.evaluate('checkConnection()')
        await asyncio.sleep(0.5)
        lost = await page.evaluate('connHealthy')
        await page.unroute('**/api/health')
        await page.evaluate('checkConnection()')
        await asyncio.sleep(0.8)
        toast_texts = await page.evaluate("[...document.querySelectorAll('#toastContainer > *')].map(e => e.textContent.trim())")
        await page.set_viewport_size({'width': 375, 'height': 800})
        conn = await page.evaluate("""() => { const t = document.getElementById('connText'); const r = t.getBoundingClientRect();
            return { text: t.textContent, readable: t.textContent.length > 0 && getComputedStyle(t).display !== 'none', visuallyHidden: r.width <= 1,
                     dotTitle: document.getElementById('connDot').title }; }""")
        await page.set_viewport_size({'width': 1280, 'height': 900})
        record('#28 connection status', lost is False and any('restored' in t for t in toast_texts) and conn['readable'] and conn['dotTitle'] == 'Connected',
               f"after 503 healthy={lost}, recovery toast={any('restored' in t for t in toast_texts)}, phone={conn}")

        # #29 queue removed and its stored passwords purged
        await page.evaluate("localStorage.setItem('uploadQueue', JSON.stringify([{config: {password: 'secret'}}]))")
        await page.reload()
        await page.wait_for_load_state('networkidle')
        stored = await page.evaluate("localStorage.getItem('uploadQueue')")
        section = await page.locator('#uploadQueueSection').count()
        record('#29 queue removed, stored passwords cleared', stored is None and section == 0, f"stored={stored!r}, queue section elements={section}")

        # #30 dismissing hides only that upload
        await page.evaluate("showActiveBanner({uploadId: 'up-a', target: 'T', completedFiles: 1, totalFiles: 2, expectedFiles: 10, elapsedMs: 1000})")
        await page.evaluate('dismissBanner()')
        await page.evaluate("showActiveBanner({uploadId: 'up-a', target: 'T', completedFiles: 2, totalFiles: 3, expectedFiles: 10, elapsedMs: 2000})")
        same_hidden = await page.evaluate("document.getElementById('activeUploadsBanner').classList.contains('hidden')")
        await page.evaluate("showActiveBanner({uploadId: 'up-b', target: 'T', completedFiles: 1, totalFiles: 1, expectedFiles: 5, elapsedMs: 500})")
        other_shown = not await page.evaluate("document.getElementById('activeUploadsBanner').classList.contains('hidden')")
        record('#30 dismissed banner returns for a new upload', same_hidden and other_shown, f"same upload hidden={same_hidden}, new upload shown={other_shown}")

        record('no JavaScript errors on the page', not errors, f"{errors[:3]}")
        await browser.close()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


asyncio.run(main())
