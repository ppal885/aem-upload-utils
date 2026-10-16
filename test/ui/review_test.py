"""Browser checks for the fresh-review fixes. usage: python test/ui/review_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import json
import os
import sys
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1:4]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR))]
GOOD = [f for f in FILES if os.path.basename(f).startswith('t')]
results = []
MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"


def record(name, ok, detail):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


_COOKIE = None


def api(path):
    global _COOKIE
    if _COOKIE is None:
        aem = AEM
        req = urllib.request.Request(APP + '/api/auth/login', data=json.dumps({'email': 'admin@test.local', 'password': 'admin-pass-123'}).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req) as r:
            _COOKIE = r.headers.get('Set-Cookie').split(';')[0]
    with urllib.request.urlopen(urllib.request.Request(APP + path, headers={'Cookie': _COOKIE})) as r:
        return json.loads(r.read())


async def fill(page, dam):
    await page.fill('#damPath', dam)


async def start(page):
    await page.evaluate('initiateUpload()')
    await page.wait_for_function(MODAL_OPEN, timeout=10000)
    await page.evaluate('handleConfirmOk()')


async def idle(page, timeout_s=90):
    for _ in range(timeout_s * 4):
        if not await page.evaluate('uploading') and not api('/api/upload/active'):
            await asyncio.sleep(0.5)
            return True
        await asyncio.sleep(0.25)
    return False


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
        page = await ctx.new_page()
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", AEM)
        await fill(page, 'content/dam/Review')

        # uploadplan: Thumbs.db is skipped by the library, progress still reaches 100% and expected = 305
        await page.set_input_files('#browserFileInput', FILES)
        await start(page)
        await idle(page)
        expected = await page.evaluate('uploadExpectedFiles')
        label = await page.inner_text('#progressLabel')
        record('server upload plan replaces the browser count', expected == 305, f"uploadExpectedFiles={expected}, label={label!r}")

        # #7 Retry keeps the summary until a new upload starts (this run had 5 failures -> Partial)
        summary_before = await page.inner_text('#summaryBar')
        await page.evaluate('retryLastUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=10000)
        await page.evaluate('closeConfirm()')
        summary_after = await page.inner_text('#summaryBar') if await page.is_visible('#summaryBar') else ''
        record('#7 Retry keeps the summary when the dialog is cancelled', 'Partially' in summary_before and summary_after == summary_before,
               f"before={' '.join(summary_before.split())[:60]!r}, after visible={bool(summary_after)}")

        # #14 focus returns after closing/confirming a dialog
        await page.focus('#uploadBtn')
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=10000)
        await page.evaluate('closeConfirm()')
        trap_cleared = await page.evaluate('_modalFocusTrap === null')
        record('#14 dialog focus trap removed and focus restored', trap_cleared and await page.evaluate("document.activeElement.id") == 'uploadBtn',
               f"trap cleared={trap_cleared}, focus={await page.evaluate('document.activeElement.id')!r}")

        # #8 cancelled summary shows failures that happened before the cancel
        await page.set_input_files('#browserFileInput', FILES[:-1])
        await start(page)
        for _ in range(240):
            if await page.evaluate('filesFailed >= 1 && filesCompleted > 20'):
                break
            await asyncio.sleep(0.25)
        await page.keyboard.press('Escape')
        await page.keyboard.press('Escape')
        await idle(page)
        summary = ' '.join((await page.inner_text('#summaryBar')).split())
        stat_failed = await page.inner_text('#statFailed')
        report = await page.locator('#summaryBar >> text=Download Error Report').count()
        record('#8 cancelled summary counts earlier failures', 'Upload Cancelled' in summary and 'failed' in summary and stat_failed != '0' and report == 1,
               f"summary={summary[:90]!r}, Failed stat={stat_failed}, report button={report}")
        newest = api('/api/upload/history')[0]
        record('#8 cancelled history row keeps the failure count', newest.get('cancelled') and newest.get('errorCount', 0) >= 1,
               f"cancelled={newest.get('cancelled')}, errorCount={newest.get('errorCount')}")

        # #1 "View Progress" cannot take over a running upload
        await page.set_input_files('#browserFileInput', GOOD[:200])
        await start(page)
        for _ in range(240):
            if await page.evaluate('!!currentUploadId'):
                break
            await asyncio.sleep(0.25)
        own = await page.evaluate('currentUploadId')
        await page.evaluate("window._activeBannerUploadId = 'someone-else'; reconnectActiveUpload()")
        await asyncio.sleep(0.5)
        still = await page.evaluate('currentUploadId')
        banner_hidden = await page.evaluate("document.getElementById('activeUploadsBanner').classList.contains('hidden')")
        record('#1 reconnect blocked during own upload', still == own and banner_hidden, f"currentUploadId kept={still == own}, banner hidden while uploading={banner_hidden}")

        # #5 folder picker locked during an upload
        await page.evaluate('pickFolder()')
        await asyncio.sleep(0.3)
        toasts = await page.evaluate("[...document.querySelectorAll('#toastContainer > *')].map(e => e.textContent.trim())")
        record('#5 Select Folder blocked during upload', any('while an upload is running' in t for t in toasts), f"last toast={toasts[-1][:60] if toasts else None!r}")
        await idle(page)

        # #3 a finished upload whose done event was missed is detected instead of looping
        finished_id = api('/api/upload/history')[0]['uploadId']
        await page.evaluate(f"setUploading(true); currentUploadId = {json.dumps(finished_id)}; reconnectOrFinish({json.dumps(finished_id)})")
        await asyncio.sleep(1.5)
        record('#3 missed done event is handled', not await page.evaluate('uploading') and 'see Upload History' in await page.inner_text('#progressLabel'),
               f"uploading={await page.evaluate('uploading')}, label={await page.inner_text('#progressLabel')!r}")

        # #4 Enter on "Clear All" opens the dialog instead of collapsing history
        await page.evaluate("showView('history')")
        await page.focus('#clearHistoryBtn')
        await page.keyboard.press('Enter')
        await asyncio.sleep(0.5)
        opened = await page.evaluate(MODAL_OPEN)
        headline = await page.inner_text('#confirmHeadline')
        await page.evaluate('closeConfirm()')
        record('#4 Clear All works from the keyboard', opened and 'Clear' in headline, f"dialog opened={opened}, headline={headline!r}")

        # Cancel while Finalizing, then the tab closes: the server must not finish the upload
        async def hold_finalize(route):
            response = await route.fetch()
            await asyncio.sleep(2)
            try:
                await route.fulfill(response=response)
            except Exception:
                pass
        page2 = await ctx.new_page()
        await page2.goto(APP)
        await page2.wait_for_load_state('networkidle')
        await page2.wait_for_function('!!currentUser', timeout=15000)
        # the AEM password is never remembered, so a new tab needs it typed again
        await page2.evaluate("""(aem) => { document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; }""", AEM)
        await fill(page2, 'content/dam/ReviewTabClose')
        await page2.route('**/api/upload/browser/finalize', hold_finalize)
        await page2.set_input_files('#browserFileInput', GOOD)
        hist_before = len(api('/api/upload/history'))
        await start(page2)
        await page2.wait_for_function("/Finaliz/i.test(document.getElementById('transferStatus').textContent)", timeout=30000)
        await page2.evaluate('cancelUpload()')
        await asyncio.sleep(0.3)
        await page2.close()
        await asyncio.sleep(6)
        hist = api('/api/upload/history')
        newest = hist[0] if len(hist) > hist_before else None
        record('cancel during Finalizing holds even if the tab closes', not api('/api/upload/active') and (newest is None or newest.get('cancelled')),
               f"active={api('/api/upload/active')}, newest row={({k: newest.get(k) for k in ('cancelled', 'uploadedFiles', 'totalFiles')} if newest else 'none (never started)')}")

        # #12 #13 dark-mode colours
        await page.evaluate('applyDarkMode(true)')
        colors = await page.evaluate("""async () => { const u = document.getElementById('aemUrl');
            u.classList.remove('field-valid', 'field-warning'); u.classList.add('field-error');
            await new Promise(r => setTimeout(r, 600));   // the input has a CSS colour transition
            const c = getComputedStyle(u).borderColor; u.classList.remove('field-error'); return c; }""")
        await page.evaluate('applyDarkMode(false)')
        record('#12 validation border visible in dark mode', colors == 'rgb(215, 55, 63)', f"border={colors}")

        record('no JavaScript errors', not errors, f"{errors[:3]}")
        await browser.close()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


asyncio.run(main())
