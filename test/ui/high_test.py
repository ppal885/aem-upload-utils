"""Browser checks for the High 1-5 UI fixes. usage: python test/ui/high_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import json
import os
import sys
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1], sys.argv[2], sys.argv[3]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR))]
results = []


def record(name, ok, detail):
    results.append((name, ok, detail))
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


async def fill_form(page, password):
    await page.fill('#damPath', 'content/dam/UiTest')
    await page.set_input_files('#browserFileInput', FILES)


async def start_upload(page):
    await page.evaluate('initiateUpload()')
    await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')", timeout=10000)
    await page.evaluate('handleConfirmOk()')


async def wait_idle(page, timeout_s=60):
    for _ in range(timeout_s * 4):
        if not await page.evaluate('uploading') and not api('/api/upload/active'):
            return True
        await asyncio.sleep(0.25)
    return False


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", AEM)

        # #3 a refusal before the AEM phase must show a visible "did not start" message
        async def refuse_init(route):
            await route.fulfill(status=401, content_type='application/json',
                                body='{"error":"AEM rejected the username or password (HTTP 401). Check the credentials and try again."}')
        await page.route('**/api/upload/browser/init', refuse_init)
        await fill_form(page, 'admin')
        await start_upload(page)
        await page.wait_for_function("!uploading", timeout=20000)
        await page.unroute('**/api/upload/browser/init')
        visible = await page.is_visible('#summaryBar')
        text = (await page.inner_text('#summaryBar')) if visible else ''
        record('#3 error before AEM phase is shown', visible and 'did not start' in text and '401' in text,
               f"summary visible={visible}, text={text.strip()[:110]!r}")

        # #1 progress % must follow completed/selected files, not completed/started
        await page.set_input_files('#browserFileInput', FILES)
        # history keeps 200 rows per person, so compare the newest row instead of counting
        newest_before = (api('/api/upload/history') or [{}])[0].get('uploadId')
        await start_upload(page)
        samples = []
        for _ in range(240):
            active = api('/api/upload/active')
            if active and active[0]['completedFiles'] > 20:
                a = active[0]
                pct_text = await page.inner_text('#progressPct')
                remaining = await page.inner_text('#statRemaining')
                samples.append((a['completedFiles'], a['expectedFiles'], pct_text, remaining))
                if len(samples) >= 3:
                    break
            await asyncio.sleep(0.5)
        ok = bool(samples)
        details = []
        for done, expected, pct_text, remaining in samples:
            pct = int(pct_text.rstrip('%') or 0)
            real = round(done * 100 / expected)
            ok = ok and abs(pct - real) <= 10 and int(remaining) >= expected - done - 40
            details.append(f"done={done}/{expected} real={real}% shown={pct}% remaining={remaining}")
        record('#1 progress uses the real total', ok, '; '.join(details) or 'no samples taken')
        await wait_idle(page)
        newest = api('/api/upload/history')[0]
        record('#1 upload itself completes', newest.get('uploadId') != newest_before and newest['success'],
               f"newest: success={newest['success']} files={newest['totalFiles']}")

        # #4 Cancel while "Finalizing..." must stop the AEM upload
        async def slow_finalize(route):
            # the server starts the AEM upload at once; only the reply to the browser is held back
            response = await route.fetch()
            await asyncio.sleep(3)
            await route.fulfill(response=response)
        await page.route('**/api/upload/browser/finalize', slow_finalize)
        # same selection as the previous successful upload is ignored by the page (separate known bug), so drop one file
        await page.set_input_files('#browserFileInput', FILES[:-1])
        newest_before = (api('/api/upload/history') or [{}])[0].get('uploadId')
        await start_upload(page)
        await page.wait_for_function("document.getElementById('transferPanel') && /Finaliz/i.test(document.body.innerText)", timeout=20000)
        await page.evaluate('cancelUpload()')
        # The page shows "Cancelling..." until the server confirms (finalize time + the 3 s held response)
        for _ in range(80):
            if not await page.evaluate('uploading'):
                break
            await asyncio.sleep(0.25)
        await asyncio.sleep(1)
        hist = api('/api/upload/history')
        newest = hist[0] if hist and hist[0].get('uploadId') != newest_before else None
        ui_uploading = await page.evaluate('uploading')
        sse_open = await page.evaluate('!!activeUploadEventSource')
        # Either the server never started the AEM upload (no row) or it started and was cancelled
        cancelled_ok = newest is None or (newest.get('cancelled') and newest['totalFiles'] < len(FILES))
        record('#4 cancel during Finalizing stops the upload',
               bool(cancelled_ok) and not ui_uploading and not sse_open and not api('/api/upload/active'),
               f"history newest={({k: newest[k] for k in ('cancelled', 'totalFiles', 'success')} if newest else None)}, "
               f"ui uploading={ui_uploading}, progress stream open={sse_open}")
        await page.unroute('**/api/upload/browser/finalize')
        await wait_idle(page)

        # #2 history must be visible on a narrow window
        await page.set_viewport_size({'width': 600, 'height': 900})
        await page.evaluate("showView('history')")
        await page.evaluate('loadHistory()')
        await asyncio.sleep(1)
        display = await page.evaluate("getComputedStyle(document.querySelector('#histPanel table')).display")
        rows = await page.locator('#historyBody tr').count()
        first_row_visible = await page.is_visible('#historyBody tr >> nth=0')
        record('#2 history visible at 600px', display != 'none' and rows > 0 and first_row_visible,
               f"table display={display}, rows={rows}, first row visible={first_row_visible}")
        await page.set_viewport_size({'width': 1280, 'height': 900})

        # #5 Enter on the dialog's X must close it, not clear history
        hist_before = len(api('/api/upload/history'))
        await page.evaluate('clearAllHistory()')
        await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')", timeout=10000)
        focused = await page.evaluate("document.activeElement.getAttribute('aria-label') || document.activeElement.textContent.trim()")
        await page.keyboard.press('Enter')
        await asyncio.sleep(1)
        modal_hidden = await page.evaluate("document.getElementById('confirmModal').classList.contains('hidden')")
        hist_after = len(api('/api/upload/history'))
        record('#5 Enter on X closes without clearing', modal_hidden and hist_after == hist_before,
               f"focused={focused!r}, dialog closed={modal_hidden}, history {hist_before} -> {hist_after}")

        await browser.close()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


asyncio.run(main())
