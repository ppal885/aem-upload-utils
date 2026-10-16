"""Browser checks for High #7 and #8. usage: python test/ui/high_7_8_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import json
import os
import sys
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1:4]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR))][:400]
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


DROP_JS = """(target) => {
    const dt = new DataTransfer();
    for (let i = 0; i < 7; i++) dt.items.add(new File(['<t/>'], 'dropped' + i + '.dita'));
    const el = target === 'body' ? document.querySelector('footer') || document.body : document.getElementById('dropZone');
    el.dispatchEvent(new DragEvent('dragenter', { dataTransfer: dt, bubbles: true }));
    el.dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
}"""


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", AEM)

        # #7 a drop outside the dashed box selects the files; the drop zone still works
        url_before = page.url
        await page.evaluate(DROP_JS, 'body')
        await asyncio.sleep(0.5)
        outside = await page.evaluate('browserFileEntries.length')
        overlay_hidden = await page.evaluate("document.getElementById('fullPageDropOverlay').classList.contains('hidden')")
        await page.evaluate('clearBrowserFiles()')
        await page.evaluate(DROP_JS, 'zone')
        await asyncio.sleep(0.5)
        inside = await page.evaluate('browserFileEntries.length')
        record('#7 drop anywhere on the page selects files', outside == 7 and inside == 7 and overlay_hidden and page.url == url_before,
               f"outside drop={outside} files, drop zone={inside} files, overlay hidden={overlay_hidden}, stayed on page={page.url == url_before}")

        # #8 a cancelled upload is shown as cancelled, not as an error
        await page.fill('#damPath', 'content/dam/UiHigh78')
        await page.set_input_files('#browserFileInput', FILES)
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=10000)
        await page.evaluate('handleConfirmOk()')
        for _ in range(240):
            if await page.evaluate('filesCompleted > 30'):
                break
            await asyncio.sleep(0.25)
        await page.keyboard.press('Escape')
        await page.keyboard.press('Escape')
        await page.wait_for_function('!uploading', timeout=60000)
        await asyncio.sleep(1)
        summary = await page.inner_text('#summaryBar')
        report_btn = await page.locator('#summaryBar >> text=Download Error Report').count()
        stat_uploaded = int(await page.inner_text('#statUploaded'))
        record('#8 summary says Cancelled with the real count',
               'Upload Cancelled' in summary and 'error' not in summary.lower() and report_btn == 0 and 0 < stat_uploaded < 400,
               f"summary={' '.join(summary.split())[:120]!r}, error report button={report_btn}, Uploaded stat={stat_uploaded}")

        newest = api('/api/upload/history')[0]
        await page.evaluate('loadHistory()')
        await asyncio.sleep(1)
        badge = await page.inner_text('#historyBody tr:first-child [data-col="status"]')
        await page.evaluate("setHistoryFilter('cancelled')")
        cancelled_rows = await page.locator('#historyBody tr').count()
        await page.evaluate("setHistoryFilter('failed')")
        failed_texts = await page.locator('#historyBody [data-col="status"]').all_inner_texts()
        await page.evaluate("setHistoryFilter('all')")
        record('#8 history shows a Cancelled badge and filter',
               newest.get('cancelled') and newest.get('errors') == [] and newest.get('uploadedFiles') is not None
               and badge.strip() == 'Cancelled' and cancelled_rows >= 1 and 'Cancelled' not in failed_texts,
               f"history row errors={newest.get('errors')}, uploadedFiles={newest.get('uploadedFiles')}, badge={badge.strip()!r}, "
               f"Cancelled filter rows={cancelled_rows}, Cancelled in Failed filter={'Cancelled' in failed_texts}")

        await browser.close()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


asyncio.run(main())
