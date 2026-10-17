"""Progress display follows finished files. usage: python test/ui/progress_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import json
import os
import re
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1:4]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR)) if f.startswith('t')]
# the page gets progress in small batches, so it may show a server reading from up to this long ago
PAGE_LAG_S = 1.0
MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"


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


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", AEM)
        await page.fill('#damPath', 'content/dam/ProgressTest')
        await page.set_input_files('#browserFileInput', FILES)
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=10000)
        await page.evaluate('handleConfirmOk()')
        samples = []
        readings = []
        last_sample = 0
        for _ in range(400):
            a = api('/api/upload/active')
            if a:
                readings.append((time.monotonic(), a[0]['completedFiles'] * 100 / a[0]['expectedFiles']))
            if a and a[0]['completedFiles'] > 40 and (not samples or time.monotonic() - last_sample >= 1.5):
                shown = int((await page.inner_text('#progressPct')).rstrip('%') or 0)
                last_sample = time.monotonic()
                recent = [pct for t, pct in readings if last_sample - t <= PAGE_LAG_S]
                samples.append((recent, shown, await page.inner_text('#uploadSpeed'), await page.inner_text('#uploadEta')))
                if len(samples) >= 4:
                    break
            await asyncio.sleep(0.25)
        header = await page.evaluate("document.getElementById('fileList').innerText.split('\\n').slice(0, 5).join(' | ')")
        ok = bool(samples)
        for recent, shown, speed, eta in samples:
            real = recent[-1]
            ok = ok and any(abs(shown - pct) <= 3 for pct in recent) and (eta.startswith('Calculating') or re.match(r'ETA (\d+h )?\d+m? ?\d*s?|ETA \d+s', eta) is not None)
            print(f"  real={real:.1f}% shown={shown}% speed={speed!r} eta={eta!r}")
        print(f"  header: {header[:140]!r}")
        ok = ok and ('remaining' in header or '300 done' in header) and 'in progress' not in header and not errors
        print('[PASS]' if ok else '[FAIL]', 'progress follows finished files; files/s; real ETA', errors[:2])
        while await page.evaluate('uploading'):
            await asyncio.sleep(0.5)
        await browser.close()
        sys.exit(0 if ok else 1)


asyncio.run(main())
