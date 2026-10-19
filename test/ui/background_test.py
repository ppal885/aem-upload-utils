"""Reconnected speed/ETA and Run in background. usage: python test/ui/background_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import json
import os
import re
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1:4]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR))]
RUN = int(time.time())
results = []
_COOKIE = None


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} — {detail}")


def api(path):
    global _COOKIE
    if _COOKIE is None:
        req = urllib.request.Request(APP + '/api/auth/login', data=json.dumps({'email': 'admin@test.local', 'password': 'admin-pass-123'}).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req) as r:
            _COOKIE = r.headers.get('Set-Cookie').split(';')[0]
    with urllib.request.urlopen(urllib.request.Request(APP + path, headers={'Cookie': _COOKIE})) as r:
        return json.loads(r.read())


async def signed_in(context):
    page = await context.new_page()
    await page.goto(APP)
    await page.wait_for_function("typeof applySignedIn === 'function'", timeout=60000)
    await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user);
        document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin';
        updateSummaryPanel(); }""", AEM)
    return page


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={'width': 1280, 'height': 900})
        page = await signed_in(context)
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        await page.fill('#damPath', f'content/dam/BgTest{RUN}')
        await page.set_input_files('#browserFileInput', FILES)
        await page.evaluate('initiateUpload()')
        await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')", timeout=20000)
        await page.evaluate('handleConfirmOk()')
        await page.wait_for_function("document.getElementById('bgBtnInProgress').style.display === ''", timeout=120000)
        record('Run in background appears once the files are on the server', True)

        # A second tab reconnects after the upload has run a while: the replay must not read as a burst
        for _ in range(240):
            a = api('/api/upload/active')
            if a and a[0]['elapsedMs'] > 17000 and a[0]['completedFiles'] > 300:
                break
            await asyncio.sleep(0.25)
        other = await signed_in(context)
        await other.wait_for_function("!document.getElementById('activeUploadsBanner').classList.contains('hidden')", timeout=15000)
        await other.evaluate('reconnectActiveUpload()')
        await other.wait_for_function("/files\\/s/.test(document.getElementById('uploadSpeed').textContent)", timeout=20000)
        await asyncio.sleep(1.5)
        shown = await other.inner_text('#uploadSpeed')
        eta = await other.inner_text('#uploadEta')
        a = api('/api/upload/active')[0]
        real = a['completedFiles'] / (a['elapsedMs'] / 1000)
        rate = float(re.match(r'([\d.]+) files/s', shown).group(1))
        left_s = (a['expectedFiles'] - a['completedFiles']) / real
        record('after a reconnect, files/s is the average since the upload started (not the replay burst)',
               real / 2.5 <= rate <= real * 2.5, f'shown={shown!r} server average={real:.1f} files/s eta={eta!r} (about {left_s:.0f}s left)')
        # On a big upload the replay spans seconds, so the page would see a burst; the average must start at the
        # upload's start on the server, not when the replay began
        anchor = await other.evaluate("({ at: etaFirstDoneAt, count: etaFirstDoneCount, now: Date.now() })")
        started_ago = anchor['now'] - anchor['at']
        record('after a reconnect, speed is measured from the upload start on the server', anchor['count'] == 0 and abs(started_ago - a['elapsedMs']) < 5000,
               f"measured from {started_ago / 1000:.1f}s ago, upload started {a['elapsedMs'] / 1000:.1f}s ago, count={anchor['count']}")
        await other.close()

        # Run in background: the upload goes on, this page is free again
        before = api('/api/upload/active')
        await page.click('#bgBtnInProgress')
        await page.wait_for_function("!uploading", timeout=5000)
        state = await page.evaluate("""() => ({ panelHidden: document.getElementById('progressPanel').classList.contains('hidden'),
            damEnabled: !document.getElementById('damPath').disabled, files: browserFileEntries.length,
            banner: !document.getElementById('activeUploadsBanner').classList.contains('hidden') })""")
        await page.wait_for_function("!document.getElementById('activeUploadsBanner').classList.contains('hidden')", timeout=10000)
        after = api('/api/upload/active')
        record('Run in background: the upload keeps running on the server', bool(before) and bool(after) and after[0]['uploadId'] == before[0]['uploadId'],
               f"before={len(before)} after={len(after)}")
        record('Run in background: this page is free (form enabled, selection cleared so it is not sent twice), banner shows the upload',
               state['panelHidden'] and state['damEnabled'] and state['files'] == 0, json.dumps(state))
        await asyncio.sleep(2)
        record('Run in background: the page does not jump back to the upload by itself', not await page.evaluate('uploading'), '')
        await page.click('#activeUploadsBanner >> text=View Progress')
        await page.wait_for_function("uploading && document.getElementById('bgBtnInProgress').style.display === ''", timeout=10000)
        record('View Progress on the banner shows it again', True)
        for _ in range(600):
            if not await page.evaluate('uploading'):
                break
            await asyncio.sleep(0.5)
        hist = api('/api/upload/history')
        row = next((h for h in hist if h.get('damPath', '').endswith(f'BgTest{RUN}')), {})
        record('the upload finishes with every file', row.get('success') is True and row.get('totalFiles') == len(FILES), json.dumps({k: row.get(k) for k in ('success', 'totalFiles')}))
        record('no page errors', not errors, str(errors[:2]))
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
