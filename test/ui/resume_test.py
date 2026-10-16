"""Hard-kill the server mid-upload, start it again, Resume. usage: python test/ui/resume_test.py <mock aem url>"""
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

AEM = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
APP = 'http://127.0.0.1:3970'
LAUNCH = os.path.join(os.path.dirname(HERE), 'launch.js')
WORK = os.environ['TEST_WORK_DIR']


def server_env(name, port, extra=None):
    d = os.path.join(WORK, name)
    tmp = os.path.join(d, 'tmp')
    os.makedirs(tmp, exist_ok=True)
    env = dict(os.environ, PORT=str(port), TMPDIR=tmp, TEMP=tmp, TMP=tmp,
               HISTORY_FILE=os.path.join(d, 'upload-history.json'), PACKAGES_DIR=os.path.join(d, 'packages'),
               BACKUP_DIR=os.path.join(d, 'backups'))
    env.update(extra or {})
    return env
N = 3000
RUN = str(int(time.time()))
DAM = f'content/dam/resume{RUN}'
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''))


def completed():
    with urllib.request.urlopen(AEM + '/__completed') as r:
        return [p for p in json.loads(r.read()) if p.startswith('/' + DAM + '/')]


def start_server():
    log = open(os.path.join(WORK, 'resume-server.log'), 'a')
    proc = subprocess.Popen(['node', LAUNCH], stdout=log, stderr=log, env=server_env('resume', 3970))
    for _ in range(60):
        try:
            urllib.request.urlopen(APP + '/api/health', timeout=1)
            return proc
        except Exception:
            time.sleep(0.5)
    raise RuntimeError('server did not start')


async def sign_in(page):
    await page.goto(APP, wait_until='domcontentloaded')
    await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
    await page.fill('#loginEmail', 'admin@test.local')
    await page.fill('#loginPassword', 'admin-pass-123')
    await page.click('#loginSubmit')
    await page.wait_for_function('!!currentUser', timeout=15000)


async def main():
    proc = start_server()
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await sign_in(page)
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'admin';
            document.getElementById('password').value = 'admin'; }""", AEM)
        await page.fill('#damPath', DAM)
        await page.evaluate("""(n) => selectNewFiles(Array.from({length: n}, (_, i) => ({ file: new File(['<topic id="t' + i + '"/>'], 't' + i + '.dita'),
            relativePath: 'set/g' + Math.floor(i / 300) + '/t' + i + '.dita' })))""", N)
        await page.evaluate('initiateUpload()')
        await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
        await page.evaluate('handleConfirmOk()')
        for _ in range(600):
            if len(completed()) >= N * 0.3:
                break
            await asyncio.sleep(0.2)
        before_kill = len(completed())
        proc.kill()
        proc.wait()
        await asyncio.sleep(1.5)
        record('server killed in the middle of the upload', 0 < before_kill < N, f'{before_kill} of {N} files in AEM when killed')

        proc = start_server()
        await sign_in(page)
        await page.wait_for_function("!document.getElementById('interruptedBox').classList.contains('hidden')", timeout=10000)
        text = await page.inner_text('#interruptedList')
        record('after restart the page offers Resume with the remaining count', DAM in text and 'left' in text, text.replace('\n', ' | ')[:160])

        await page.fill('#interruptedList input[type=password]', 'wrong')
        await page.click('#interruptedList button[type=submit]')
        await page.wait_for_function("!document.querySelector('#interruptedList .err').classList.contains('hidden')", timeout=10000)
        err = await page.inner_text('#interruptedList .err')
        record('wrong AEM password is refused before anything is sent', len(err) > 0, err)

        await page.fill('#interruptedList input[type=password]', 'admin')
        await page.click('#interruptedList button[type=submit]')
        await page.wait_for_function("document.getElementById('interruptedBox').classList.contains('hidden')", timeout=15000)
        for _ in range(900):
            h = json.loads(await page.evaluate("fetch('/api/upload/active').then(r => r.text())"))
            if not h:
                break
            await asyncio.sleep(0.3)
        got = completed()
        unique = set(got)
        want = {f'/{DAM}/set/g{i // 300}/t{i}.dita' for i in range(N)}
        record('every file reached AEM after Resume', want <= unique, f'{len(want - unique)} missing')
        record('almost nothing was sent twice', len(got) - len(unique) <= 64, f'{len(got) - len(unique)} files sent twice (in flight at the kill)')
        hist = json.loads(await page.evaluate("fetch('/api/upload/history').then(r => r.text())"))
        rows = hist if isinstance(hist, list) else hist.get('history', [])
        resumed = next((r for r in rows if r.get('damPath') == DAM and 'Resumed' in (r.get('localPath') or '')), None)
        record('history shows the resumed upload as successful', bool(resumed and resumed.get('success')), json.dumps({k: resumed.get(k) for k in ('localPath', 'success', 'totalFiles')}) if resumed else 'no row')
        left = [d for d in os.listdir(os.path.join(WORK, 'resume', 'tmp')) if d.startswith('guides-upload-')] if os.path.isdir(os.path.join(WORK, 'resume', 'tmp')) else []
        await asyncio.sleep(31)
        left = [d for d in os.listdir(os.path.join(WORK, 'resume', 'tmp')) if d.startswith('guides-upload-')]
        record('staging folder removed after the resumed upload finished', not left, str(left))
        await browser.close()
    proc.kill()
    proc.wait()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
