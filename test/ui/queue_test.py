"""Per-AEM-server queue, with MAX_UPLOADS_PER_SERVER=1. usage: python test/ui/queue_test.py"""
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
APP = 'http://127.0.0.1:3971'
AEM_A = 'http://127.0.0.1:4620'
AEM_OTHER = 'http://localhost:4620'
RUN = str(int(time.time()))
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
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''))


def completed(prefix):
    with urllib.request.urlopen(AEM_A + '/__completed') as r:
        return [p for p in json.loads(r.read()) if p.startswith(prefix)]


def start_server():
    log = open(os.path.join(WORK, 'queue-server.log'), 'w')
    proc = subprocess.Popen(['node', LAUNCH], stdout=log, stderr=log, env=server_env('queue', 3971, {'MAX_UPLOADS_PER_SERVER': '1'}))
    for _ in range(60):
        try:
            urllib.request.urlopen(APP + '/api/health', timeout=1)
            return proc
        except Exception:
            time.sleep(0.5)
    raise RuntimeError('server did not start')


async def user(browser, email, password, aem, dam, n):
    ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
    page = await ctx.new_page()
    await page.goto(APP, wait_until='domcontentloaded')
    await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
    await page.fill('#loginEmail', email)
    await page.fill('#loginPassword', password)
    await page.click('#loginSubmit')
    await page.wait_for_function('!!currentUser', timeout=15000)
    await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'admin';
        document.getElementById('password').value = 'admin'; }""", aem)
    await page.fill('#damPath', dam)
    await page.evaluate("""(n) => selectNewFiles(Array.from({length: n}, (_, i) => ({ file: new File(['<t/>'], 't' + i + '.dita'),
        relativePath: 's/t' + i + '.dita' })))""", n)
    await page.evaluate('initiateUpload()')
    await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
    await page.evaluate('handleConfirmOk()')
    await page.wait_for_function('uploading', timeout=15000)
    return page


async def wait_done(page, timeout=180):
    for _ in range(timeout * 4):
        if not await page.evaluate('uploading'):
            return True
        await asyncio.sleep(0.25)
    return False


async def main():
    proc = start_server()
    a_dam, b_dam, c_dam, d_dam = (f'content/dam/q{RUN}{x}' for x in 'abcd')
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        a = await user(browser, 'admin@test.local', 'admin-pass-123', AEM_A, a_dam, 3000)
        await a.wait_for_function("document.getElementById('progressLabel').textContent.includes('%')", timeout=60000)

        b = await user(browser, 'qe@test.local', 'qe-pass-12345', AEM_A, b_dam, 300)
        await b.wait_for_function("!document.getElementById('queueNotice').classList.contains('hidden')", timeout=60000)
        b_note = await b.inner_text('#queueNoticeTitle')
        record('second upload to the same AEM waits and sees its place', '#1 in line' in b_note and '127.0.0.1:4620' in b_note, b_note)
        b_files_while_waiting = len(completed('/' + b_dam + '/'))

        c = await user(browser, 'qe@test.local', 'qe-pass-12345', AEM_OTHER, c_dam, 200)
        await asyncio.sleep(3)
        c_hidden = await c.evaluate("document.getElementById('queueNotice').classList.contains('hidden')")
        c_done = await wait_done(c, 60)
        record('an upload to a different AEM server is not held up', c_hidden and c_done, f'notice hidden={c_hidden}, finished={c_done}')

        d = await user(browser, 'admin@test.local', 'admin-pass-123', AEM_A, d_dam, 100)
        await d.wait_for_function("document.getElementById('queueNoticeTitle').textContent.includes('#2')", timeout=60000)
        record('third upload is #2 in line', True, await d.inner_text('#queueNoticeTitle'))
        banner = json.loads(await d.evaluate("fetch('/api/upload/active').then(r => r.text())"))
        record('active uploads list shows the queue position', any(u.get('queuePosition') == 2 for u in banner), str([u.get('queuePosition') for u in banner]))
        await d.evaluate('cancelUpload()')
        await asyncio.sleep(1)
        if await d.is_visible('#confirmModal:not(.hidden)'):
            await d.evaluate('handleConfirmOk()')
        d_stopped = await wait_done(d, 20)
        record('cancel while waiting leaves the line cleanly', d_stopped, f'stopped={d_stopped}')

        a_ok = await wait_done(a, 240)
        await b.wait_for_function("document.getElementById('queueNotice').classList.contains('hidden')", timeout=60000)
        b_ok = await wait_done(b, 120)
        record('waiting upload sent nothing until its turn, then finished by itself',
               b_files_while_waiting == 0 and a_ok and b_ok and len(set(completed('/' + b_dam + '/'))) == 300,
               f'files while waiting={b_files_while_waiting}, A done={a_ok}, B done={b_ok}, B files={len(set(completed("/" + b_dam + "/")))}')
        record('the cancelled upload never reached AEM', len(completed('/' + d_dam + '/')) == 0, str(len(completed('/' + d_dam + '/'))))
        record('first upload complete', len(set(completed('/' + a_dam + '/'))) == 3000, str(len(set(completed('/' + a_dam + '/')))))
        await browser.close()
    proc.kill()
    proc.wait()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
