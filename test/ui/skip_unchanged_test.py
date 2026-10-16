"""Skip files already in AEM and unchanged. usage: python test/ui/skip_unchanged_test.py <app> <mock aem>"""
import asyncio
import json
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
RUN = str(int(time.time()))
results = []
SELECT_JS = """(files) => selectNewFiles(files.map(([p, body]) => ({ file: new File([body], p.split('/').pop()), relativePath: p })))"""


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''))


def completed(prefix):
    with urllib.request.urlopen(AEM + '/__completed') as r:
        return [p for p in json.loads(r.read()) if p.startswith(prefix)]


def files(changed=(), extra=0):
    out = [[f'docs/My Folder/t{i}.dita' if i % 10 == 0 else f'docs/set/t{i}.dita',
            f'<topic id="t{i}">{"changed " if i in changed else ""}body {i}</topic>'] for i in range(200)]
    out.append(['docs/v1.2/a#b (draft).dita', '<topic id="special">special</topic>'])
    out += [[f'docs/new/n{i}.dita', f'<topic id="n{i}"/>'] for i in range(extra)]
    return out


async def upload(page, dam, selection, skip):
    await page.fill('#damPath', dam)
    await page.evaluate(SELECT_JS, selection)
    await page.set_checked('#skipUnchanged', skip)
    toasts_before = await page.evaluate("document.querySelectorAll('#toastContainer > *').length")
    before = len(completed('/' + dam + '/'))
    await page.evaluate('initiateUpload()')
    await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
    note = await page.inner_text('#confirmBody')
    await page.evaluate('handleConfirmOk()')
    await page.wait_for_function('uploading', timeout=15000)
    for _ in range(600):
        if not await page.evaluate('uploading'):
            break
        await asyncio.sleep(0.25)
    sent = completed('/' + dam + '/')[before:]
    toasts = await page.evaluate("[...document.querySelectorAll('#toastContainer > *')].map(t => t.textContent.trim())")
    return note, sent, toasts


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'qe@test.local')
        await page.fill('#loginPassword', 'qe-pass-12345')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'qeuser';
            document.getElementById('password').value = 'qeuser'; }""", AEM)
        dam = f'content/dam/skip{RUN}'

        _, sent, _ = await upload(page, dam, files(), False)
        record('first upload sends everything', len(sent) == 201, f'{len(sent)} sent')

        note, sent, toasts = await upload(page, dam, files(changed={3, 50, 120}, extra=7), True)
        names = sorted(x.split('/')[-1] for x in sent)
        record('confirm dialog explains the option', 'already in AEM with the same content are skipped' in note, note[:200])
        record('second upload sends only changed and new files', len(sent) == 10 and {'t3.dita', 't50.dita', 't120.dita'} <= set(names),
               f'{len(sent)} sent: {names[:12]}')
        record('special-character names are matched too (a#b → a-b)', not any('a-b' in x for x in sent), str([x for x in sent if 'draft' in x]))
        record('the page says how many were skipped', any('198 file(s) skipped' in t for t in toasts), str(toasts[-3:]))
        hist = json.loads(await page.evaluate("fetch('/api/upload/history').then(r => r.text())"))
        rows = hist if isinstance(hist, list) else hist.get('history', [])
        row = next((r for r in rows if r.get('damPath') == dam), {})
        record('history remembers the skipped count', row.get('skippedFiles') == 198 and row.get('success'), json.dumps({k: row.get(k) for k in ('totalFiles', 'skippedFiles', 'success')}))
        await page.evaluate("showView('history')")
        await page.wait_for_function("document.getElementById('historyBody') && document.getElementById('historyBody').innerText.includes('198 skipped')", timeout=10000)
        record('history row shows "+198 skipped"', True)
        await page.evaluate("showView('upload')")

        _, sent, _ = await upload(page, dam, files(changed={3, 50, 120}, extra=7), False)
        record('with the option off everything is sent again', len(sent) == 208, f'{len(sent)} sent')

        fail_dam = f'content/dam/qbfail{RUN}'
        await upload(page, fail_dam, files(), False)
        _, sent, toasts = await upload(page, fail_dam, files(), True)
        record('if AEM search fails, everything is uploaded and the page warns', len(sent) == 201 and any('Could not check AEM' in t for t in toasts),
               f'{len(sent)} sent; {toasts[-2:]}')
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
