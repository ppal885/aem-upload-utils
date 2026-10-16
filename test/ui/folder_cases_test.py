"""Folder/name edge cases end to end. usage: python test/ui/folder_cases_test.py <app url> <mock aem url>"""
import asyncio
import json
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"
SELECT_JS = """(paths) => { selectNewFiles(paths.map((p) => ({ file: new File(['<t/>'], p.split('/').pop()), relativePath: p }))); }"""
RUN = str(int(time.time()))
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"\n        {detail}" if detail and not ok else ''))


def mock(path):
    with urllib.request.urlopen(AEM + path) as r:
        return json.loads(r.read())


async def upload(page, dam, paths, levels=0):
    await page.fill('#damPath', dam)
    await page.evaluate(SELECT_JS, paths)
    if levels:
        await page.select_option('#skipLevels', str(levels))
    before = len(mock('/__completed'))
    await page.evaluate('initiateUpload()')
    try:
        await page.wait_for_function(MODAL_OPEN, timeout=5000)
    except Exception:
        return None, None, None
    note = await page.inner_text('#confirmBody')
    planned = await page.evaluate("() => AemNames.planUpload(relPathsOf(browserFileEntries), val('damPath'), skipLevels()).landed")
    await page.evaluate('handleConfirmOk()')
    await page.wait_for_function('uploading', timeout=10000)
    for _ in range(400):
        if not await page.evaluate('uploading'):
            break
        await asyncio.sleep(0.25)
    return note, sorted(planned), sorted(mock('/__completed')[before:])


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'admin@test.local')
        await page.fill('#loginPassword', 'admin-pass-123')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'admin';
            document.getElementById('password').value = 'admin'; }""", AEM)

        # 1. skip hierarchy: any number of shared levels
        deep = ['c/f1/f2/a.dita', 'c/f1/f2/sub/b.dita', 'c/f1/f2/top.ditamap']
        await page.evaluate(SELECT_JS, deep)
        options = await page.eval_on_selector_all('#skipLevels option', 'els => els.map(e => e.textContent)')
        record('level picker offers keep c, inside c, inside c/f1, inside c/f1/f2', len(options) == 4, str(options))
        base = f'content/dam/qa{RUN}'
        _, _, got = await upload(page, base + '/en', deep, 2)
        want = [f'/{base}/en/f2/a.dita', f'/{base}/en/f2/sub/b.dita', f'/{base}/en/f2/top.ditamap']
        record('skip 2 levels (c, f1): f2 lands in an existing/new en folder', got == want, f'got {got}')
        _, _, got = await upload(page, base + '/en3', deep, 3)
        want = [f'/{base}/en3/a.dita', f'/{base}/en3/sub/b.dita', f'/{base}/en3/top.ditamap']
        record('skip all 3 levels: contents go straight into the DAM folder', got == want, f'got {got}')
        _, _, got = await upload(page, base + '/keep', deep, 0)
        record('no skip keeps the whole structure', got == [f'/{base}/keep/c/f1/f2/a.dita', f'/{base}/keep/c/f1/f2/sub/b.dita', f'/{base}/keep/c/f1/f2/top.ditamap'], f'got {got}')

        # 2. deep, not-yet-existing DAM sub path typed in different ways
        await page.fill('#damPath', f'\\content\\dam\\qa{RUN}\\Product Docs\\v 2\\')
        await page.dispatch_event('#damPath', 'blur')
        cleaned = await page.input_value('#damPath')
        record('DAM path with backslashes/slashes is cleaned', cleaned == f'content/dam/qa{RUN}/Product Docs/v 2', cleaned)
        _, _, got = await upload(page, cleaned, ['sub/x.dita'], 0)
        record('missing parent folders (with spaces) are created, then files land', got == [f'/content/dam/qa{RUN}/Product Docs/v 2/sub/x.dita'], f'got {got}')
        await page.fill('#damPath', f'https://author.example.com/assets.html/content/dam/qa{RUN}/pasted')
        await page.dispatch_event('#damPath', 'blur')
        record('a pasted Assets URL becomes the DAM path', await page.input_value('#damPath') == f'content/dam/qa{RUN}/pasted')

        # 3. DAM paths that must be refused with a clear message
        for bad, why in [('content/dam/a#b', 'character #'), ('content/foo', 'inside /content/dam'), ('content/dam/x/../y', '".."'), ('content/dam/50%', 'character %')]:
            await page.fill('#damPath', bad)
            await page.evaluate(SELECT_JS, ['a.dita'])
            await page.evaluate('initiateUpload()')
            await asyncio.sleep(0.3)
            err = await page.evaluate("() => (document.getElementById('damPath-error') || document.querySelector('[data-error-for=damPath]') || {}).textContent || document.getElementById('damPath').getAttribute('aria-invalid')")
            opened = await page.evaluate(MODAL_OPEN)
            record(f'DAM path {bad!r} refused', not opened, f'modal opened, field msg={err!r}')
            if opened:
                await page.evaluate("document.getElementById('confirmModal').classList.add('hidden')")

        # 4. special characters, file types, system files
        nfd = 'ñandú'.replace('ú', 'ú')
        special = [
            'Docs/My Folder/My Topic (v2).dita', 'Docs/My Folder/a#b.dita', 'Docs/My Folder/50% off.png', 'Docs/My Folder/a&b.svg',
            f'Docs/v1.2/{nfd}.dita', 'Docs/v1.2/日本語.dita', 'Docs/v1.2/a..b.dita', 'Docs/v1.2/TestFile1.dita', 'Docs/v1.2/~notes.dita',
            'Docs/C#/report.tmp', 'Docs/C#/noext', 'Docs/C#/UPPER.DITA', 'Docs/C#/a.b.c.xml', "Docs/R&D/it's.dita", 'Docs/R&D/a+b.dita',
            'Docs/R&D/[draft].dita', 'Docs/Größe/x.pdf', 'Docs/folder [old]/y.zip', 'Docs/Größe/space .dita',
            'Docs/.git/config', 'Docs/My Folder/.DS_Store', 'Docs/My Folder/Thumbs.db', 'Docs/~$lock.docx', 'Docs/__MACOSX/z.dita',
        ]
        await page.evaluate(SELECT_JS, special)
        sysnote = await page.inner_text('#systemFilesNote')
        count = await page.evaluate('browserFileEntries.length')
        record('system files left out (.git, .DS_Store, Thumbs.db, ~$ lock, __MACOSX) and counted', count == 19 and sysnote.startswith('5 system files'), f'count={count}, note={sysnote!r}')
        note, planned, got = await upload(page, f'content/dam/qa{RUN}/special', special, 1)
        record('every special-character file landed exactly where the preview said', planned == got and len(got) == 19,
               f'\n        missing={sorted(set(planned) - set(got))}\n        extra={sorted(set(got) - set(planned))}')
        for expect in [f'/content/dam/qa{RUN}/special/My-Folder/My Topic (v2).dita', f'/content/dam/qa{RUN}/special/My-Folder/a-b.dita',
                       f'/content/dam/qa{RUN}/special/v1-2/a..b.dita', f'/content/dam/qa{RUN}/special/v1-2/TestFile1.dita',
                       f'/content/dam/qa{RUN}/special/v1-2/~notes.dita', f'/content/dam/qa{RUN}/special/C-/report.tmp',
                       f'/content/dam/qa{RUN}/special/v1-2/ñandú.dita', f'/content/dam/qa{RUN}/special/folder--old-/y.zip']:
            record(f'landed {expect.split("/special/")[1]}', expect in got)
        titles = mock('/__folders')
        record('renamed folders keep the original name as title',
               titles.get(f'/content/dam/qa{RUN}/special/My-Folder') == 'My Folder' and titles.get(f'/content/dam/qa{RUN}/special/R-D') == 'R&D',
               json.dumps({k: v for k, v in titles.items() if '/special/' in k}, ensure_ascii=False))
        record('confirm dialog lists the renamed names', 'will change' in (note or '') and 'My Folder' in (note or '') and 'My-Folder' in (note or ''), note)

        # 5. folder named like the DAM folder is not doubled
        note, _, got = await upload(page, f'content/dam/qa{RUN}/en', ['en/a.dita', 'en/b/c.dita'], 0)
        record('"en" into .../en does not create en/en, and the dialog says so',
               got == [f'/content/dam/qa{RUN}/en/a.dita', f'/content/dam/qa{RUN}/en/b/c.dita'] and 'same name as the DAM folder' in (note or ''), f'got {got}')

        # 6. clashes and too-long names are flagged before upload
        await page.fill('#damPath', f'content/dam/qa{RUN}/clash')
        await page.evaluate(SELECT_JS, ['k/a#.dita', 'k/a-.dita', 'k/' + 'x' * 260 + '.dita'])
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=5000)
        note = await page.inner_text('#confirmBody')
        record('clash warning shown', 'would overwrite another file' in note, note)
        record('too-long name warning shown', 'longer than 255 bytes' in note, note)
        await page.evaluate("document.getElementById('confirmModal').classList.add('hidden')")

        fav = await page.evaluate("document.querySelector('link[rel=icon]').href")
        record('tab icon is the red logo', '/favicon.svg' in fav, fav)

        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
