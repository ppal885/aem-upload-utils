"""'Upload the folder's contents' option. usage: python test/ui/skip_top_test.py <app url> <mock aem url>"""
import asyncio
import json
import sys
import urllib.request

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"
SELECT_JS = """(paths) => { selectNewFiles(paths.map((p) => ({ file: new File(['<t/>'], p.split('/').pop()), relativePath: p }))); }"""
PATHS = ['f1/f2/a.dita', 'f1/f2/sub/b.dita', 'f1/top.dita']


def completed():
    with urllib.request.urlopen(AEM + '/__completed') as r:
        return json.loads(r.read())


async def run(page, dam, skip):
    await page.fill('#damPath', dam)
    await page.evaluate(SELECT_JS, PATHS)
    await page.select_option('#skipLevels', '1' if skip else '0')
    hint = await page.inner_text('#skipTopFolderHint')
    before = len(completed())
    await page.evaluate('initiateUpload()')
    await page.wait_for_function(MODAL_OPEN, timeout=10000)
    note = await page.inner_text('#confirmBody')
    await page.evaluate('handleConfirmOk()')
    for _ in range(240):
        if not await page.evaluate('uploading'):
            break
        await asyncio.sleep(0.25)
    return hint, note, sorted(completed()[before:])


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'admin@test.local')
        await page.fill('#loginPassword', 'admin-pass-123')
        await page.evaluate("""(a) => { window._pendingAem = a; }""", [AEM, 'admin'])
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)
        await page.evaluate("""() => { const a = window._pendingAem; if (a) { document.getElementById('aemUrl').value = a[0]; document.getElementById('username').value = a[1]; document.getElementById('password').value = a[1]; updateSummaryPanel(); } }""")

        hint_on, note_on, got_on = await run(page, 'content/dam/en', True)
        want_on = ['/content/dam/en/f2/a.dita', '/content/dam/en/f2/sub/b.dita', '/content/dam/en/top.dita']
        ok_on = got_on == want_on and 'Not created' in hint_on and 'Not created' in note_on
        print(f"  option ON : hint={hint_on!r}\n              landed={got_on}")
        print('[PASS]' if ok_on else '[FAIL]', 'f1 is not created; its contents go straight into content/dam/en')

        hint_off, _, got_off = await run(page, 'content/dam/en2', False)
        want_off = ['/content/dam/en2/f1/f2/a.dita', '/content/dam/en2/f1/f2/sub/b.dita', '/content/dam/en2/f1/top.dita']
        ok_off = got_off == want_off and 'Creates' in hint_off
        print(f"  option OFF: hint={hint_off!r}\n              landed={got_off}")
        print('[PASS]' if ok_off else '[FAIL]', 'without the option the folder is kept')
        await browser.close()
        sys.exit(0 if ok_on and ok_off else 1)


asyncio.run(main())
