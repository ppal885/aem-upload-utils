"""Two users upload at the same time from separate browsers. usage: python test/ui/two_users_test.py <app url> <mock aem url> <files dir>"""
import asyncio
import time
import os
import sys

from playwright.async_api import async_playwright

APP, AEM, FILES_DIR = sys.argv[1:4]
FILES = [os.path.join(FILES_DIR, f) for f in sorted(os.listdir(FILES_DIR)) if f.startswith('t')][:200]
MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"


RUN = str(int(time.time()))
DAM_A, DAM_B = f'content/dam/TwoUsersA{RUN}', f'content/dam/TwoUsersB{RUN}'


async def user_page(browser, name):
    ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
    page = await ctx.new_page()
    await page.goto(APP, wait_until='domcontentloaded')
    await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
    await page.fill('#loginEmail', ('admin@test.local' if name == 'admin' else 'qe@test.local'))
    await page.fill('#loginPassword', ('admin-pass-123' if name == 'admin' else 'qe-pass-12345'))
    await page.evaluate("""(a) => { window._pendingAem = a; }""", [AEM, name])
    await page.click('#loginSubmit')
    await page.wait_for_function('!!currentUser', timeout=15000)
    await page.evaluate("""() => { const a = window._pendingAem; if (a) { document.getElementById('aemUrl').value = a[0]; document.getElementById('username').value = a[1]; document.getElementById('password').value = a[1]; updateSummaryPanel(); } }""")
    return page


async def upload(page, dam):
    await page.fill('#damPath', dam)
    await page.set_input_files('#browserFileInput', FILES)
    await page.evaluate('initiateUpload()')
    await page.wait_for_function(MODAL_OPEN, timeout=10000)
    await page.evaluate('handleConfirmOk()')


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        a = await user_page(browser, 'admin')
        b = await user_page(browser, 'qeuser')
        await asyncio.gather(upload(a, DAM_A), upload(b, DAM_B))
        # both must be running at the same time at some point
        overlap = False
        for _ in range(240):
            if await a.evaluate('uploading && !!currentUploadId') and await b.evaluate('uploading && !!currentUploadId'):
                overlap = True
                break
            await asyncio.sleep(0.25)
        for pg in (a, b):
            for _ in range(600):
                if not await pg.evaluate('uploading'):
                    break
                await asyncio.sleep(0.25)
        ha = await a.evaluate("fetch('/api/upload/history').then(r => r.json())")
        hb = await b.evaluate("fetch('/api/upload/history').then(r => r.json())")
        a_row = next((h for h in ha if h['damPath'] == DAM_A), None)
        b_rows_for_b = [h for h in hb if h['damPath'] in (DAM_A, DAM_B)]
        admin_sees_b = any(h['damPath'] == DAM_B for h in ha)
        ok = (overlap and a_row and a_row['success'] and a_row['totalFiles'] == 200
              and len(b_rows_for_b) == 1 and b_rows_for_b[0]['damPath'] == DAM_B and b_rows_for_b[0]['success']
              and admin_sees_b)
        print(f"  ran at the same time={overlap}")
        print(f"  admin row: {a_row and {k: a_row[k] for k in ('success', 'totalFiles', 'ownerName')}}")
        print(f"  qeuser sees only its own: {[ (h['damPath'], h['ownerName'], h['success']) for h in b_rows_for_b ]}")
        print(f"  admin also sees qeuser's upload: {admin_sees_b}")
        print('[PASS]' if ok else '[FAIL]', 'two users upload concurrently, each sees their own')
        await browser.close()
        sys.exit(0 if ok else 1)


asyncio.run(main())
