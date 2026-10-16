"""Build package from the browser, download, install. usage: python test/ui/package_test.py <app url> <mock aem url>"""
import asyncio
import io
import json
import sys
import time
import urllib.request
import zipfile

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
SELECT_JS = """(paths) => { selectNewFiles(paths.map((p) => ({ file: new File(['<topic id="x"/>' + p], p.split('/').pop()), relativePath: p }))); }"""
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ('' if ok else f'\n        {detail}'))


async def main():
    run = str(int(time.time()))
    paths = ['proj/docs/My Folder/a#b.dita', 'proj/docs/My Folder/intro.dita', 'proj/docs/v1.2/map.ditamap',
             'proj/docs/images/logo.png', 'proj/docs/.DS_Store', 'proj/docs/TestFile1.dita']
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={'width': 1400, 'height': 1000}, accept_downloads=True)
        page = await ctx.new_page()
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'qe@test.local')
        await page.fill('#loginPassword', 'qe-pass-12345')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)

        await page.fill('#damPath', f'content/dam/qa{run}/en')
        await page.evaluate(SELECT_JS, paths)
        await page.select_option('#skipLevels', '1')
        await page.click('#modePackage')
        name = await page.input_value('#pkgName')
        btn = await page.evaluate("document.getElementById('uploadBtnText').textContent")
        record('Build package mode: name suggested from the folder, button renamed', name == 'docs' and btn == 'Build package', f'name={name!r} btn={btn!r}')
        await page.fill('#pkgName', f'qa-pkg-{run}')
        await page.fill('#pkgVersion', '2.1')
        # AEM fields are empty: building must not need them
        await page.evaluate("['aemUrl','username','password'].forEach(id => document.getElementById(id).value = '')")
        await page.evaluate('initiateUpload()')
        await page.wait_for_function("!document.getElementById('viewPackages').classList.contains('hidden')", timeout=60000)
        await page.wait_for_function("document.getElementById('pkgBody').innerText.includes('Ready')", timeout=30000)
        row = await page.inner_text('#pkgBody tr')
        record('package builds without AEM sign-in and is listed as Ready', f'qa-pkg-{run}-2.1.zip' in row and '5 files' in row, row)

        async with page.expect_download() as dl:
            await page.click('#pkgBody tr >> text=Download')
        d = await dl.value
        data = open(await d.path(), 'rb').read()
        z = zipfile.ZipFile(io.BytesIO(data))
        names = z.namelist()
        base = f'jcr_root/content/dam/qa{run}/en/'
        filt = z.read('META-INF/vault/filter.xml').decode()
        props = z.read('META-INF/vault/properties.xml').decode()
        record('download is a valid zip named like the package', d.suggested_filename == f'qa-pkg-{run}-2.1.zip' and z.testzip() is None, d.suggested_filename)
        record('skip level applied: "proj" not in the package, "docs" is', any(n.startswith(base + 'docs/') for n in names) and not any('/proj/' in n for n in names),
               [n for n in names if n.startswith('jcr_root')][:6])
        for expect in ['docs/My-Folder/a-b.dita/_jcr_content/renditions/original', 'docs/v1-2/map.ditamap/.content.xml',
                       'docs/TestFile1.dita/_jcr_content/renditions/original', 'docs/images/logo.png/.content.xml']:
            record(f'contains {expect}', base + expect in names)
        record('system file left out of the package', not any('DS_Store' in n for n in names))
        title = z.read(base + 'docs/My-Folder/.content.xml').decode()
        record('renamed folder keeps its original name as title', 'jcr:title="My Folder"' in title, title)
        record('filter.xml only covers the packaged folder, mode=update', f'root="/content/dam/qa{run}/en/docs" mode="update"' in filt and filt.count('<filter ') == 1, filt)
        record('properties.xml has name, version, group', f'<entry key="name">qa-pkg-{run}</entry>' in props and '<entry key="version">2.1</entry>' in props
               and '<entry key="group">guides-upload</entry>' in props, props[:300])
        orig = z.read(base + 'docs/My Folder/intro.dita'.replace('My Folder', 'My-Folder') + '/_jcr_content/renditions/original').decode()
        record('file content is byte-identical', orig == '<topic id="x"/>proj/docs/My Folder/intro.dita', orig)

        # install on the mock AEM
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'qeuser';
            document.getElementById('password').value = 'qeuser'; }""", AEM)
        await page.click('#pkgBody tr >> text=Install on AEM')
        await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')", timeout=5000)
        await page.evaluate('handleConfirmOk()')
        await page.wait_for_function("document.getElementById('pkgBody').innerText.includes('Installed on')", timeout=30000)
        with urllib.request.urlopen(AEM + '/__installs') as r:
            installs = json.loads(r.read())
        last = installs[-1] if installs else {}
        record('Install on AEM sends the zip to Package Manager with install=true', last.get('name') == f'qa-pkg-{run}-2.1.zip' and last.get('install') and last.get('who') == 'qeuser', last)

        # privacy: another member cannot see or download it
        pkg_id = await page.evaluate("fetch('/api/packages').then(r => r.json()).then(j => j.packages[0].id)")
        ctx2 = await browser.new_context()
        p2 = await ctx2.new_page()
        await p2.goto(APP, wait_until='domcontentloaded')
        await p2.wait_for_function("typeof submitSignup === 'function'", timeout=60000)
        other = f'qa.other{run}@adobe.com'
        await p2.evaluate("""(e) => fetch('/api/auth/register', { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: 'Other', email: e, password: 'other-password-1' }) })""", other)
        seen = await p2.evaluate("fetch('/api/packages').then(r => r.json()).then(j => j.packages.length)")
        dl_status = await p2.evaluate(f"fetch('/api/packages/{pkg_id}/download').then(r => r.status)")
        record("another member cannot see or download someone else's package", seen == 0 and dl_status == 404, f'seen={seen}, download={dl_status}')
        record('no JavaScript errors', not errors, errors)
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
