"""Keep names exactly + DTD catalog warning. usage: python test/ui/keep_names_test.py <app url> <mock aem url>"""
import asyncio
import io
import json
import sys
import time
import urllib.request
import zipfile

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
RUN = str(int(time.time()))
results = []
NS = 'urn:oasis:names:tc:entity:xmlns:xml:catalog'
DTD_FOLDER = [
    ['wd-dtd/catalog.xml', f'<catalog xmlns="{NS}"><public publicId="-//WD//DTD Topic//EN" uri="wd-1.3/topic.dtd"/>'
                           f'<public publicId="-//WD//DTD Map//EN" uri="base/dtd/map.dtd"/><nextCatalog catalog="plugins/my plugin/catalog.xml"/>'
                           f'<uri name="x" uri="https://example.com/x.dtd"/></catalog>'],
    ['wd-dtd/wd-1.3/topic.dtd', '<!ELEMENT topic ANY>'],
    ['wd-dtd/plugins/my plugin/catalog.xml', f'<catalog xmlns="{NS}"><public publicId="-//WD//DTD Task//EN" uri="task.dtd"/></catalog>'],
    ['wd-dtd/plugins/my plugin/task.dtd', '<!ELEMENT task ANY>'],
]
SELECT_JS = """(files) => selectNewFiles(files.map(([p, body]) => ({ file: new File([body], p.split('/').pop()), relativePath: p })))"""


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''), flush=True)


def completed(prefix):
    with urllib.request.urlopen(AEM + '/__completed') as r:
        return [p for p in json.loads(r.read()) if p.startswith(prefix)]


async def upload(page, dam, files):
    await page.fill('#damPath', dam)
    await page.evaluate(SELECT_JS, files)
    await page.wait_for_timeout(300)
    await page.evaluate('initiateUpload()')
    await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
    note = await page.inner_text('#confirmBody')
    await page.evaluate('handleConfirmOk()')
    await page.wait_for_function('uploading', timeout=15000)
    for _ in range(400):
        if not await page.evaluate('uploading'):
            break
        await asyncio.sleep(0.25)
    return note


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 900})
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.on('dialog', lambda d: asyncio.ensure_future(d.accept()))
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'qe@test.local')
        await page.fill('#loginPassword', 'qe-pass-12345')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'qeuser';
            document.getElementById('password').value = 'qeuser'; }""", AEM)

        record('the option is off by default', not await page.is_checked('#keepNames'))
        await page.evaluate(SELECT_JS, DTD_FOLDER)
        await page.wait_for_function("!document.getElementById('catalogNote').classList.contains('hidden')", timeout=5000)
        note = await page.inner_text('#catalogNote')
        record('warning names the catalog path that would break', 'wd-1.3/topic.dtd' in note and 'wd-1-3' in note and 'would break' in note, note[:300])
        record('warning also catches a renamed folder in nextCatalog', 'my plugin' in note and 'my-plugin' in note, '')
        record('warning lists a referenced file missing from the selection', 'Not in your selection' in note and 'base/dtd/map.dtd' in note, '')
        record('links to the web are not flagged', 'example.com' not in note, '')
        record('the warning also offers the DITA profile steps', 'How to set up the DTD in AEM' in note, '')

        await page.click('#catalogNote button')
        record('the button turns on Keep names exactly', await page.is_checked('#keepNames'))
        note = await page.inner_text('#catalogNote')
        record('with the option on only the missing file is left', 'would break' not in note and 'base/dtd/map.dtd' in note, note[:200])
        summary = await page.inner_text('#sumOptions')
        record('summary shows the option', 'Names kept exactly' in summary, summary)

        dam = f'content/dam/kn{RUN}'
        await upload(page, dam, DTD_FOLDER)
        got = sorted(completed('/' + dam + '/'))
        record('upload keeps dots and spaces in names', f'/{dam}/wd-dtd/wd-1.3/topic.dtd' in got and f'/{dam}/wd-dtd/plugins/my plugin/task.dtd' in got, str(got))

        await page.evaluate("setKeepNames(false)")
        dam2 = f'content/dam/kn{RUN}b'
        confirm_note = await upload(page, dam2, DTD_FOLDER)
        record('without the option the confirm dialog warns about the catalog', 'DTD catalog paths would break' in confirm_note, confirm_note[:300])
        got2 = sorted(completed('/' + dam2 + '/'))
        record('without the option names are cleaned as before', f'/{dam2}/wd-dtd/wd-1-3/topic.dtd' in got2, str(got2))

        other = [['docs/v1.2/a.dita', '<topic id="a"/>']]
        await page.evaluate(SELECT_JS, other)
        await page.wait_for_timeout(300)
        record('no warning for a selection without catalogs', await page.is_hidden('#catalogNote'))

        clean = [['crown-dtd/catalog.xml', f'<catalog xmlns="{NS}"><public publicId="-//C//DTD T//EN" uri="topic.dtd"/></catalog>'],
                 ['crown-dtd/topic.dtd', '<!ELEMENT topic ANY>']]
        await page.fill('#damPath', 'content/dam/dtds')
        await page.evaluate(SELECT_JS, clean)
        await page.wait_for_function("!document.getElementById('catalogNote').classList.contains('hidden')", timeout=5000)
        info = await page.inner_text('#catalogNote')
        record('a clean DTD catalog gets an info box with where it lands', 'DTD catalog found' in info and '/content/dam/dtds/crown-dtd/catalog.xml' in info, info[:200])
        await page.click('#catalogNote summary')
        steps = await page.inner_text('#catalogNote')
        record('the steps name the real DITA Profiles labels', all(x in steps for x in ('Create Profile', 'Assigned Paths', 'Catalogs', 'Done', '/content/dam/dtds/crown-dtd/catalog.xml')), steps[-400:])
        link = await page.get_attribute('#catalogNote a', 'href')
        record('the steps link to DITA Profiles on the chosen AEM server', link == AEM + '/libs/fmdita/profiles/profiles.html', str(link))
        await page.fill('#damPath', 'content/dam/other')
        await page.dispatch_event('#damPath', 'input')
        info2 = await page.inner_text('#catalogNote')
        record('the path follows the DAM folder and the opened steps stay open', '/content/dam/other/crown-dtd/catalog.xml' in info2
               and await page.evaluate("!!document.querySelector('#catalogNote details[open]')"), info2[:160])

        # Package with the option: names inside the zip stay as they are
        await page.evaluate("setKeepNames(true)")
        await page.evaluate("setWorkMode('package')")
        await page.fill('#pkgName', f'kn-{RUN}')
        await page.fill('#damPath', f'content/dam/knpkg{RUN}')
        await page.evaluate(SELECT_JS, DTD_FOLDER)
        await page.wait_for_timeout(300)
        await page.evaluate('buildPackageFromSelection()')
        await page.wait_for_function("!document.querySelector('[data-view-panel=\"packages\"]').classList.contains('hidden')", timeout=30000)
        pk = json.loads(await page.evaluate("fetch('/api/packages').then(r => r.text())"))['packages'][0]
        body = await (await page.request.get(f"{APP}/api/packages/{pk['id']}/download")).body()
        names = zipfile.ZipFile(io.BytesIO(body)).namelist()
        record('package keeps the names too', any('/wd-dtd/wd-1.3/topic.dtd/' in n for n in names) and any('/my plugin/task.dtd/' in n for n in names),
               str([n for n in names if 'topic.dtd' in n or 'task.dtd' in n][:4]))
        record('no JavaScript errors', not errors, str(errors[:2]))
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
