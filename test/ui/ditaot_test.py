"""DITA-OT zips and DTD catalogs for DITA profiles: checks on the selection (browser) and when creating a profile
(server, on the suite's mock AEM). usage: python test/ui/ditaot_test.py <app url> <mock aem url>"""
import asyncio
import base64
import io
import json
import sys
import time
import urllib.request
import zipfile

from playwright.async_api import async_playwright

APP, AEM = sys.argv[1:3]
RUN = str(int(time.time()))
NS = 'urn:oasis:names:tc:entity:xmlns:xml:catalog'
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''), flush=True)


def zip_b64(entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for e in entries:
            z.writestr(e, 'x')
    return base64.b64encode(buf.getvalue()).decode()


GOOD_ZIP = zip_b64(['DITA-OT/plugins/org.dita.base/catalog-dita.xml', 'DITA-OT/bin/dita'])
BAD_ZIP = zip_b64(['dita-ot-3.7.4/plugins/org.dita.base/catalog-dita.xml', 'dita-ot-3.7.4/bin/dita'])
OTHER_ZIP = zip_b64(['images/a.png', 'images/b.png'])
txt = lambda t: base64.b64encode(t.encode()).decode()

# [relative path, base64 body]
SELECT_JS = """(files) => selectNewFiles(files.map(([p, b64]) => ({ file: new File([Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))], p.split('/').pop()), relativePath: p })))"""


def get_json(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={'Authorization': 'Basic YWRtaW46YWRtaW4='})) as r:
        return json.loads(r.read())


async def upload(page, dam, files):
    await page.fill('#damPath', dam)
    await page.evaluate(SELECT_JS, files)
    await page.wait_for_timeout(400)
    await page.evaluate('initiateUpload()')
    await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
    await page.evaluate('handleConfirmOk()')
    await page.wait_for_function('uploading', timeout=15000)
    for _ in range(200):
        if not await page.evaluate('uploading'):
            return
        await asyncio.sleep(0.25)


async def plan(page, body):
    return await page.evaluate("""(b) => fetch('/api/guides/profile/plan', {method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(b)}).then(r => r.json())""", {'baseUrl': AEM, 'username': 'admin', 'password': 'admin', **body})


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1280, 'height': 1000})
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.on('dialog', lambda d: asyncio.ensure_future(d.accept()))
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'qe@test.local')
        await page.fill('#loginPassword', 'qe-pass-12345')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)
        await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'admin';
            document.getElementById('password').value = 'admin'; }""", AEM)

        # ── on the selection, in the browser ──
        dam = f'content/dam/dot{RUN}'
        await page.fill('#damPath', dam)
        good = [['kit/ot/dita-ot.zip', GOOD_ZIP],
                ['kit/dtd/catalog.xml', txt(f'<catalog xmlns="{NS}"><public publicId="-//K//DTD T//EN" uri="t.dtd"/><public publicId="-//K//DTD S//EN" uri="../shared/s.dtd"/></catalog>')],
                ['kit/dtd/t.dtd', txt('<!ELEMENT t ANY>')], ['kit/shared/s.dtd', txt('<!ELEMENT s ANY>')]]
        await page.evaluate(SELECT_JS, good)
        await page.wait_for_function("!document.getElementById('catalogNote').classList.contains('hidden') && document.getElementById('catalogNote').innerText.includes('DITA-OT zip')", timeout=8000)
        note = await page.inner_text('#catalogNote')
        record('a DITA-OT zip in the selection is recognised and its structure checked', 'DITA-OT zip found' in note and 'Its structure is right' in note, note[:300])
        record('the box gives the zip path a DITA profile needs', f'/content/dam/dot{RUN}/kit/ot/dita-ot.zip/jcr:content/renditions/original' in note, '')
        record('a catalog reference leaving its folder is flagged', "Outside the catalog's folder" in note and '../shared/s.dtd' in note, '')

        bad = [['kit/ot/dita-ot.zip', BAD_ZIP]]
        await page.evaluate(SELECT_JS, bad)
        await page.wait_for_function("document.getElementById('catalogNote').innerText.includes('dita-ot-3.7.4')", timeout=8000)
        note = await page.inner_text('#catalogNote')
        record('a zip whose top folder is not DITA-OT is called out', 'named exactly "DITA-OT"' in note and await page.evaluate("document.getElementById('catalogNote').classList.contains('bad')"), note[:200])

        await page.evaluate(SELECT_JS, [['pics/images.zip', OTHER_ZIP]])
        await page.wait_for_timeout(500)
        record('an ordinary zip gets no DITA-OT box', await page.is_hidden('#catalogNote'))

        # ── creating a DITA profile, on the server ──
        await upload(page, dam, good + [['kit/bad/dita-ot-old.zip', BAD_ZIP], ['kit/flat/catalog.xml', txt(f'<catalog xmlns="{NS}"/>')]])
        zip_asset = f'/{dam}/kit/ot/dita-ot.zip'
        cat = f'/{dam}/kit/dtd/catalog.xml'
        shared = f'/{dam}/kit/shared'
        r = await plan(page, {'name': f'OT {RUN}', 'paths': [f'/content/dam/ot{RUN}'], 'catalogs': [f'/{dam}/kit/flat/catalog.xml'], 'ditaotZip': zip_asset})
        record('a DITA-OT zip in DAM gets the original rendition path in the profile', r['ok'] and r['profile']['ditaotZip'] == zip_asset + '/jcr:content/renditions/original'
               and any('is a DAM asset' in n for n in r['notes']), json.dumps(r)[:300])
        record('its contents were read (no "could not be checked" note)', not any('could not be checked' in n for n in r['notes']), json.dumps(r['notes'])[:200])

        r = await plan(page, {'name': f'OTbad {RUN}', 'paths': [f'/content/dam/otb{RUN}'], 'catalogs': [f'/{dam}/kit/flat/catalog.xml'], 'ditaotZip': f'/{dam}/kit/bad/dita-ot-old.zip'})
        record('a DITA-OT zip without a DITA-OT top folder is refused', not r['ok'] and any('named exactly "DITA-OT"' in x for x in r['problems']), json.dumps(r['problems'])[:300])

        r = await plan(page, {'name': f'OTesc {RUN}', 'paths': [f'/content/dam/ote{RUN}'], 'catalogs': [cat]})
        record('a catalog that refers outside its folder is refused', not r['ok'] and any('outside its folder' in x and '../shared/s.dtd' in x for x in r['problems']), json.dumps(r['problems'])[:300])

        r = await plan(page, {'name': f'OTnest {RUN}', 'paths': [f'/content/dam/otn{RUN}'], 'catalogs': [f'/{dam}/kit/flat/catalog.xml', f'/{dam}/kit/flat/catalog.xml'.replace('flat/', 'flat/sub/')]})
        record('two catalogs in nested folders are refused', not r['ok'] and any('same or nested folders' in x for x in r['problems']), json.dumps(r['problems'])[:300])

        r = await plan(page, {'name': f'OTdir {RUN}', 'paths': [f'/content/dam/otd{RUN}'], 'catalogs': [f'/{dam}/kit/dtd']})
        record('the catalog must be the file, not its folder', not r['ok'] and any('.xml file' in x or 'is a folder' in x for x in r['problems']), json.dumps(r['problems'])[:200])

        created = await page.evaluate("""(b) => fetch('/api/guides/profile/create', {method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(b)}).then(r => r.json())""", {'baseUrl': AEM, 'username': 'admin', 'password': 'admin', 'name': f'OT {RUN}',
                                                             'paths': [f'/content/dam/ot{RUN}'], 'catalogs': [f'/{dam}/kit/flat/catalog.xml'], 'ditaotZip': zip_asset})
        saved = next((x for x in get_json(AEM + '/bin/fmdita/profiles?operation=getallprofiles') if x['name'] == f'OT {RUN}'), None)
        record('the created profile stores the original rendition path with overwrite on', bool(created.get('ok')) and saved
               and saved['ditaot']['zip'] == zip_asset + '/jcr:content/renditions/original' and saved['ditaot'].get('overwrite') is True, json.dumps(saved and saved['ditaot'])[:200])
        record('no JavaScript errors', not errors, str(errors[:2]))
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
