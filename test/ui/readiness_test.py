"""Check in AEM + Create DITA profile. A seeded mock AEM (4622) plays a server with processed / waiting / failed files;
the suite's mock (4620) is the allowed server for creating profiles. usage: python test/ui/readiness_test.py <app url> <mock aem url>"""
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, ALLOWED = sys.argv[1:3]
OTHER = 'http://127.0.0.1:4622'
HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get('TEST_WORK_DIR', HERE)
RUN = str(int(time.time()))
results = []
NS = 'urn:oasis:names:tc:entity:xmlns:xml:catalog'
G = lambda n: 'GUID-dddddddd-0000-4000-8000-%012d' % n
PUB = '-//READY//DTD Topic//EN'
SEED = {
    'assets': {
        '/content/dam/ready/guide.ditamap': {'guid': G(10), 'guidesState': 'SUCCESS',
            'text': f'<map><topicref href="{G(1)}.dita"/><topicref href="{G(2)}.dita"/><topicref href="{G(99)}.dita"/></map>'},
        '/content/dam/ready/t1.dita': {'guid': G(1), 'guidesState': 'SUCCESS', 'text': f'<!DOCTYPE topic PUBLIC "{PUB}" "topic.dtd"><topic id="t1"/>'},
        '/content/dam/ready/t2.dita': {'guid': G(2), 'text': '<topic id="t2"/>'},
        '/content/dam/ready/t3.dita': {'guid': G(3), 'guidesState': 'FAILED', 'text': '<topic id="t3"/>'},
        '/content/dam/ready/logo.png': {'base64': 'iVBORw0KGgo='},
        '/content/dam/ready-dtd/catalog.xml': {'text': f'<catalog xmlns="{NS}"><public publicId="{PUB}" uri="topic.dtd"/></catalog>'},
    },
    'profiles': [
        {'id': 'b1aad0a7-9079-e56c-1ed8-6fcababe8166', 'name': 'Default Profile', 'paths': ['/content/dam'],
         'schema': {'catalogs': ['/libs/fmdita/dita_resources/DITA-1.3/dtd/catalog.xml']}, 'ditaot': {'zip': '/libs/fmdita/dita_resources/DITA-OT.zip', 'catalogs': []}},
        {'id': 'r-1', 'name': 'Ready', 'paths': ['/content/dam/ready'],
         'schema': {'catalogs': ['/libs/fmdita/dita_resources/DITA-1.3/dtd/catalog.xml', '/content/dam/ready-dtd/catalog.xml']},
         'ditaot': {'zip': '/libs/fmdita/dita_resources/DITA-OT.zip', 'catalogs': []}},
    ],
}
SELECT_JS = """(files) => selectNewFiles(files.map(([p, body]) => ({ file: new File([body], p.split('/').pop()), relativePath: p })))"""


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''), flush=True)


def get_json(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={'Authorization': 'Basic YWRtaW46YWRtaW4='})) as r:
        return json.loads(r.read())


def start_other():
    seed = os.path.join(WORK, 'readiness-seed.json')
    with open(seed, 'w', encoding='utf-8') as f:
        json.dump(SEED, f)
    log = open(os.path.join(WORK, 'readiness-aem.log'), 'w')
    proc = subprocess.Popen(['node', os.path.join(os.path.dirname(HERE), 'mock-aem.js'), '4622', seed], stdout=log, stderr=log)
    for _ in range(40):
        try:
            urllib.request.urlopen(OTHER + '/__completed')
            return proc
        except Exception:
            time.sleep(0.25)
    raise RuntimeError('mock did not start')


async def dialog_text(page):
    return await page.inner_text('#aemDialog')


async def main():
    other = start_other()
    try:
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

            # Check in AEM on a server that is not on New upload: asks for the credentials
            await page.evaluate("(u) => openAemStatus({ baseUrl: u, folder: '/content/dam/ready' })", OTHER)
            record('asks for credentials of another server', await page.is_visible('#adUser'))
            await page.fill('#adUser', 'admin')
            await page.fill('#adPass', 'admin')
            await page.click('#adCheckBtn')
            await page.wait_for_function("document.getElementById('adResult').innerText.includes('AEM Guides processed')", timeout=20000)
            t = await dialog_text(page)
            record('counts processed DITA files (2 of 4)', '2 / 4' in t, t[:400])
            record('lists the failed file', 'Failed in AEM Guides' in t and '/content/dam/ready/t3.dita' in t, '')
            record('verdict: failed files are called out', '1 file(s) failed in AEM Guides and will not open properly' in t, '')
            record('links: 2 ok, 1 outside the folder', '2 ok, 1 outside the folder' in t, '')
            record('editor DTD check passes with the Ready profile', '1 / 1 topics checked' in t, '')
            record('AEM processing counted (none yet)', '0 / 5' in t, '')

            # Reprocess through the API (the button is shown for AEM as a Cloud Service only)
            rp = await page.evaluate("""(u) => fetch('/api/guides/reprocess', {method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({baseUrl: u, username: 'admin', password: 'admin', folder: '/content/dam/ready'})}).then(r => r.status)""", OTHER)
            record('Reprocess Assets starts AEM\'s reprocess on the folder', rp == 200 and get_json(OTHER + '/__reprocessed') == ['/content/dam/ready'], str(rp))
            await page.click('#adCheckBtn')
            await page.wait_for_function("document.getElementById('adResult').innerText.includes('5 / 5')", timeout=20000)
            record('after reprocessing AEM processing is complete', True)
            await page.evaluate('closeAemDialog()')

            bad = await page.evaluate("""() => fetch('/api/guides/status', {method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({baseUrl: 'http://127.0.0.1:4622', username: 'admin', password: 'admin', folder: '/etc/passwd'})}).then(r => r.status)""")
            record('only folders inside /content/dam can be checked', bad == 400, str(bad))

            # Create DITA profile: refused on a server that is not allowed
            await page.evaluate("(u) => openProfileDialog({ baseUrl: u, creds: { username: 'admin', password: 'admin' }, values: { name: 'Nope', path: '/content/dam/ready', catalogs: ['/content/dam/ready-dtd/catalog.xml'] } })", OTHER)
            await page.click('#pfCheckBtn')
            await page.wait_for_function("document.getElementById('pfResult').innerText.length > 20", timeout=10000)
            t = await page.inner_text('#pfResult')
            record('creating a profile on a server that is not allowed is refused', 'allowed servers' in t, t[:200])
            await page.evaluate('closeAemDialog()')

            # Upload a DTD folder to the allowed server, then create the profile from the DTD box
            await page.evaluate("""(a) => { document.getElementById('aemUrl').value = a; document.getElementById('username').value = 'admin';
                document.getElementById('password').value = 'admin'; }""", ALLOWED)
            await page.fill('#damPath', f'content/dam/rd{RUN}')
            dtd = [['acme-dtd/catalog.xml', f'<catalog xmlns="{NS}"><public publicId="-//ACME//DTD T//EN" uri="t.dtd"/></catalog>'], ['acme-dtd/t.dtd', '<!ELEMENT t ANY>']]
            await page.evaluate(SELECT_JS, dtd)
            await page.wait_for_function("!document.getElementById('catalogNote').classList.contains('hidden')", timeout=5000)
            await page.evaluate('initiateUpload()')
            await page.wait_for_function("!document.getElementById('confirmModal').classList.contains('hidden')")
            await page.evaluate('handleConfirmOk()')
            await page.wait_for_function('uploading', timeout=15000)
            for _ in range(200):
                if not await page.evaluate('uploading'):
                    break
                await asyncio.sleep(0.25)
            catalog = f'/content/dam/rd{RUN}/acme-dtd/catalog.xml'
            await page.evaluate(SELECT_JS, dtd)
            await page.wait_for_function("!document.getElementById('catalogNote').classList.contains('hidden')", timeout=5000)
            await page.click('#catalogNote >> text=Create DITA profile')
            record('the profile dialog is filled with the uploaded catalog', await page.input_value('#pfCatalogs') == catalog, await page.input_value('#pfCatalogs'))
            await page.fill('#pfName', f'Acme {RUN}')
            await page.fill('#pfPath', f'/content/dam/acme{RUN}')
            await page.click('#pfCheckBtn')
            await page.wait_for_selector('#pfCreateBtn', timeout=10000)
            t = await page.inner_text('#pfResult')
            record('the check shows what changes before creating', 'Ready to create' in t and f'/content/dam/acme{RUN}' in t and 'Default Profile' in t, t[:300])
            await page.click('#pfCreateBtn')
            await page.wait_for_function("document.getElementById('aemDialog').classList.contains('hidden')", timeout=20000)
            profs = get_json(ALLOWED + '/bin/fmdita/profiles?operation=getallprofiles')
            mine = next((x for x in profs if x['name'] == f'Acme {RUN}'), None)
            record('the profile is created with the path and the catalog, built-in catalogs kept',
                   bool(mine) and mine['paths'] == [f'/content/dam/acme{RUN}'] and catalog in mine['schema']['catalogs']
                   and any(c.startswith('/libs/') for c in mine['schema']['catalogs']), json.dumps(mine)[:300])
            record('existing profiles are not changed', next(x for x in profs if x['name'] == 'Default Profile')['paths'] == ['/content/dam'], '')

            dup = await page.evaluate(f"""() => fetch('/api/guides/profile/plan', {{method: 'POST', headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{baseUrl: {json.dumps(ALLOWED)}, username: 'admin', password: 'admin', name: 'Acme {RUN}', paths: ['/content/dam/x'], catalogs: ['/content/dam/missing/catalog.xml']}})}}).then(r => r.json())""")
            record('same name and a missing catalog are refused', not dup['ok'] and any('already exists' in x for x in dup['problems'])
                   and any('not on this server yet' in x for x in dup['problems']), json.dumps(dup['problems']))
            moved = await page.evaluate(f"""() => fetch('/api/guides/profile/plan', {{method: 'POST', headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{baseUrl: {json.dumps(ALLOWED)}, username: 'admin', password: 'admin', name: 'Acme2 {RUN}', paths: ['/content/dam/acme{RUN}'], catalogs: ['{catalog}']}})}}).then(r => r.json())""")
            record('moving a folder out of another profile is shown first', moved['ok'] and any(f'is in profile "Acme {RUN}" now and moves' in x for x in moved['notes']), json.dumps(moved['notes']))

            # History: Check in AEM opens on the folder that was uploaded
            await page.evaluate("showView('history')")
            await page.wait_for_function("document.getElementById('historyBody').innerText.includes('Check in AEM')", timeout=10000)
            await page.click('#historyBody >> text=Check in AEM')
            await page.wait_for_function("!document.getElementById('aemDialog').classList.contains('hidden')", timeout=5000)
            folder = await page.input_value('#adFolder')
            record('History opens the check on the uploaded folder', folder == f'/content/dam/rd{RUN}/acme-dtd', folder)
            await page.wait_for_function("document.getElementById('adResult').innerText.includes('AEM processing')", timeout=20000)
            t = await page.inner_text('#adResult')
            record('same server as New upload: no credentials asked, check runs', '2 / 2' in t and not await page.is_visible('#adUser'), t[:200])
            await page.evaluate('closeAemDialog()')
            await page.evaluate("""() => { window._historyData.unshift({ uploadId: 'wide', timestamp: new Date().toISOString(), success: true, totalFiles: 3966, totalBytes: 38000000, duration: 90000,
                baseUrl: 'https://author-p38855-e367364-cmstg.adobeaemcloud.com', damPath: 'content/dam/qe-copy-pkg/very-long-folder-name-for-layout-check',
                localPath: 'Copied from author-p38855-e367364-cmstg.adobeaemcloud.com/content/dam/qe-reprocess-uuid/run-20261017085229 (3966 files, 36 MB)', ownerName: 'QE User' });
                renderFilteredHistory(); }""")
            await page.set_viewport_size({'width': 1280, 'height': 900})
            await page.wait_for_timeout(300)
            fit = await page.evaluate("""() => { const sc = document.querySelector('#histPanel .overflow-x-auto'); const p = document.getElementById('histPanel');
                return { content: sc.scrollWidth, visible: sc.clientWidth, search: document.getElementById('histSearch').getBoundingClientRect().left >= p.getBoundingClientRect().left }; }""")
            record('History fits a 1280px screen without sideways scrolling, search fully visible', fit['content'] <= fit['visible'] + 2 and fit['search'], str(fit))
            await page.evaluate("noteServerVersion('first'); noteServerVersion('second')")
            record('a newer server version shows the refresh notice', await page.is_visible('#newVersionNote') and 'new version' in (await page.inner_text('#newVersionNote')).lower(), '')
            record('no JavaScript errors', not errors, str(errors[:2]))
            await browser.close()
    finally:
        other.kill()
        other.wait()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
