"""Copy from AEM: a second mock AEM is the source (port 4621), the suite's mock (4620) the only allowed target.
usage: python test/ui/copy_test.py <app url> <target mock aem url>"""
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request

from playwright.async_api import async_playwright

APP, TARGET = sys.argv[1:3]
SOURCE = 'http://127.0.0.1:4621'
HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.environ.get('TEST_WORK_DIR', HERE)
results = []

G = lambda n: 'GUID-aaaaaaaa-0000-4000-8000-%012d' % n
OUTSIDE = 'GUID-bbbbbbbb-0000-4000-8000-000000000009'
MISSING = 'GUID-cccccccc-0000-4000-8000-0000000000ff'
CROWN_TOPIC = '-//CROWN//DTD Crown Topic//EN'
CROWN_MAP = '-//CROWN//DTD Crown Map//EN'
SEED = {
    'assets': {
        '/content/dam/crown/guide.ditamap': {'guid': G(10), 'text':
            f'<?xml version="1.0"?>\n<!DOCTYPE map PUBLIC "{CROWN_MAP}" "crownmap.dtd">\n<map><title>Crown</title>'
            f'<topicref href="{G(1)}.dita"/><topicref href="{G(2)}.dita#t2"/><topicref href="{MISSING}.dita"/>'
            f'<topicref href="{OUTSIDE}.dita"/><topicref href="https://example.com/x" format="html" scope="external"/></map>\n'},
        '/content/dam/crown/v1.2/intro.dita': {'guid': G(1), 'text':
            f'<?xml version="1.0"?>\n<!DOCTYPE topic PUBLIC "{CROWN_TOPIC}" "crowntopic.dtd">\n<topic id="intro"><title>Intro</title>'
            f'<body><p conref="{G(2)}.dita#t2/p1"/><image href="{G(3)}.png"/></body></topic>\n'},
        '/content/dam/crown/v1.2/My Topic.dita': {'guid': G(2), 'staleSha1': True, 'text':
            f'<?xml version="1.0"?>\n<!DOCTYPE topic PUBLIC "{CROWN_TOPIC}" "crowntopic.dtd">\n<topic id="t2"><title>Two</title><body><p id="p1">Reused</p></body></topic>\n'},
        '/content/dam/crown/images/logo.png': {'guid': G(3), 'base64': 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGBgAAAABQABpfZFQAAAAABJRU5ErkJggg=='},
        '/content/dam/shared/common.dita': {'guid': OUTSIDE, 'text': '<topic id="c"><title>C</title></topic>'},
        '/content/dam/crown-dtd/catalog.xml': {'text': f'<catalog><public publicId="{CROWN_TOPIC}" uri="crowntopic.dtd"/><nextCatalog catalog="maps/catalog.xml"/></catalog>'},
        '/content/dam/crown-dtd/crowntopic.dtd': {'text': '<!ELEMENT topic ANY>'},
        '/content/dam/crown-dtd/maps/catalog.xml': {'text': f'<catalog><public publicId="{CROWN_MAP}" uri="crownmap.dtd"/></catalog>'},
    },
    'profiles': [
        {'id': 'b1aad0a7-9079-e56c-1ed8-6fcababe8166', 'name': 'Default Profile', 'paths': ['/content/dam'],
         'schema': {'catalogs': ['/libs/fmdita/dita_resources/DITA-1.3/dtd/catalog.xml']}, 'ditaot': {'zip': '/libs/fmdita/dita_resources/DITA-OT.zip', 'catalogs': []}},
        {'id': 'crown-1', 'name': 'Crown', 'paths': ['/content/dam/crown'],
         'schema': {'catalogs': ['/libs/fmdita/dita_resources/DITA-1.3/dtd/catalog.xml', '/content/dam/crown-dtd/catalog.xml']},
         'ditaot': {'zip': '/content/dam/crown-ot/dita-ot.zip', 'catalogs': []}},
    ],
}


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f' — {detail}' if detail else ''), flush=True)


def get(url, auth=False):
    req = urllib.request.Request(url, headers={'Authorization': 'Basic YWRtaW46YWRtaW4='} if auth else {})
    with urllib.request.urlopen(req) as r:
        return r.read()


def completed(prefix):
    return [p for p in json.loads(get(TARGET + '/__completed')) if p.startswith(prefix)]


def wait_completed(prefix, n, timeout=90):
    for _ in range(timeout * 2):
        if len(set(completed(prefix))) >= n:
            return True
        time.sleep(0.5)
    return False


def start_source():
    seed = os.path.join(WORK, 'copy-source-seed.json')
    with open(seed, 'w', encoding='utf-8') as f:
        json.dump(SEED, f)
    log = open(os.path.join(WORK, 'copy-source-aem.log'), 'w')
    proc = subprocess.Popen(['node', os.path.join(os.path.dirname(HERE), 'mock-aem.js'), '4621', seed], stdout=log, stderr=log)
    for _ in range(40):
        try:
            get(SOURCE + '/__completed')
            return proc
        except Exception:
            time.sleep(0.25)
    raise RuntimeError('source mock did not start')


async def signed_in(browser):
    page = await browser.new_page(viewport={'width': 1280, 'height': 1000})
    await page.goto(APP + '/#copy', wait_until='domcontentloaded')
    await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
    await page.fill('#loginEmail', 'qe@test.local')
    await page.fill('#loginPassword', 'qe-pass-12345')
    await page.click('#loginSubmit')
    await page.wait_for_function('!!currentUser', timeout=15000)
    await page.evaluate("showView('copy')")
    await page.wait_for_function("document.getElementById('cpTgtUrl').options.length > 0", timeout=10000)
    return page


async def copy(page, folder, mode='upload', target_dam=None):
    await page.evaluate(f"cpSetMode('{mode}')")
    await page.fill('#cpSrcUrl', SOURCE)
    await page.fill('#cpSrcUser', 'admin')
    await page.fill('#cpSrcPass', 'admin')
    await page.fill('#cpFolder', folder)
    await page.dispatch_event('#cpFolder', 'input')
    if target_dam is not None:
        await page.fill('#cpTgtDam', target_dam)
        await page.dispatch_event('#cpTgtDam', 'input')
    if mode == 'upload':
        await page.fill('#cpTgtUser', 'admin')
        await page.fill('#cpTgtPass', 'admin')
    await page.click('#cpStartBtn')
    await page.wait_for_function("cpLast && ['done', 'failed', 'cancelled'].includes(cpLast.state)", timeout=60000)
    return await page.evaluate('cpLast')


async def main():
    source = start_source()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await signed_in(browser)
            options = await page.evaluate("[...document.getElementById('cpTgtUrl').options].map(o => o.value)")
            record('only the allowed target server is offered', options == [TARGET], str(options))
            await page.fill('#cpFolder', '/content/dam/crown')
            await page.dispatch_event('#cpFolder', 'input')
            land = await page.inner_text('#cpLandNote')
            record('the target folder defaults to the same path as on the source', '/content/dam/crown' in land and 'recommended' in land, land)
            await page.fill('#cpTgtDam', '')
            await page.type('#cpTgtDam', 'content/dam/elsewhere')
            typed = await page.input_value('#cpTgtDam')
            record('a target folder typed by the user is kept (not overwritten by the default)', typed == 'content/dam/elsewhere', typed)
            await page.fill('#cpTgtDam', '')
            await page.dispatch_event('#cpTgtDam', 'input')
            await page.fill('#cpFolder', '/content/dam/crown')
            await page.dispatch_event('#cpFolder', 'input')

            c = await copy(page, '/content/dam/crown')
            d = c.get('dita') or {}
            setup = c.get('setup') or {}
            record('all 4 files copied, none failed', c['state'] == 'done' and c['done'] == 4 and c['failedCount'] == 0, json.dumps({k: c.get(k) for k in ('state', 'done', 'failedCount', 'verified', 'unverified')}))
            record('a file whose AEM SHA-1 is older than its content is still copied (checked by size)', c['verified'] == 3 and c['unverified'] == 1, f"verified={c['verified']} unverified={c['unverified']}")
            record('GUID links turned into paths, the missing GUID reported', d.get('rewrittenLinks') == 5 and d.get('unresolvedCount') == 1, json.dumps({k: d.get(k) for k in ('rewrittenLinks', 'rewrittenFiles', 'unresolvedCount')}))
            steps = ' '.join(setup.get('steps') or [])
            record('not ready: the target cannot resolve the CROWN DTDs yet', setup.get('ready') is False and CROWN_TOPIC in steps and 'DITA Profiles' in steps, steps[:300])
            record('setup check names the separate DTD folder and the linked folder outside', 'Copy /content/dam/crown-dtd too' in steps and '/content/dam/shared' in steps, steps[:300])
            record('setup check sees the custom DITA-OT of the source profile', 'crown-ot/dita-ot.zip' in steps, '')
            help_html = await page.inner_html('#cpResult')
            record('not ready: the DITA profile steps link to the target server', 'How to set up the DTD in AEM' in help_html and TARGET + '/libs/fmdita/profiles/profiles.html' in help_html and '/content/dam/crown-dtd/catalog.xml' in help_html, '')
            go = await page.inner_text('#cpGoBtn')
            record('the button warns instead of offering a plain upload', go == 'Upload anyway', go)
            record('nothing was written on the source server', json.loads(get(SOURCE + '/__writes')) == [], get(SOURCE + '/__writes').decode()[:200])

            # Set the target up like an admin would: copy the DTD folder to the same path, then save a DITA profile
            page2 = await signed_in(browser)
            c2 = await copy(page2, '/content/dam/crown-dtd')
            await page2.click('#cpGoBtn')
            record('the DTD folder is copied to the same path on the target', wait_completed('/content/dam/crown-dtd/', 3), str(completed('/content/dam/crown-dtd/')))
            # Create the DITA profile from the copy result (filled in from the source's "Crown" profile)
            await page.click('#cpProfileBtn')
            await page.wait_for_selector('#pfName', timeout=5000)
            record('profile dialog is filled from the source profile', await page.input_value('#pfName') == 'Crown (copy)'
                   and await page.input_value('#pfPath') == '/content/dam/crown' and await page.input_value('#pfCatalogs') == '/content/dam/crown-dtd/catalog.xml'
                   and await page.input_value('#pfZip') == '/content/dam/crown-ot/dita-ot.zip', '')
            await page.click('#pfCheckBtn')
            await page.wait_for_function("document.getElementById('pfResult').innerText.length > 10", timeout=10000)
            t = await page.inner_text('#pfResult')
            record('a DITA-OT zip that is not on the target blocks creating', 'Not possible yet' in t and 'dita-ot.zip is not on this server yet' in t, t[:200])
            await page.fill('#pfZip', '')
            await page.click('#pfCheckBtn')
            await page.wait_for_selector('#pfCreateBtn', timeout=10000)
            await page.click('#pfCreateBtn')
            await page.wait_for_function("cpLast.setup && cpLast.setup.ready === true", timeout=20000)
            record('after creating the profile, Check again says ready by itself', True)
            go = await page.inner_text('#cpGoBtn')
            record('Check again: ready once the DTDs and the profile are on the target', go.startswith('Upload to'), go)

            await page.click('#cpGoBtn')
            ok = wait_completed('/content/dam/crown/', 4)
            names = sorted(completed('/content/dam/crown/'))
            record('uploaded with the exact node names (v1.2 stays v1.2, spaces kept)', ok and '/content/dam/crown/v1.2/My Topic.dita' in names, str(names))
            ditamap = get(TARGET + '/__asset/content/dam/crown/guide.ditamap').decode()
            intro = get(TARGET + '/__asset/content/dam/crown/v1.2/intro.dita').decode()
            record('map links are relative paths on the target', 'href="v1.2/intro.dita"' in ditamap and 'href="v1.2/My%20Topic.dita#t2"' in ditamap
                   and 'href="../shared/common.dita"' in ditamap and f'href="{MISSING}.dita"' in ditamap and 'https://example.com/x' in ditamap, ditamap[:400])
            record('conref and image links too', 'conref="My%20Topic.dita#t2/p1"' in intro and 'href="../images/logo.png"' in intro, intro[:300])
            await page.wait_for_function("document.querySelector('[data-view-panel=\"upload\"]') && !document.querySelector('[data-view-panel=\"upload\"]').classList.contains('hidden')", timeout=10000)
            for _ in range(60):
                hist = json.loads(await page.evaluate("fetch('/api/upload/history').then(r => r.text())"))
                rows = hist if isinstance(hist, list) else hist.get('history', [])
                row = next((r for r in rows if (r.get('localPath') or '').startswith('Copied from 127.0.0.1:4621/content/dam/crown ')), None)
                if row:
                    break
                await asyncio.sleep(0.5)
            record('History records where the copy came from', bool(row and row.get('success')), json.dumps(row)[:200] if row else 'no row')

            # Package of copied content: only installable on an allowed server, never downloadable
            page3 = await signed_in(browser)
            await copy(page3, '/content/dam/crown', mode='package', target_dam='content/dam')
            await page3.click('#cpGoBtn')
            await page3.wait_for_function("!document.querySelector('[data-view-panel=\"packages\"]').classList.contains('hidden')", timeout=30000)
            pk = json.loads(await page3.evaluate("fetch('/api/packages').then(r => r.text())"))['packages'][0]
            record('package remembers it holds copied content', pk.get('copiedFrom', {}).get('folder') == '/content/dam/crown' and pk['state'] == 'ready', json.dumps(pk.get('copiedFrom')))
            record('package filter covers only the copied folder, never all of /content/dam', pk.get('filterRoots') == ['/content/dam/crown'], str(pk.get('filterRoots')))
            dl = await page3.evaluate(f"fetch('/api/packages/{pk['id']}/download').then(r => r.status)")
            record('a package of copied content cannot be downloaded', dl == 403, str(dl))
            await page3.wait_for_function("document.getElementById('pkgBody').innerText.includes('Copied from')", timeout=10000)
            row_html = await page3.evaluate("[...document.querySelectorAll('#pkgBody tr')].map(r => r.innerHTML).find(h => h.includes('Copied from')) || ''")
            record('Packages page shows the source and no Download link', 'Copied from' in row_html and '/download' not in row_html, row_html[:200])
            bad = await page3.evaluate(f"""fetch('/api/packages/{pk['id']}/install', {{method: 'POST', headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{baseUrl: 'http://localhost:4620', username: 'admin', password: 'admin'}})}}).then(async r => [r.status, (await r.json()).code])""")
            record('installing copied content on a server that is not allowed is refused', bad == [403, 'COPY_TARGET_NOT_ALLOWED'], str(bad))

            # A copy aimed at a server that is not allowed is refused before anything is read
            refused = await page3.evaluate("""(async () => {
                const init = await fetch('/api/upload/browser/init', {method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({baseUrl: 'http://localhost:4620', damPath: 'content/dam/x', username: 'admin', password: 'admin'})}).then(r => r.json());
                const r = await fetch('/api/copy/' + init.sessionId + '/start', {method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sourceUrl: 'http://127.0.0.1:4621', sourceUsername: 'admin', sourcePassword: 'admin', folder: '/content/dam/crown'})});
                return [r.status, (await r.json()).code];
            })()""")
            record('copying to a server that is not allowed is refused', refused == [403, 'COPY_TARGET_NOT_ALLOWED'], str(refused))
            onto = await page3.evaluate("""(t) => (async () => {
                const init = await fetch('/api/upload/browser/init', {method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({baseUrl: t, damPath: 'content/dam', username: 'admin', password: 'admin'})}).then(r => r.json());
                const r = await fetch('/api/copy/' + init.sessionId + '/start', {method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sourceUrl: t, sourceUsername: 'admin', sourcePassword: 'admin', folder: '/content/dam/crown'})});
                await fetch('/api/upload/browser/cancel/' + init.sessionId, {method: 'POST'});
                return [r.status, (await r.json()).code];
            })()""", TARGET)
            record('a copy that would land on its own source folder (same server) is refused', onto == [409, 'COPY_ONTO_SOURCE'], str(onto))

            # Cancel throws the copied files away
            page4 = await signed_in(browser)
            await copy(page4, '/content/dam/crown')
            sid = await page4.evaluate('cpSessionId')
            page4.on('dialog', lambda dlg: asyncio.ensure_future(dlg.accept()))
            await page4.click('#cpCancelBtn')
            await asyncio.sleep(0.5)
            gone = await page4.evaluate(f"fetch('/api/copy/{sid}').then(r => r.status)")
            record('Cancel discards the copied files', gone == 404, str(gone))
            other = await page4.evaluate("fetch('/api/copy/' + 'not-a-session').then(r => r.status)")
            record('a copy of someone else / unknown session is not visible', other == 404, str(other))
            record('still nothing written on the source', json.loads(get(SOURCE + '/__writes')) == [], '')
            await browser.close()
    finally:
        source.kill()
        source.wait()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
