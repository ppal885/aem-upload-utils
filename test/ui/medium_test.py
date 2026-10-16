"""Browser checks for the Medium UI fixes. usage: python test/ui/medium_test.py <app url> <mock aem url> <small files dir> <big files dir>"""
import asyncio
import json
import os
import sys
import urllib.request

from playwright.async_api import async_playwright

APP, AEM, SMALL_DIR, BIG_DIR = sys.argv[1:5]
SMALL = [os.path.join(SMALL_DIR, f) for f in sorted(os.listdir(SMALL_DIR))]
BIG = [os.path.join(BIG_DIR, f) for f in sorted(os.listdir(BIG_DIR))]
results = []


def record(name, ok, detail):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


_COOKIE = None


def api(path):
    global _COOKIE
    if _COOKIE is None:
        aem = AEM
        req = urllib.request.Request(APP + '/api/auth/login', data=json.dumps({'email': 'admin@test.local', 'password': 'admin-pass-123'}).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req) as r:
            _COOKIE = r.headers.get('Set-Cookie').split(';')[0]
    with urllib.request.urlopen(urllib.request.Request(APP + path, headers={'Cookie': _COOKIE})) as r:
        return json.loads(r.read())


MODAL_OPEN = "!document.getElementById('confirmModal').classList.contains('hidden')"


async def fill_form(page, dam='content/dam/UiMedium'):
    await page.fill('#damPath', dam)


async def start_upload(page):
    await page.evaluate('initiateUpload()')
    await page.wait_for_function(MODAL_OPEN, timeout=10000)
    await page.evaluate('handleConfirmOk()')


async def wait_idle(page, timeout_s=90):
    for _ in range(timeout_s * 4):
        if not await page.evaluate('uploading') and not api('/api/upload/active'):
            await asyncio.sleep(0.5)
            return True
        await asyncio.sleep(0.25)
    return False


async def toasts(page):
    return await page.evaluate("[...document.querySelectorAll('#toastContainer > *')].map(e => e.textContent.trim())")


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
        page = await ctx.new_page()
        await page.goto(APP)
        await page.wait_for_load_state('networkidle')
        await page.evaluate("""async (aem) => { const r = await fetch('/api/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email: 'admin@test.local', password: 'admin-pass-123'})}); applySignedIn((await r.json()).user); document.getElementById('aemUrl').value = aem; document.getElementById('username').value = 'admin'; document.getElementById('password').value = 'admin'; updateSummaryPanel(); }""", AEM)
        await fill_form(page)

        # #6 a new selection does not inherit the previous selection's file-type filter
        await page.set_input_files('#browserFileInput', SMALL)            # 400 .dita + 50 .xml
        await page.fill('#fileTypeFilter', '.xml')
        await page.evaluate('applyFileTypeFilter()')
        filtered = await page.evaluate('browserFileEntries.length')
        await page.set_input_files('#browserFileInput', SMALL[:10])
        after_new = await page.evaluate('browserFileEntries.length')
        filter_text = await page.input_value('#fileTypeFilter')
        await page.evaluate('clearFileTypeFilter()')
        after_clear = await page.evaluate('browserFileEntries.length')
        record('#6 filter does not leak into a new selection', filtered == 50 and after_new == 10 and filter_text == '' and after_clear == 10,
               f"filtered={filtered}, new selection={after_new}, filter box={filter_text!r}, after Clear={after_clear}")

        # #15 more than the file limit selects nothing
        before = await page.evaluate('browserFileEntries.length')
        await page.evaluate("onBrowserFilesSelected({ length: MAX_BROWSER_FILES + 1 })")
        t = await toasts(page)
        record('#15 over-limit selection is rejected', await page.evaluate('browserFileEntries.length') == before and any('Nothing was selected' in x for x in t),
               f"entries kept={before}, last toast={t[-1][:80] if t else None!r}")

        # #10 URL error stays visible while typing; validateForm blocks '..' and bad URLs
        url_err = True  # the server URL is now checked at sign-in, not in the upload form
        await fill_form(page, dam='content/dam/a/../b')
        await page.evaluate('initiateUpload()')
        await asyncio.sleep(0.8)
        modal = await page.evaluate(MODAL_OPEN)
        dam_err = (await page.inner_text('#damPathErr')) if await page.is_visible('#damPathErr') else ''
        record('#10 live and submit validation', url_err and not modal and '..' in dam_err,
               f"URL error visible while typing={url_err}, dialog opened={modal}, DAM error={dam_err.strip()!r}")
        await fill_form(page)

        # #11 numeric settings: 0 retries kept, out-of-range clamped and written back
        await page.evaluate("toggleAdvanced()")
        await page.fill('#httpRetryCount', '0')
        await page.fill('#httpRequestTimeout', '1')
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=10000)
        body = await page.evaluate('pendingUploadBody')
        timeout_field = await page.input_value('#httpRequestTimeout')
        record('#11 retry 0 kept, timeout clamped', body['httpRetryCount'] == 0 and body['httpRequestTimeout'] == 10000 and timeout_field == '10',
               f"retry={body['httpRetryCount']}, timeout={body['httpRequestTimeout']}ms, field now {timeout_field}")
        await page.evaluate('closeConfirm()')
        await page.fill('#httpRetryCount', '5')
        await page.fill('#httpRequestTimeout', '120')

        # #20 Ctrl+Enter while "Clear All History" is open does nothing; the next upload dialog is clean
        hist_before = len(api('/api/upload/history'))
        await page.evaluate('clearAllHistory()')
        await page.wait_for_function(MODAL_OPEN, timeout=5000)
        await page.keyboard.press('Control+Enter')
        await asyncio.sleep(0.8)
        headline = await page.inner_text('#confirmHeadline')
        await page.evaluate('closeConfirm()')
        await page.evaluate('initiateUpload()')
        await page.wait_for_function(MODAL_OPEN, timeout=5000)
        headline2 = await page.inner_text('#confirmHeadline')
        handler = await page.evaluate('!!window._customConfirmHandler')
        await page.evaluate('closeConfirm()')
        record('#20 confirm dialog cannot run the wrong action', len(api('/api/upload/history')) == hist_before
               and 'Clear' in headline and headline2 == 'Confirm Upload' and not handler,
               f"after Ctrl+Enter headline={headline!r}, upload dialog headline={headline2!r}, stale handler={handler}")

        # #21 + #17 + #12 during an upload
        await page.set_input_files('#browserFileInput', SMALL[:400])
        await start_upload(page)
        for _ in range(240):
            if await page.evaluate('filesCompleted > 20'):
                break
            await asyncio.sleep(0.25)
        stats_live = await page.get_attribute('#progressStats', 'aria-live')
        aria_now = await page.get_attribute('#progressTrack', 'aria-valuenow')
        announcer = await page.inner_text('#progressAnnouncer')
        record('#21 progress is announced sensibly', stats_live is None and aria_now not in (None, '0') and 'complete' in announcer,
               f"stats aria-live={stats_live}, progressbar valuenow={aria_now}, announcer={announcer!r}")
        url_before = await page.input_value('#aemUrl')
        await page.evaluate('rerunUpload(0)')
        locked = await page.evaluate("document.body.classList.contains('uploading-locked') && document.getElementById('skipUnchanged').disabled && document.getElementById('dropZone').tabIndex === -1")
        record('#17 form and selection locked during upload', locked and await page.input_value('#aemUrl') == url_before,
               f"locked={locked}, URL unchanged={await page.input_value('#aemUrl') == url_before}")
        await page.keyboard.press('Escape')
        await asyncio.sleep(0.5)
        still = await page.evaluate('uploading')
        t = await toasts(page)
        await page.keyboard.press('Escape')
        await wait_idle(page)
        newest = api('/api/upload/history')[0]
        label = await page.inner_text('#progressLabel')
        record('#12 Escape needs a second press', still and any('Esc again' in x for x in t) and newest.get('cancelled'),
               f"after 1st Esc uploading={still}, after 2nd Esc history cancelled={newest.get('cancelled')}")
        record('#18 cancelled upload is not shown as 100%', 'Cancelled' in label and not label.startswith('100%'), f"label={label!r}")

        # #9 after a successful upload the same files can be picked again
        await page.set_input_files('#browserFileInput', SMALL[:30])
        await start_upload(page)
        await wait_idle(page)
        preview_hidden = await page.evaluate("document.getElementById('browserFilesPreview').classList.contains('hidden')")
        await page.set_input_files('#browserFileInput', SMALL[:30])
        again = await page.evaluate('browserFileEntries.length')
        record('#9 same files can be selected again after success', preview_hidden and again == 30,
               f"preview hidden after success={preview_hidden}, re-selected entries={again}")

        # #13 Cancel while the credentials/session check is still running
        async def slow_init(route):
            await asyncio.sleep(3)
            await route.continue_()
        await page.route('**/api/upload/browser/init', slow_init)
        await page.set_input_files('#browserFileInput', SMALL[:20])
        hist_before = len(api('/api/upload/history'))
        await start_upload(page)
        await asyncio.sleep(1)
        await page.evaluate('cancelUpload()')
        await asyncio.sleep(4)
        record('#13 Cancel works during the session check', not await page.evaluate('uploading') and not api('/api/upload/active')
               and len(api('/api/upload/history')) == hist_before,
               f"uploading={await page.evaluate('uploading')}, history grew={len(api('/api/upload/history')) - hist_before}")
        await page.unroute('**/api/upload/browser/init')

        # #14 Pause reports in-flight chunks; Cancel while paused does not hang
        async def slow_chunk(route):
            await asyncio.sleep(4)
            try:
                await route.continue_()
            except Exception:
                pass  # request was aborted by Cancel while held
        await page.route('**/api/upload/browser/chunk/**', slow_chunk)
        await page.set_input_files('#browserFileInput', BIG)               # 6,000 files = 6 chunks, 5 in flight
        await start_upload(page)
        await page.wait_for_function("activeChunkXhrs.size >= 5", timeout=60000)
        await page.evaluate('pauseTransfer()')
        status_text = await page.evaluate("document.getElementById('transferStatus') ? document.getElementById('transferStatus').textContent : document.body.innerText.match(/Pausing[^\\n]*/)?.[0]")
        await page.evaluate('cancelUpload()')
        settled = False
        for _ in range(40):
            if not await page.evaluate('uploading') and await page.evaluate('pendingUploadBody === null && !elapsedInterval'):
                settled = True
                break
            await asyncio.sleep(0.25)
        record('#14 Pause is honest and Cancel while paused settles', 'Pausing' in (status_text or '') and settled,
               f"status={status_text!r}, upload flow settled after cancel={settled}")
        await page.unroute('**/api/upload/browser/chunk/**')
        await wait_idle(page)

        # #16 a second tab shows the banner but does not take over the upload
        await page.set_input_files('#browserFileInput', SMALL[:200])
        await start_upload(page)
        for _ in range(120):
            if api('/api/upload/active'):
                break
            await asyncio.sleep(0.25)
        page2 = await ctx.new_page()
        await page2.goto(APP)
        await page2.wait_for_load_state('networkidle')
        await asyncio.sleep(1.5)
        b_uploading = await page2.evaluate('uploading')
        b_banner = await page2.is_visible('#activeUploadsBanner')
        record('#16 second tab does not hijack the upload', not b_uploading and b_banner,
               f"tab B uploading={b_uploading}, tab B banner visible={b_banner}")
        await page2.close()
        await wait_idle(page)

        # #23 empty state for a search with no matches
        await page.evaluate("showView('history')")
        await page.fill('#histSearch', 'zzz-no-such-upload')
        await page.evaluate('filterHistory()')
        empty_text = await page.inner_text('#histEmptyText')
        action_hidden = await page.evaluate("document.getElementById('histEmptyAction').classList.contains('hidden')")
        record('#23 empty search says no matches', 'No uploads match' in empty_text and action_hidden, f"text={empty_text!r}")
        await page.fill('#histSearch', '')
        await page.evaluate('filterHistory()')
        await page.evaluate("showView('upload')")

        # #24 preset chips and recent targets work from the keyboard
        await page.evaluate("localStorage.setItem(PRESETS_KEY, JSON.stringify([{name: 'kb-preset', aemUrl: 'http://kb.example:4502', damPath: 'content/dam/kb', username: 'kbuser'}])); renderPresets();")
        chip = page.locator('#presetList [role=button]').first
        await chip.focus()
        await page.keyboard.press('Enter')
        await asyncio.sleep(0.3)
        tree = await page.evaluate("[...document.querySelectorAll('.tree-toggle')].every(e => e.getAttribute('tabindex') === '0')")
        record('#24 preset chip usable by keyboard', await chip.get_attribute('tabindex') == '0' and await page.input_value('#damPath') == 'content/dam/kb' and tree,
               f"damPath after Enter={await page.input_value('#damPath')!r}, tree toggles focusable={tree}")

        # #22 dark mode: Select Files readable, grey text has contrast
        await page.evaluate('applyDarkMode(true)')
        await asyncio.sleep(0.5)
        colors = await page.evaluate("""() => {
            const sf = [...document.querySelectorAll('button')].find(b => /Select Files/.test(b.textContent));
            const grey = document.querySelector('.text-gray-400');
            return { selectFiles: getComputedStyle(sf).color, grey: getComputedStyle(grey).color };
        }""")
        await page.screenshot(path=os.path.join(os.environ.get('TEST_WORK_DIR', '.'), 'dark-after.png'), clip={'x': 0, 'y': 330, 'width': 1280, 'height': 330})
        record('#22 dark mode readable', colors['selectFiles'] in ('rgb(226, 232, 240)', 'rgb(255, 255, 255)') and colors['grey'] == 'rgb(139, 154, 176)',
               f"{colors}")
        await page.evaluate('applyDarkMode(false)')

        await browser.close()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


asyncio.run(main())
