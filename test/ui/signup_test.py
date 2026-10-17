"""Sign up / sign in / forgot password screens. usage: python test/ui/signup_test.py <app url>"""
import asyncio
import sys
import time

from playwright.async_api import async_playwright

APP = sys.argv[1]
results = []


def record(name, ok, detail=''):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ('' if ok else f'\n        {detail}'))


async def fresh(browser):
    ctx = await browser.new_context(viewport={'width': 1280, 'height': 900})
    page = await ctx.new_page()
    errors = []
    page.on('pageerror', lambda e: errors.append(str(e)))
    await page.goto(APP, wait_until='domcontentloaded')
    await page.wait_for_function("typeof submitSignup === 'function'", timeout=60000)
    return ctx, page, errors


async def main():
    email = f'qe.ui{int(time.time())}@adobe.com'
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx, page, errors = await fresh(browser)

        await page.click('text=Create an account')
        await page.fill('#signupName', 'QE Ui')
        await page.fill('#signupEmail', 'qe@gmail.com')
        await page.fill('#signupPassword', 'my-own-password-1')
        await page.fill('#signupConfirm', 'my-own-password-1')
        await page.click('#signupSubmit')
        msg = await page.inner_text('#signupError')
        record('non-Adobe email is stopped on the sign-up screen', 'Adobe email' in msg, msg)

        await page.fill('#signupEmail', email)
        await page.fill('#signupConfirm', 'different-password')
        await page.click('#signupSubmit')
        msg = await page.inner_text('#signupError')
        record('mismatched passwords are caught', 'not the same' in msg, msg)

        await page.fill('#signupConfirm', 'my-own-password-1')
        await page.click('#signupSubmit')
        await page.wait_for_function('!!currentUser', timeout=10000)
        hidden = await page.evaluate("document.getElementById('loginScreen').classList.contains('hidden')")
        name = await page.evaluate('currentUser.name')
        record('sign-up opens the app signed in', hidden and name == 'QE Ui', f'login hidden={hidden}, name={name}')

        await page.evaluate('signOut()')
        await page.wait_for_selector('#loginForm', state='visible')
        await page.fill('#loginEmail', email)
        await page.fill('#loginPassword', 'my-own-password-1')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=10000)
        record('signs back in with email + password', True)
        await page.evaluate('signOut()')
        await page.wait_for_selector('#loginForm', state='visible')

        await page.fill('#loginEmail', 'me@gmail.com')
        await page.fill('#loginPassword', 'whatever-12345')
        await page.click('#loginSubmit')
        msg = await page.inner_text('#loginError')
        record('sign-in with a non-Adobe email shows the rule', 'Adobe email' in msg, msg)

        await page.fill('#loginEmail', email)
        await page.click('text=Forgot password?')
        carried = await page.input_value('#forgotEmail')
        record('forgot password carries over the typed email', carried == email, carried)
        await page.fill('#forgotEmail', 'me@gmail.com')
        await page.click('#forgotSubmit')
        msg = await page.inner_text('#forgotError')
        record('forgot password only takes Adobe emails', 'Adobe email' in msg, msg)
        await page.fill('#forgotEmail', email)
        await page.click('#forgotSubmit')
        await page.wait_for_selector('#forgotDone', state='visible')
        msg = await page.inner_text('#forgotDone')
        record('forgot password confirms the request', 'admin' in msg or 'reset link' in msg, msg)
        await page.click('text=Back to sign in')
        record('back to sign in works', await page.is_visible('#loginForm'))

        await page.fill('#loginEmail', 'admin@test.local')
        await page.fill('#loginPassword', 'admin-pass-123')
        resp = []
        page.on('response', lambda r: resp.append(f'{r.status} {r.url.split("/api/")[-1]}') if '/api/auth' in r.url else None)
        await page.click('#loginSubmit')
        try:
            await page.wait_for_function('!!currentUser', timeout=10000)
        except Exception:
            diag = await page.evaluate("""() => ({ err: document.getElementById('loginError').innerText, errHidden: document.getElementById('loginError').classList.contains('hidden'),
                visible: [...document.querySelectorAll('#loginScreen .login-card')].filter(e => !e.classList.contains('hidden')).map(e => e.id),
                email: document.getElementById('loginEmail').value, pwLen: document.getElementById('loginPassword').value.length,
                btnDisabled: document.getElementById('loginSubmit').disabled })""")
            print('DIAG', diag, 'responses', resp)
            raise
        await page.evaluate("showView('team')")
        await page.wait_for_function(f"document.getElementById('teamBody').innerText.includes({email!r})", timeout=10000)
        row = await page.evaluate(f"[...document.querySelectorAll('#teamBody tr')].find(r => r.innerText.includes({email!r})).innerText")
        record('admin sees "Asked for password reset" on Team members', 'Asked for password reset' in row, row)
        record('no JavaScript errors', not errors, str(errors))
        await ctx.close()

        # a slow start-up check must not throw someone off the sign-up screen
        ctx, page, errors = await fresh(browser)
        await page.click('text=Create an account')
        await page.fill('#signupName', 'Typing')
        await page.evaluate('showLogin()')
        record('late session check keeps the sign-up screen and what was typed',
               await page.is_visible('#signupForm') and await page.input_value('#signupName') == 'Typing')
        await ctx.close()

        # reload while signed in, on a slow network: the sign-in screen must never flash
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(APP, wait_until='domcontentloaded')
        await page.wait_for_function("typeof submitLogin === 'function'", timeout=60000)
        await page.fill('#loginEmail', 'qe@test.local')
        await page.fill('#loginPassword', 'qe-pass-12345')
        await page.click('#loginSubmit')
        await page.wait_for_function('!!currentUser', timeout=15000)

        async def slow_me(route):
            await asyncio.sleep(1)
            await route.continue_()
        await page.route('**/api/auth/me', slow_me)
        await page.add_init_script("""window.__loginSeen = false; const t = setInterval(() => {
            const el = document.getElementById('loginScreen');
            if (el && !el.classList.contains('hidden') && getComputedStyle(el).display !== 'none') window.__loginSeen = true;
            if (window.currentUser) clearInterval(t); }, 10);""")
        await page.reload(wait_until='domcontentloaded')
        await page.wait_for_function('!!currentUser', timeout=15000)
        seen = await page.evaluate('window.__loginSeen')
        record('reload while signed in never shows the sign-in screen, even when the check is slow', seen is False, f'loginSeen={seen}')
        await ctx.close()
        await browser.close()
    print(f'\n{sum(results)}/{len(results)} passed')
    sys.exit(0 if all(results) else 1)


asyncio.run(main())
