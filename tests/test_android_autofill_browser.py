"""Run the actual Android autofill script against isolated provider-form fixtures."""
from pathlib import Path
import pytest

playwright = pytest.importorskip('playwright.sync_api')
SCRIPT = (Path(__file__).resolve().parents[1] / 'static/mobile_wizz_login.js').read_text()
URL = 'https://multipass.wizzair.com/auth/login'
FORM = '<form method="post" action="/auth/submit"><input name="username"><input type="password" name="password"><button>Sign in</button></form>'


@pytest.fixture
def page():
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page()
        page.route('**/*', lambda route: route.fulfill(status=200, content_type='text/html', body=FORM))
        page.goto(URL)
        yield page
        browser.close()


def fill(page, url=URL):
    return page.evaluate(
        '(args) => (' + SCRIPT + ')(...args)', [url, 'member@example.com', 'quotes"<safe>&\\secret'])


def test_fills_saved_details_fires_events_and_does_not_submit(page):
    page.evaluate("window.events=[]; document.addEventListener('input',e=>events.push(e.target.name))")
    assert fill(page)=='filled'
    assert page.locator('[name=username]').input_value()=='member@example.com'
    assert page.locator('[name=password]').input_value()=='quotes"<safe>&\\secret'
    assert page.evaluate('events')==['username','password']
    assert page.url==URL
    page.locator('[name=password]').fill('user-changed-it')
    assert fill(page)=='edited'
    assert page.locator('[name=password]').input_value()=='user-changed-it'


@pytest.mark.parametrize('html', [
    FORM.replace('method="post"','method="get"'),
    FORM.replace('/auth/submit','https://example.com/steal'),
    FORM.replace('/auth/submit','https://multipass.wizzair.com.evil.example/steal'),
    FORM.replace('name="password"','name="password" autocomplete="new-password"'),
    FORM.replace('name="password"','name="password" style="display:none"'),
    '<iframe srcdoc="'+FORM.replace('"','&quot;')+'"></iframe>',
])
def test_ineligible_forms_remain_empty(page, html):
    page.set_content(html)
    assert fill(page)=='waiting'
    assert page.locator('input[type=password]').count()==0 or page.locator('input[type=password]').input_value()==''


def test_redirected_page_never_receives_password(page):
    page.goto('https://example.com/auth/login')
    assert fill(page)=='blocked'
    assert page.locator('[name=password]').input_value()==''
    assert fill(page,'https://example.com/auth/login')=='blocked'


def test_delayed_form_can_fill_without_overwriting_another_account(page):
    page.set_content('<main>Loading</main>')
    assert fill(page)=='waiting'
    page.set_content(FORM)
    page.locator('[name=username]').fill('different@example.com')
    assert fill(page)=='edited'
    assert page.locator('[name=password]').input_value()==''
