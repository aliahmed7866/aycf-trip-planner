from termux.refresh_wizz_from_chrome import _extract_availability_url


def test_extract_searchflight_url():
    html = r'{"searchFlight":"https:\/\/multipass.wizzair.com\/w6\/subscriptions\/json\/availability\/803e9c9c-5331-4b98-aa74-3104bb3b858e"}'
    assert _extract_availability_url(html) == "https://multipass.wizzair.com/w6/subscriptions/json/availability/803e9c9c-5331-4b98-aa74-3104bb3b858e"


def test_extract_pass_id():
    html = "window.foo = { pass_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' };"
    assert _extract_availability_url(html) == "https://multipass.wizzair.com/w6/subscriptions/json/availability/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_extract_none():
    assert _extract_availability_url("<html>no endpoint</html>") is None
