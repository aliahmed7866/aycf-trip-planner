from termux.auto_login_wizz import _credential_submit_script


def test_submit_script_uses_native_form_fallback():
    script = _credential_submit_script("user@example.com", "secret")
    assert "requestSubmit" in script
    assert "input[type=submit]" in script
    assert "[role=button]" in script
    assert "user@example.com" in script


def test_submit_script_still_stops_for_security_challenges():
    script = _credential_submit_script("user@example.com", "secret")
    assert "captcha" in script
    assert "passkey" in script
    assert "state:'challenge'" in script
