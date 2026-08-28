"""Attempt ordinary Wizz username/password login in Android Chrome via CDP.

This intentionally stops for CAPTCHA/MFA/passkey/security challenges. It never
tries to bypass an interactive Wizz security control.
"""
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from termux.import_wizz_from_chrome import _load_termux_env, _json_get, _cdp_call  # noqa: E402
_load_termux_env()
from credential_vault import CredentialVault  # noqa: E402

LOGIN_URL = os.environ.get(
    "AYCF_WIZZ_LOGIN_URL",
    "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets",
)


def _eval(ws, expression):
    out = _cdp_call(
        ws,
        "Runtime.evaluate",
        {"expression": expression, "returnByValue": True, "awaitPromise": True},
    )
    return (((out or {}).get("result") or {}).get("value"))


def _target(browser_ws):
    pages = [
        x
        for x in _json_get("/json")
        if x.get("type") == "page" and x.get("webSocketDebuggerUrl")
    ]
    wizz = [x for x in pages if "wizzair.com" in str(x.get("url") or "")]
    if wizz:
        return wizz[0]
    _cdp_call(browser_ws, "Target.createTarget", {"url": LOGIN_URL})
    time.sleep(3)
    pages = [
        x
        for x in _json_get("/json")
        if x.get("type") == "page" and x.get("webSocketDebuggerUrl")
    ]
    wizz = [x for x in pages if "wizzair.com" in str(x.get("url") or "")]
    if not wizz:
        raise RuntimeError("Could not open Wizz login page in Chrome")
    return wizz[0]


def _login_state(ws, click_entry=False):
    click_json = "true" if click_entry else "false"
    return _eval(
        ws,
        fr"""(()=>{{
          const text=(document.body&&document.body.innerText||'').toLowerCase();
          if (/captcha|verify you are human|security check|verification code|one-time|passkey/.test(text))
            return {{state:'challenge',url:location.href}};
          const visible=e=>!!(e&&e.offsetParent!==null&&!e.disabled);
          const inputs=[...document.querySelectorAll('input')].filter(visible);
          const user=inputs.find(e=>['email','text'].includes((e.type||'').toLowerCase()) && /email|user|login/i.test((e.name||'')+' '+(e.id||'')+' '+(e.placeholder||''))) || inputs.find(e=>(e.type||'').toLowerCase()==='email');
          const pass=inputs.find(e=>(e.type||'').toLowerCase()==='password');
          if(user&&pass) return {{state:'form',url:location.href}};
          if({click_json}) {{
            const nodes=[...document.querySelectorAll('button,a,[role=button]')].filter(visible);
            const login=nodes.find(e=>/^(log\s*in|login|sign\s*in|sign-in)$/i.test(((e.innerText||e.textContent||e.getAttribute('aria-label')||'').trim()))) ||
                        nodes.find(e=>/log\s*in|sign\s*in/i.test((e.innerText||e.textContent||e.getAttribute('aria-label')||'')));
            if(login) {{ login.click(); return {{state:'entry_clicked',url:location.href}}; }}
          }}
          return {{state:'form_not_found',url:location.href}};
        }})()""",
    ) or {}


def _ensure_login_form(ws):
    for attempt in range(18):
        state = _login_state(ws, click_entry=(attempt in {0, 4, 8, 12}))
        if state.get("state") in {"form", "challenge"}:
            return state
        if attempt == 5:
            try:
                _cdp_call(ws, "Page.enable")
                _cdp_call(ws, "Page.navigate", {"url": LOGIN_URL})
            except Exception:
                pass
        time.sleep(1)
    return _login_state(ws, click_entry=True)


def _credential_submit_script(username: str, password: str) -> str:
    """Build the ordinary login submission script without bypassing challenges."""
    user_json, pass_json = json.dumps(username), json.dumps(password)
    return f"""(()=>{{
      const text=(document.body&&document.body.innerText||'').toLowerCase();
      if (/captcha|verify you are human|security check|verification code|one-time|passkey/.test(text)) return {{state:'challenge'}};
      const visible=e=>!!(e&&e.offsetParent!==null&&!e.disabled);
      const inputs=[...document.querySelectorAll('input')].filter(visible);
      const user=inputs.find(e=>['email','text'].includes((e.type||'').toLowerCase()) && /email|user|login/i.test((e.name||'')+' '+(e.id||'')+' '+(e.placeholder||''))) || inputs.find(e=>(e.type||'').toLowerCase()==='email');
      const pass=inputs.find(e=>(e.type||'').toLowerCase()==='password');
      if(!user||!pass) return {{state:'form_not_found',url:location.href}};
      const set=(el,v)=>{{const p=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value');p.set.call(el,v);el.dispatchEvent(new Event('input',{{bubbles:true}}));el.dispatchEvent(new Event('change',{{bubbles:true}}));}};
      set(user,{user_json}); set(pass,{pass_json});
      const controls=[...document.querySelectorAll('button,input[type=submit],[role=button]')].filter(visible);
      const label=e=>(e.innerText||e.textContent||e.value||e.getAttribute('aria-label')||e.getAttribute('data-testid')||e.id||e.name||'');
      const submit=controls.find(e=>/log.?in|sign.?in|continue|submit/i.test(label(e))) || controls.find(e=>(e.type||'').toLowerCase()==='submit');
      if(submit) {{ submit.click(); return {{state:'submitted',method:'control'}}; }}
      const form=pass.form||user.form||pass.closest('form')||user.closest('form');
      if(form&&typeof form.requestSubmit==='function') {{ form.requestSubmit(); return {{state:'submitted',method:'requestSubmit'}}; }}
      return {{state:'submit_not_found',url:location.href}};
    }})()"""


def main():
    creds = CredentialVault().load()
    if not creds:
        print("[AYCF] No encrypted Wizz login credentials configured.")
        return 10
    version = _json_get("/json/version")
    browser_ws = version.get("webSocketDebuggerUrl")
    if not browser_ws:
        return 11
    target = _target(browser_ws)
    ws = target["webSocketDebuggerUrl"]

    state = _ensure_login_form(ws)
    if state.get("state") == "challenge":
        print("[AYCF] Wizz requires an interactive security challenge; manual attention is required.")
        return 12
    if state.get("state") != "form":
        print(
            "[AYCF] Automatic Wizz login form unavailable after opening the login flow: "
            f"{state.get('state', 'unknown')} ({state.get('url', 'unknown URL')})"
        )
        return 13

    result = _eval(ws, _credential_submit_script(creds["username"], creds["password"])) or {}
    if result.get("state") == "challenge":
        print("[AYCF] Wizz requires an interactive security challenge; manual attention is required.")
        return 12
    if result.get("state") != "submitted":
        print(f"[AYCF] Automatic Wizz login form unavailable: {result.get('state', 'unknown')}")
        return 13

    for _ in range(35):
        time.sleep(1)
        state = _eval(
            ws,
            "(()=>{const t=(document.body&&document.body.innerText||'').toLowerCase();return {url:location.href,challenge:/captcha|verify you are human|security check|verification code|one-time|passkey/.test(t),error:/incorrect|invalid password|wrong password|login failed/.test(t)}})()",
        ) or {}
        if "private-page" in str(state.get("url") or "") and not state.get("challenge"):
            print("[AYCF] Wizz ordinary password login completed in Chrome.")
            return 0
        if state.get("challenge"):
            print("[AYCF] Wizz login reached an interactive security challenge; manual attention is required.")
            return 12
        if state.get("error"):
            print("[AYCF] Wizz rejected the stored credentials. Update the encrypted credential vault.")
            return 14
    print("[AYCF] Wizz login did not complete automatically; manual attention may be required.")
    return 15


if __name__ == "__main__":
    raise SystemExit(main())
