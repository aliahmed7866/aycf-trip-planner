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

LOGIN_URL = os.environ.get("AYCF_WIZZ_LOGIN_URL", "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets")


def _eval(ws, expression):
    out = _cdp_call(ws, "Runtime.evaluate", {"expression": expression, "returnByValue": True, "awaitPromise": True})
    return (((out or {}).get("result") or {}).get("value"))


def _target(browser_ws):
    pages = [x for x in _json_get("/json") if x.get("type") == "page" and x.get("webSocketDebuggerUrl")]
    wizz = [x for x in pages if "wizzair.com" in str(x.get("url") or "")]
    if wizz:
        return wizz[0]
    _cdp_call(browser_ws, "Target.createTarget", {"url": LOGIN_URL})
    time.sleep(3)
    pages = [x for x in _json_get("/json") if x.get("type") == "page" and x.get("webSocketDebuggerUrl")]
    wizz = [x for x in pages if "wizzair.com" in str(x.get("url") or "")]
    if not wizz:
        raise RuntimeError("Could not open Wizz login page in Chrome")
    return wizz[0]


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
    user_json, pass_json = json.dumps(creds["username"]), json.dumps(creds["password"])
    # Generic selector set deliberately avoids dependency on Wizz CSS classes.
    script = f"""(()=>{{
      const text=(document.body&&document.body.innerText||'').toLowerCase();
      if (/captcha|verify you are human|security check|verification code|one-time|passkey/.test(text)) return {{state:'challenge'}};
      const visible=e=>!!(e&&e.offsetParent!==null&&!e.disabled);
      const inputs=[...document.querySelectorAll('input')].filter(visible);
      const user=inputs.find(e=>['email','text'].includes((e.type||'').toLowerCase()) && /email|user|login/i.test((e.name||'')+' '+(e.id||'')+' '+(e.placeholder||''))) || inputs.find(e=>(e.type||'').toLowerCase()==='email');
      const pass=inputs.find(e=>(e.type||'').toLowerCase()==='password');
      if(!user||!pass) return {{state:'form_not_found',url:location.href}};
      const set=(el,v)=>{{const p=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value');p.set.call(el,v);el.dispatchEvent(new Event('input',{{bubbles:true}}));el.dispatchEvent(new Event('change',{{bubbles:true}}));}};
      set(user,{user_json}); set(pass,{pass_json});
      const buttons=[...document.querySelectorAll('button,input[type=submit]')].filter(visible);
      const submit=buttons.find(e=>/log.?in|sign.?in|continue/i.test((e.innerText||e.value||e.getAttribute('aria-label')||''))) || buttons.find(e=>(e.type||'').toLowerCase()==='submit');
      if(!submit) return {{state:'submit_not_found'}};
      submit.click(); return {{state:'submitted'}};
    }})()"""
    result = _eval(ws, script) or {}
    if result.get("state") == "challenge":
        print("[AYCF] Wizz requires an interactive security challenge; manual attention is required.")
        return 12
    if result.get("state") != "submitted":
        print(f"[AYCF] Automatic Wizz login form unavailable: {result.get('state', 'unknown')}")
        return 13
    # Wait for either private-page success or a security/error state.
    for _ in range(30):
        time.sleep(1)
        state = _eval(ws, "(()=>{const t=(document.body&&document.body.innerText||'').toLowerCase();return {url:location.href,challenge:/captcha|verify you are human|security check|verification code|one-time|passkey/.test(t),error:/incorrect|invalid password|wrong password|login failed/.test(t)}})()") or {}
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
