"""Parse the standard Wizz login form and serialize its session cookies."""
import html
import re
from html.parser import HTMLParser
import requests

CHALLENGE_RE = re.compile(
    r"captcha|verify you are human|security check|verification code|one[- ]time|"
    r"passkey|two[- ]factor|multi[- ]factor|authenticator|enter.*code",
    re.I,
)
AUTH_ERROR_RE = re.compile(
    r"incorrect|invalid password|wrong password|login failed|invalid username|invalid email",
    re.I,
)


class LoginFormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.forms: list[dict] = []
        self.current: dict | None = None

    def handle_starttag(self, tag, attrs):
        data = {str(k): str(v or "") for k, v in attrs}
        if tag.lower() == "form":
            self.current = {
                "action": html.unescape(data.get("action", "")),
                "method": data.get("method", "post").lower(),
                "inputs": [],
            }
            self.forms.append(self.current)
        elif tag.lower() == "input" and self.current is not None:
            self.current["inputs"].append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "form":
            self.current = None


class VisibleTextParser(HTMLParser):
    """Extract user-visible page text, excluding scripts/styles/templates."""

    HIDDEN = {"script", "style", "template", "noscript", "svg"}

    def __init__(self):
        super().__init__()
        self.hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.HIDDEN:
            self.hidden_depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in self.HIDDEN and self.hidden_depth:
            self.hidden_depth -= 1

    def handle_data(self, data):
        if self.hidden_depth == 0 and str(data or "").strip():
            self.parts.append(str(data))


def _visible_text(value: str) -> str:
    parser = VisibleTextParser()
    try:
        parser.feed(str(value or ""))
        return " ".join(parser.parts)
    except Exception:
        return ""


def _login_form(response: requests.Response) -> dict | None:
    parser = LoginFormParser()
    try:
        parser.feed(response.text or "")
    except Exception:
        return None
    for form in parser.forms:
        inputs = form.get("inputs") or []
        has_password = any((x.get("type") or "").lower() == "password" for x in inputs)
        if has_password:
            return form
    return None


def _looks_authenticated(response: requests.Response) -> bool:
    url = str(response.url or "").lower()
    text = _visible_text(response.text or "")
    if CHALLENGE_RE.search(text):
        return False
    if "private-page" in url and not _login_form(response):
        return response.status_code == 200
    return False


def _field_name(inputs: list[dict], kind: str) -> str | None:
    if kind == "password":
        for item in inputs:
            if (item.get("type") or "").lower() == "password" and item.get("name"):
                return item["name"]
        return None
    candidates = []
    for item in inputs:
        typ = (item.get("type") or "text").lower()
        if typ not in {"email", "text", "username"} or not item.get("name"):
            continue
        haystack = " ".join(
            str(item.get(k) or "") for k in ("name", "id", "placeholder", "autocomplete")
        ).lower()
        score = sum(token in haystack for token in ("email", "user", "login", "identifier"))
        candidates.append((score, item["name"]))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _cookie_state(session: requests.Session) -> dict:
    cookies = []
    for cookie in session.cookies:
        domain = str(cookie.domain or "").lstrip(".").lower()
        if domain != "wizzair.com" and not domain.endswith(".wizzair.com"):
            continue
        item = {
            "name": str(cookie.name),
            "value": str(cookie.value),
            "domain": str(cookie.domain or ".wizzair.com"),
            "path": str(cookie.path or "/"),
            "expires": int(cookie.expires) if cookie.expires else -1,
            "httpOnly": bool(cookie.has_nonstandard_attr("HttpOnly")),
            "secure": bool(cookie.secure),
            "sameSite": "Lax",
        }
        cookies.append(item)
    return {"cookies": cookies, "origins": []}


