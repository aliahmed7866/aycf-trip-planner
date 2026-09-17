"""Bounded, shareable diagnostics. Never copy request cookies or headers."""
from __future__ import annotations

from html.parser import HTMLParser
import json
import re

_SENSITIVE = re.compile(r'password|passwd|secret|token|cookie|authorization|api.?key|session|credential|storage.?state', re.I)
_URL = re.compile(r'https?://[^\s<>"\']+', re.I)


def redact_text(value: str) -> str:
    """Mask common credentials in old free-text logs; callers bound input size."""
    value = re.sub(r'(?<![\w])[A-Za-z0-9_+/=-]{32,}(?![\w])', '[long identifier removed]', value)
    value = re.sub(r'(?im)^.*(?:authorization|(?:set-)?cookie)\s*["\']?\s*[:=].*$', '[credential line removed]', value)
    value = re.sub(r'(?im)((?<![\w.-])["\']?[\w.-]{0,128}(?:password|passwd|secret|token|api[_-]?key|credential)[\w.-]{0,128}["\']?\s*[:=])[^\r\n]*', r'\1 [redacted]', value)
    value = re.sub(r'(?i)\b(?:Bearer|Basic)\s+[^\s,;"\'<>]+', '[authorization removed]', value)
    value = _URL.sub(lambda match: match.group(0).split('?', 1)[0].split('#', 1)[0]
                    if '@' not in match.group(0) else '[credential URL removed]', value)
    value = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', '[email removed]', value)
    return ''.join(c for c in value if c in '\n\r\t' or ord(c) >= 32)


def _redact_json(value, depth=0):
    if depth > 8:
        return '[nested content omitted]'
    if isinstance(value, dict):
        return {str(key): '[redacted]' if _SENSITIVE.search(str(key)) else _redact_json(item, depth + 1)
                for key, item in list(value.items())[:50]}
    if isinstance(value, list):
        return [_redact_json(item, depth + 1) for item in value[:20]]
    if isinstance(value, str):
        return redact_text(value)
    return value


class _VisibleText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hidden = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'noscript'}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'noscript'}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden and data.strip():
            self.parts.append(data.strip())


def response_details(response):
    """Capture only a 429 response, without URL, request headers or cookies.

    JSON credentials and HTML scripts/form attributes are excluded. This is a
    redacted preview, not a byte-for-byte HTTP capture or a provider diagnosis.
    """
    content = getattr(response, 'content', b'')
    content = content if isinstance(content, bytes) else b''
    mime = str(response.headers.get('Content-Type', '')).split(';', 1)[0].strip().lower()
    mime = mime if mime in {'application/json', 'application/problem+json', 'text/html', 'text/plain'} else 'other/unknown'
    result = {'http_status': 429, 'content_type': mime, 'body_bytes': len(content),
              'preview_truncated': len(content) > 16384}
    if not content:
        result['body_preview'] = '(empty response body)'
        return result
    if mime == 'other/unknown':
        result['body_preview'] = '(unrecognised content type; body omitted)'
        return result
    text = content[:16384].decode('utf-8', errors='replace')
    if 'json' in mime:
        try:
            text = json.dumps(_redact_json(json.loads(text)), ensure_ascii=False, indent=2)
        except (ValueError, RecursionError):
            text = '(incomplete or invalid JSON; body omitted)'
    elif mime == 'text/html':
        parser = _VisibleText()
        parser.feed(text)
        text = '\n'.join(parser.parts)
    else:
        text = redact_text(text)
    # JSON has already been redacted structurally; HTML is now plain text.
    if mime == 'text/html':
        text = redact_text(text)
    result['preview_truncated'] = result['preview_truncated'] or len(text) > 4096
    result['body_preview'] = text[:4096]
    return result
