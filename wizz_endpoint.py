"""Shared extraction of an availability URL from Multipass page configuration."""
import html
import re
from urllib.parse import urljoin, urlparse


def extract_availability_url(page_text):
    text = html.unescape(str(page_text or '')).replace(r'\/', '/').replace(r'\"', '"')
    patterns = (
        r'''["']searchFlight["']\s*:\s*["']([^"']+)["']''',
        r'''(?:window\.)?CVO\.flightSearchUrlJson\s*=\s*["']([^"']+)["']''',
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            url = urljoin('https://multipass.wizzair.com/', match.group(1))
            parsed = urlparse(url)
            if parsed.scheme == 'https' and parsed.netloc == 'multipass.wizzair.com' and '/subscriptions/json/availability/' in parsed.path:
                return url
    match = re.search(r'''\bpass_id["']?\s*[:=]\s*["']?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\b''', text, re.I)
    if match:
        return 'https://multipass.wizzair.com/w6/subscriptions/json/availability/' + match.group(1)
    return None
