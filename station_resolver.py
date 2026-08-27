"""Robust station-name -> IATA preparation for AYCF scans.

The authenticated Chrome capture is authoritative and takes priority.  This
module normalizes those aliases, supplements them with deterministic fallbacks,
and only hits Wizz's public map if names from the current PDF are still missing.
It installs exact aliases for every required PDF station before the long scan so
station resolution can never fail half-way through a run.
"""

import os
import re
import unicodedata
from typing import Iterable

import requests


# Deterministic fallbacks for Wizz/AYCF stations. Captured aliases always win.
# This is deliberately broader than the small emergency table in scanner.py.
FALLBACK_IATA = {
    "abu dhabi": "AUH", "agadir": "AGA", "alghero": "AHO", "alicante": "ALC",
    "amman": "AMM", "amsterdam": "AMS", "antalya": "AYT", "athens": "ATH",
    "baku": "GYD", "bari": "BRI", "basel": "BSL", "batumi": "BUS",
    "belgrade": "BEG", "berlin": "BER", "bilbao": "BIO", "birmingham": "BHX",
    "bodrum": "BJV", "bologna": "BLQ", "bratislava": "BTS", "brno": "BRQ",
    "brussels": "CRL", "brussels charleroi": "CRL", "bucharest": "OTP",
    "bucharest otopeni": "OTP", "budapest": "BUD", "burgas": "BOJ",
    "cairo": "CAI", "catania": "CTA", "chisinau": "RMO", "cluj napoca": "CLJ",
    "cluj-napoca": "CLJ", "cologne": "CGN", "cologne bonn": "CGN",
    "copenhagen": "CPH", "corfu": "CFU", "craiova": "CRA", "dalaman": "DLM",
    "debrecen": "DEB", "dortmund": "DTM", "dubai": "DWC",
    "dubai world central": "DWC", "dubrovnik": "DBV", "eindhoven": "EIN",
    "faro": "FAO", "frankfurt": "HHN", "frankfurt hahn": "HHN", "funchal": "FNC",
    "gdansk": "GDN", "geneva": "GVA", "gothenburg": "GOT", "gran canaria": "LPA",
    "hamburg": "HAM", "helsinki": "HEL", "heraklion": "HER", "hurghada": "HRG",
    "iasi": "IAS", "istanbul": "IST", "istanbul sabiha gokcen": "SAW", "izmir": "ADB",
    "kaunas": "KUN", "katowice": "KTW", "krakow": "KRK", "kutaisi": "KUT",
    "larnaca": "LCA", "leeds bradford": "LBA", "leeds/bradford": "LBA",
    "lisbon": "LIS", "liverpool": "LPL", "ljubljana": "LJU", "lodz": "LCJ",
    "london": "LTN", "london luton": "LTN", "lyon": "LYS", "madrid": "MAD",
    "malaga": "AGP", "malmo": "MMX", "malta": "MLA", "marrakech": "RAK",
    "marrakesh": "RAK", "memmingen": "FMM", "milan": "MXP", "milan malpensa": "MXP",
    "munich": "MUC", "naples": "NAP", "nice": "NCE", "nuremberg": "NUE",
    "oslo": "OSL", "oslo torp": "TRF", "palermo": "PMO", "paphos": "PFO",
    "paris": "BVA", "paris beauvais": "BVA", "pisa": "PSA", "podgorica": "TGD",
    "porto": "OPO", "poznan": "POZ", "prague": "PRG", "pristina": "PRN",
    "reykjavik": "KEF", "reykjavik keflavik": "KEF", "rhodes": "RHO", "riga": "RIX",
    "rome": "FCO", "rome fiumicino": "FCO", "rzeszow": "RZE", "sarajevo": "SJJ",
    "satu mare": "SUJ", "seville": "SVQ", "sharm el sheikh": "SSH",
    "sharm el-sheikh": "SSH", "sibiu": "SBZ", "skopje": "SKP", "sofia": "SOF",
    "split": "SPU", "stockholm": "ARN", "stockholm arlanda": "ARN", "suceava": "SCV",
    "tallinn": "TLL", "tbilisi": "TBS", "tenerife": "TFS", "tenerife south": "TFS",
    "thessaloniki": "SKG", "timisoara": "TSR", "tirana": "TIA", "turin": "TRN",
    "valencia": "VLC", "varna": "VAR", "venice": "VCE", "verona": "VRN",
    "vienna": "VIE", "vilnius": "VNO", "warsaw": "WAW", "warsaw chopin": "WAW",
    "wroclaw": "WRO", "yerevan": "EVN", "zagreb": "ZAG",
}


def normalize_name(value) -> str:
    text = str(value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _valid_iata(value) -> str | None:
    code = str(value or "").strip().upper()
    return code if len(code) == 3 and code.isalpha() else None


def _install_alias(client, label, iata: str) -> None:
    code = _valid_iata(iata)
    label_text = str(label or "").strip()
    if not code or not label_text:
        return
    # Never overwrite the exact authenticated capture.
    client.station_ids.setdefault(label_text.casefold(), code)
    normalized = normalize_name(label_text)
    if normalized:
        client.station_ids.setdefault(normalized, code)
    # Useful for values such as "Varna (VAR)" or "Varna, Bulgaria".
    for separator in ("(", ",", " - ", " – ", " — "):
        if separator in label_text:
            prefix = label_text.split(separator, 1)[0].strip()
            if prefix:
                client.station_ids.setdefault(prefix.casefold(), code)
                norm_prefix = normalize_name(prefix)
                if norm_prefix:
                    client.station_ids.setdefault(norm_prefix, code)


def _normalize_existing(client) -> int:
    before = len(client.station_ids)
    for label, iata in list(client.station_ids.items()):
        _install_alias(client, label, iata)
    for label, iata in FALLBACK_IATA.items():
        _install_alias(client, label, iata)
    return len(client.station_ids) - before


def _lookup(client, name: str) -> str | None:
    raw = str(name or "").strip()
    if len(raw) == 3 and raw.isalpha():
        return raw.upper()
    for key in (raw.casefold(), normalize_name(raw)):
        code = _valid_iata(client.station_ids.get(key))
        if code:
            return code
    return None


def _load_public_wizz_map(client) -> int:
    """Best-effort single public-map refresh. No auth/session data is sent."""
    before = len(client.station_ids)
    session = requests.Session()
    session.headers.update({
        "User-Agent": os.environ.get(
            "WIZZ_USER_AGENT",
            "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        ),
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://www.wizzair.com",
        "Referer": "https://www.wizzair.com/",
    })
    try:
        home = session.get("https://www.wizzair.com/", timeout=20, allow_redirects=True)
        versions = re.findall(r"be\.wizzair\.com/(\d+\.\d+\.\d+)", home.text) if home.ok else []
    except Exception:
        versions = []

    candidates = []
    for version in versions[:4] + ["12.2.0"]:
        if version not in candidates:
            candidates.append(version)

    payload = None
    for version in candidates:
        for path in ("Api/asset/MapData", "Api/asset/map"):
            try:
                response = session.get(
                    f"https://be.wizzair.com/{version}/{path}?languageCode=en-gb",
                    timeout=20,
                )
                if response.status_code != 200:
                    continue
                data = response.json()
                if isinstance(data, dict) and isinstance(data.get("cities"), list):
                    payload = data
                    break
            except Exception:
                continue
        if payload:
            break

    if payload:
        for city in payload.get("cities") or []:
            if not isinstance(city, dict):
                continue
            iata = _valid_iata(city.get("iata") or city.get("iataCode"))
            if not iata:
                continue
            _install_alias(client, iata, iata)
            for key in (
                "name", "shortName", "city", "cityName", "airportName",
                "displayName", "nameWithCountry", "fullName",
            ):
                _install_alias(client, city.get(key), iata)
    return len(client.station_ids) - before


def prepare_required_stations(client, required_names: Iterable[str]) -> dict:
    """Resolve the complete PDF station set before any availability polling."""
    required = sorted({str(x).strip() for x in required_names if str(x).strip()})
    normalized_added = _normalize_existing(client)

    unresolved = [name for name in required if not _lookup(client, name)]
    public_added = 0
    if unresolved and os.environ.get("AYCF_DISABLE_PUBLIC_STATION_MAP", "false").lower() != "true":
        public_added = _load_public_wizz_map(client)
        unresolved = [name for name in required if not _lookup(client, name)]

    # Install the exact PDF spelling so scanner.resolve_station() succeeds by
    # direct lookup even when the source alias only matched after normalization.
    resolved = 0
    for name in required:
        code = _lookup(client, name)
        if code:
            client.station_ids[name.casefold()] = code
            resolved += 1

    return {
        "required": len(required),
        "resolved": resolved,
        "unresolved": unresolved,
        "normalized_added": normalized_added,
        "public_added": public_added,
        "aliases": len(client.station_ids),
    }
