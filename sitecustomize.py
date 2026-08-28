"""Small runtime compatibility shim for local AYCF station aliases.

Python imports ``sitecustomize`` automatically during startup when this repository
is on ``sys.path`` (as it is for the Termux entry points).  Keep only stable,
non-secret station-name aliases here.  This makes deterministic aliases available
before morning_scan decides whether a network airport-map fallback is necessary.

The shim is intentionally conservative: captured/runtime aliases still win, IATA
codes still pass through untouched, and the original resolver handles everything
else.
"""

try:
    import scanner
except Exception:  # Do not make unrelated Python commands fail at startup.
    scanner = None


_LOCAL_STATION_ALIASES = {
    "alghero": "AHO",
    "london luton": "LTN",
    "london": "LTN",
    "liverpool": "LPL",
    "budapest": "BUD",
    "bucharest": "OTP",
    "warsaw": "WAW",
    "kutaisi": "KUT",
    "yerevan": "EVN",
    "abu dhabi": "AUH",
    "amman": "AMM",
    "hurghada": "HRG",
    "sharm el-sheikh": "SSH",
    "gdansk": "GDN",
    "krakow": "KRK",
    "katowice": "KTW",
    "birmingham": "BHX",
    "leeds/bradford": "LBA",
}


if scanner is not None and not getattr(scanner.WizzAYCFClient, "_aycf_local_alias_patch", False):
    _original_resolve_station = scanner.WizzAYCFClient.resolve_station

    def _resolve_station_with_local_aliases(self, name):
        raw = str(name or "").strip()
        # Preserve captured aliases as highest priority.
        captured = self.station_ids.get(raw.casefold())
        if captured:
            return captured
        # Preserve native IATA behavior without needing any lookup.
        if len(raw) == 3 and raw.isalpha():
            return raw.upper()
        local = _LOCAL_STATION_ALIASES.get(raw.casefold())
        if local:
            # Cache it on the client as well so subsequent lookups are ordinary.
            self.station_ids.setdefault(raw.casefold(), local)
            self.station_ids.setdefault(local.casefold(), local)
            return local
        return _original_resolve_station(self, name)

    scanner.WizzAYCFClient.resolve_station = _resolve_station_with_local_aliases
    scanner.WizzAYCFClient._aycf_local_alias_patch = True
