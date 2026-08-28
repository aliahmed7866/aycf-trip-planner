"""Small AYCF runtime extensions loaded automatically by Python startup.

Python imports ``sitecustomize`` automatically when this repository is on
``sys.path`` (as it is for the Termux entry points). Keep stable runtime hooks
here so the Flask app and scanner receive local-only behavior before the main
application is created.
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
        captured = self.station_ids.get(raw.casefold())
        if captured:
            return captured
        if len(raw) == 3 and raw.isalpha():
            return raw.upper()
        local = _LOCAL_STATION_ALIASES.get(raw.casefold())
        if local:
            self.station_ids.setdefault(raw.casefold(), local)
            self.station_ids.setdefault(local.casefold(), local)
            return local
        return _original_resolve_station(self, name)

    scanner.WizzAYCFClient.resolve_station = _resolve_station_with_local_aliases
    scanner.WizzAYCFClient._aycf_local_alias_patch = True


# Install persistent in-app password management before app.create_app() builds
# the Flask instance. Fail open to the existing AYCF_APP_PASSWORD behavior if a
# local dependency is unavailable, so unrelated CLI/scanner commands still run.
try:
    from password_manager import install_flask_password_manager
    install_flask_password_manager()
except Exception:
    pass
