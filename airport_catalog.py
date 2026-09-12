"""Offline country grouping for the scanner's station aliases, not route discovery.

Only the current AYCF PDF can create scan edges. Unknown stations remain visible
under Other / unmapped and can still be excluded by name.
"""
from station_resolver import FALLBACK_IATA, normalize_name
from functools import lru_cache

# Physical departure airports shown by Wizz's station picker on 2026-09-12.
# City-wide pseudo stations such as LON, MIL, OOS, PAR, ROM and VEN are omitted.
COUNTRY_CODES = {
    "Albania": "TIA", "Armenia": "LWN EVN", "Azerbaijan": "GYD",
    "Belgium": "CRL", "Bosnia and Herzegovina": "BNX SJJ TZL",
    "Bulgaria": "BOJ PDV SOF VAR", "Croatia": "DBV RJK SPU ZAD",
    "Cyprus": "LCA PFO", "Czechia": "PRG", "Denmark": "BLL CPH",
    "Egypt": "ALY HBE SPX HRG RMF SSH", "Estonia": "TLL", "Finland": "TKU",
    "France": "MLH BOD GNB LYS NCE BVA ORY", "Georgia": "KUT",
    "Germany": "BER CGN DTM HHN FDH HAM FKB FMM NUE STR",
    "Greece": "ATH CHQ CFU HER KLX EFL JMK RHO JTR JSI SKG ZTH",
    "Hungary": "BUD DEB", "Iceland": "KEF", "Israel": "TLV",
    "Italy": "AHO AOI BRI BLQ BDS CAG CTA CIY GOA SUF LMP BGY MXP NAP OLB PMO PEG PSR PSA RMI CIA FCO TRS TRN VCE TSF VRN",
    "Jordan": "AMM", "Kosovo": "PRN", "Lithuania": "KUN PLQ VNO",
    "Malta": "MLA", "Moldova": "RMO", "Montenegro": "TGD",
    "Morocco": "AGA RAK", "Netherlands": "EIN MST", "North Macedonia": "OHD SKP",
    "Norway": "AES BGO HAU OSL TRF SVG TOS TRD",
    "Poland": "GDN KTW KRK LUZ SZY POZ RZE SZZ WAW WMI RDO WRO",
    "Portugal": "FAO FNC LIS OPO",
    "Romania": "BCM GHV BBU OTP CLJ CND CRA IAS OMR SUJ SBZ SCV TGM TSR",
    "Saudi Arabia": "JED", "Serbia": "BEG INI", "Slovakia": "BTS KSC TAT",
    "Slovenia": "LJU",
    "Spain": "ALC LEI OVD BCN BIO CDT FUE LPA GRX IBZ MAD AGP MAH PMI SDR SCQ SVQ TFS TFN VLC ZAZ",
    "Sweden": "GOT MMX ARN NYO", "Switzerland": "BSL",
    "Türkiye": "ESB AYT DLM IST", "United Arab Emirates": "AUH DXB",
    "United Kingdom": "LPL LBA BHX ABZ GLA LGW LTN",
}
COUNTRY_BY_IATA = {code: country for country, codes in COUNTRY_CODES.items() for code in codes.split()}
CURRENT_WIZZ_IATA = frozenset(COUNTRY_BY_IATA)
EXTRA_ALIASES = {
    "alexandria": "ALY", "aqaba": "AQJ", "sharjah": "SHJ", "ras al khaimah": "RKT",
    "jeddah": "JED", "riyadh": "RUH", "dammam": "DMM", "medina": "MED", "madinah": "MED",
    "kuwait": "KWI", "kuwait city": "KWI", "ohrid": "OHD", "bergen": "BGO",
    "tromso": "TOS", "alesund": "AES", "aalesund": "AES", "keflavik": "KEF",
    "basel mulhouse": "BSL", "basel and mulhouse": "BSL", "mykonos": "JMK",
    "alexandria borg el arab": "HBE", "cairo sphinx": "SPX", "bourgas": "BOJ",
    "gyumri": "LWN", "rijeka": "RJK", "zadar": "ZAD", "bordeaux": "BOD",
    "grenoble": "GNB", "friedrichshafen": "FDH", "karlsruhe baden baden": "FKB",
    "stuttgart": "STR", "kalamata": "KLX", "kefalonia": "EFL", "skiathos": "JSI",
    "ancona": "AOI", "brindisi": "BDS", "cagliari": "CAG", "comiso": "CIY",
    "genoa": "GOA", "lamezia terme": "SUF", "lampedusa": "LMP",
    "milan bergamo": "BGY", "olbia": "OLB", "perugia": "PEG", "pescara": "PSR",
    "rimini": "RMI", "rome ciampino": "CIA", "trieste": "TRS",
    "venice treviso": "TSF", "haugesund": "HAU", "poprad tatry": "TAT",
    "asturias": "OVD", "castellon": "CDT", "granada": "GRX", "ibiza": "IBZ",
    "menorca": "MAH", "santander": "SDR", "santiago de compostela": "SCQ",
    "tenerife norte": "TFN", "zaragoza": "ZAZ", "brasov": "GHV",
    "bucharest baneasa": "BBU", "constanta": "CND", "oradea": "OMR",
    "warsaw radom": "RDO", "almeria": "LEI", "dubai": "DXB",
}
ALIASES = {normalize_name(name): code for name, code in {**FALLBACK_IATA, **EXTRA_ALIASES}.items()}
RAW_ALIASES = {"alexandria (borg el arab)": "HBE"}


@lru_cache(maxsize=4096)
def airport_code(name):
    text = str(name or "").strip()
    # London is a group, never a synonym for Luton in exclusion policy.
    if normalize_name(text) == "london":
        return ""
    return (text.upper() if len(text) == 3 and text.isalpha()
            else RAW_ALIASES.get(text.casefold(), ALIASES.get(normalize_name(text), "")))


@lru_cache(maxsize=4096)
def country_for(name):
    if normalize_name(name) == "london":
        return "United Kingdom"
    return COUNTRY_BY_IATA.get(airport_code(name), "Other / unmapped")


def is_current_wizz_airport(name):
    """False only when a resolved physical airport is absent from the captured picker."""
    code = airport_code(name)
    return not code or code in CURRENT_WIZZ_IATA
