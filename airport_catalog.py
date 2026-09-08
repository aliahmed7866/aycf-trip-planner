"""Offline country grouping for the scanner's station aliases, not route discovery.

Only the current AYCF PDF can create scan edges. Unknown stations remain visible
under Other / unmapped and can still be excluded by name.
"""
from station_resolver import FALLBACK_IATA, normalize_name
from functools import lru_cache

COUNTRY_CODES = {
    "Albania": "TIA", "Armenia": "EVN", "Austria": "VIE",
    "Azerbaijan": "GYD", "Belgium": "CRL BRU", "Bosnia and Herzegovina": "SJJ TZL BNX",
    "Bulgaria": "SOF VAR BOJ", "Croatia": "ZAG SPU DBV",
    "Cyprus": "LCA PFO", "Czechia": "PRG BRQ", "Denmark": "CPH BLL",
    "Egypt": "CAI SPX HRG SSH HBE RMF", "Estonia": "TLL", "Finland": "HEL TKU",
    "France": "BVA CDG ORY NCE LYS GNB MLH", "Georgia": "KUT TBS BUS",
    "Germany": "BER CGN DTM HHN HAM FMM MUC NUE FRA",
    "Greece": "ATH CFU HER RHO SKG ZTH CHQ JTR JMK",
    "Hungary": "BUD DEB", "Iceland": "KEF", "Israel": "TLV",
    "Italy": "AHO BRI BLQ CTA FCO MXP NAP PMO PSA TRN VCE VRN BGY CIA",
    "Jordan": "AMM AQJ", "Kazakhstan": "NQZ ALA SCO", "Kosovo": "PRN",
    "Kuwait": "KWI", "Latvia": "RIX", "Lithuania": "KUN VNO PLQ",
    "Malta": "MLA", "Moldova": "RMO KIV", "Montenegro": "TGD TIV",
    "Morocco": "AGA RAK CMN FEZ RBA TNG", "Netherlands": "AMS EIN MST",
    "North Macedonia": "SKP OHD", "Norway": "OSL TRF BGO TOS AES SVG TRD",
    "Poland": "GDN KTW KRK LCJ POZ RZE WAW WRO WMI SZY LUZ SZZ BZG",
    "Portugal": "FAO FNC LIS OPO", "Romania": "OTP CLJ CRA IAS SUJ SBZ SCV TSR BCM TGM BBU",
    "Saudi Arabia": "JED RUH DMM MED", "Serbia": "BEG INI",
    "Slovakia": "BTS KSC", "Slovenia": "LJU",
    "Spain": "ALC BCN BIO LPA MAD AGP SVQ TFS VLC PMI GRO FUE ACE SDR",
    "Sweden": "GOT MMX ARN NYO", "Switzerland": "BSL GVA ZRH",
    "Türkiye": "AYT BJV DLM IST SAW ADB ESB",
    "United Arab Emirates": "AUH DWC DXB SHJ RKT",
    "United Kingdom": "BHX LBA LPL LTN LGW STN MAN EDI ABZ GLA BRS CWL",
    "Uzbekistan": "TAS SKD",
}
COUNTRY_BY_IATA = {code: country for country, codes in COUNTRY_CODES.items() for code in codes.split()}
EXTRA_ALIASES = {
    "alexandria": "HBE", "aqaba": "AQJ", "sharjah": "SHJ", "ras al khaimah": "RKT",
    "jeddah": "JED", "riyadh": "RUH", "dammam": "DMM", "medina": "MED", "madinah": "MED",
    "kuwait": "KWI", "kuwait city": "KWI", "ohrid": "OHD", "bergen": "BGO",
    "tromso": "TOS", "alesund": "AES", "aalesund": "AES", "keflavik": "KEF",
    "basel mulhouse": "BSL", "basel and mulhouse": "BSL",
}
ALIASES = {normalize_name(name): code for name, code in {**FALLBACK_IATA, **EXTRA_ALIASES}.items()}


@lru_cache(maxsize=4096)
def airport_code(name):
    text = str(name or "").strip()
    # London is a group, never a synonym for Luton in exclusion policy.
    if normalize_name(text) == "london":
        return ""
    return text.upper() if len(text) == 3 and text.isalpha() else ALIASES.get(normalize_name(text), "")


@lru_cache(maxsize=4096)
def country_for(name):
    if normalize_name(name) == "london":
        return "United Kingdom"
    return COUNTRY_BY_IATA.get(airport_code(name), "Other / unmapped")
