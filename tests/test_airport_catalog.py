from airport_catalog import CURRENT_WIZZ_IATA, airport_code, country_for, is_current_wizz_airport


CAPTURED_CODES = set("""
LPL LBA BHX TIA LWN EVN GYD CRL BNX SJJ TZL BOJ PDV SOF VAR DBV RJK SPU ZAD
LCA PFO PRG BLL CPH ALY HBE SPX HRG RMF SSH TLL TKU MLH BOD GNB LYS NCE BVA
ORY KUT BER CGN DTM HHN FDH HAM FKB FMM NUE STR ATH CHQ CFU HER KLX EFL JMK
RHO JTR JSI SKG ZTH BUD DEB KEF TLV AHO AOI BRI BLQ BDS CAG CTA CIY GOA SUF
LMP BGY MXP NAP OLB PMO PEG PSR PSA RMI CIA FCO TRS TRN VCE TSF VRN AMM PRN
KUN PLQ VNO MLA RMO TGD AGA RAK EIN MST OHD SKP AES BGO HAU OSL TRF SVG TOS
TRD GDN KTW KRK LUZ SZY POZ RZE SZZ WAW WMI RDO WRO FAO FNC LIS OPO BCM GHV
BBU OTP CLJ CND CRA IAS OMR SUJ SBZ SCV TGM TSR JED BEG INI BTS KSC TAT LJU
ALC LEI OVD BCN BIO CDT FUE LPA GRX IBZ MAD AGP MAH PMI SDR SCQ SVQ TFS TFN
VLC ZAZ GOT MMX ARN NYO BSL ESB AYT DLM IST AUH DXB ABZ GLA LGW LTN
""".split())


def test_catalog_exactly_matches_captured_physical_airports():
    assert CURRENT_WIZZ_IATA == CAPTURED_CODES
    assert not ({'LON', 'MIL', 'OOS', 'PAR', 'ROM', 'VEN'} & CURRENT_WIZZ_IATA)


def test_new_and_corrected_station_names_resolve():
    assert airport_code('Mykonos') == 'JMK'
    assert airport_code('Alexandria') == 'ALY'
    assert airport_code('Alexandria (Borg El Arab)') == 'HBE'
    assert airport_code('Dubai') == 'DXB'
    assert airport_code('Bucharest Baneasa') == 'BBU'
    assert airport_code('Warsaw Radom') == 'RDO'
    assert country_for('Karlsruhe/Baden-Baden') == 'Germany'


def test_removed_airports_are_not_current_but_unknown_names_remain_unknown():
    assert not is_current_wizz_airport('London Stansted')
    assert not is_current_wizz_airport('Vienna')
    assert is_current_wizz_airport('Unresolved PDF station')
