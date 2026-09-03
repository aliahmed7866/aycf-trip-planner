import os
import stat
from unittest.mock import patch

from recommendation_preferences import load_preferred_destinations, save_preferred_destinations, scan_scope_with_preferences
from stability_blueprint import _destination_preferred, _route_matches_search


def test_preferred_destinations_persist_privately_and_can_be_cleared(tmp_path):
    with patch.dict(os.environ, {"AYCF_CONFIG_DIR": str(tmp_path)}, clear=False):
        saved = save_preferred_destinations(["Cairo", " Cairo ", "Dubai", ""])
        assert saved == ["Cairo", "Dubai"]
        assert load_preferred_destinations() == ["Cairo", "Dubai"]
        mode = stat.S_IMODE((tmp_path / "recommendation_preferences.json").stat().st_mode)
        assert mode == 0o600
        save_preferred_destinations([])
        assert load_preferred_destinations() == []


def test_saved_destinations_are_attached_to_a_scan_scope_copy(tmp_path):
    original = {"origins": ["Liverpool"], "connection_hubs": ["Budapest"]}
    with patch.dict(os.environ, {"AYCF_CONFIG_DIR": str(tmp_path)}, clear=False):
        save_preferred_destinations(["Cairo"])
        enriched = scan_scope_with_preferences(original)
    assert enriched["preferred_destinations"] == ["Cairo"]
    assert "preferred_destinations" not in original


def test_route_search_matches_city_and_airport_codes():
    row = {"origin": "London Luton", "destination": "Cairo"}
    assert _route_matches_search(row, "Cairo")
    assert _route_matches_search(row, "LTN")
    assert _route_matches_search(row, "ltn cai")
    assert not _route_matches_search(row, "Liverpool")


def test_preferred_destination_matching_keeps_airports_exact_but_includes_london_summary():
    preferred = {"London Luton", "Cairo"}
    assert _destination_preferred("London Luton", preferred)
    assert _destination_preferred("London", preferred)
    assert _destination_preferred("Cairo", preferred)
    assert not _destination_preferred("London Gatwick", preferred)
