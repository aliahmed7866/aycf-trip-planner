from datetime import datetime
import json

import pytest

from planner_preferences import (UK_ZONE, load_trip_preferences, planner_connection,
                                 save_trip_preferences, weekend_presets)


@pytest.mark.parametrize('now,leave,back,next_leave', [
    ('2026-11-24T12:00', '2026-11-27T16:00', '2026-11-30T09:00', '2026-12-04T16:00'),
    ('2026-11-27T18:30', '2026-11-27T18:30', '2026-11-30T09:00', '2026-12-04T16:00'),
    ('2026-11-29T10:00', '2026-11-29T10:00', '2026-11-30T09:00', '2026-12-04T16:00'),
    ('2026-10-23T10:00', '2026-10-23T16:00', '2026-10-26T09:00', '2026-10-30T16:00'),
    ('2026-03-27T10:00', '2026-03-27T16:00', '2026-03-30T09:00', '2026-04-03T16:00'),
])
def test_weekend_shortcuts_use_uk_wall_time_including_clock_changes(now, leave, back, next_leave):
    result = weekend_presets(datetime.fromisoformat(now).replace(tzinfo=UK_ZONE))
    assert result['weekend'] == {'leave_after': leave, 'return_by': back}
    assert result['next-weekend']['leave_after'] == next_leave


def test_preferences_never_restore_dates_destinations_or_excluded_airports():
    save_trip_preferences(['Liverpool', 'London Luton'], ['Liverpool'], {
        'max_stops': '1', 'min_stay_hours': '36', 'leave_after': '2026-11-24T16:00',
        'return_by': '2026-11-30T09:00', 'destination': 'Tirana',
    })
    assert load_trip_preferences(['London Luton']) == {
        'origins': ['London Luton'], 'max_stops': '1', 'min_stay_hours': '36',
    }


def test_corrupt_preferences_and_invalid_values_fall_back(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    path = tmp_path / 'trip_preferences.json'
    path.write_text('{broken')
    assert load_trip_preferences(['Liverpool']) == {}
    path.write_text(json.dumps({'origins': 'Liverpool', 'returns': [None], 'max_stops': [], 'min_stay_hours': '999'}))
    assert load_trip_preferences(['Liverpool']) == {}


def test_saved_login_without_session_can_start_recovery(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet
    from credential_vault import CredentialVault
    monkeypatch.setenv('AYCF_SESSION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    monkeypatch.setenv('AYCF_WIZZ_CREDENTIAL_FILE', str(tmp_path / 'login.enc'))
    assert not planner_connection(False)['can_scan']
    CredentialVault().save('test@example.com', 'test-only-password')
    assert planner_connection(False)['label'] == 'Wizz login remembered'
    assert planner_connection(False)['can_scan']
    assert 'test@example.com' not in str(planner_connection(False))
    monkeypatch.setenv('AYCF_STATE_DIR', str(tmp_path))
    (tmp_path / 'android-auth.json').write_text('{"state":"attention_required"}')
    assert planner_connection(True)['tone'] == 'warning'
    assert 'needs your attention' in planner_connection(True)['label']
