"""Small local planner preferences; no account or network requests."""
from datetime import datetime, time, timedelta
import json
import os
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

UK_ZONE = ZoneInfo('Europe/London')
LIMITS = {
    'min_stay_hours': {'24', '30', '36', '48'},
    'min_daytime_hours': {'8', '10', '12', '16', '24'},
    'max_stops': {'0', '1', '2'},
    'min_transfer_minutes': {'120', '150', '180', '240'},
    'max_layover_minutes': {'360', '720', '1440', '2880', '0'},
    'max_journey_minutes': {'0', '360', '480', '720', '1440'},
}


def _read(path):
    try:
        if path.stat().st_size > 32768:
            return {}
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _path():
    return Path(os.environ.get('AYCF_CONFIG_DIR', Path.home() / '.config/aycf')) / 'trip_preferences.json'


def load_trip_preferences(options):
    raw = _read(_path())
    saved = {key: value for key, choices in LIMITS.items()
             if isinstance(value := raw.get(key), str) and value in choices}
    for key in ('origins', 'returns'):
        values = raw.get(key)
        if isinstance(values, list):
            eligible = [name for name in options if name in values]
            if eligible:
                saved[key] = eligible
    return saved


def save_trip_preferences(origins, returns, values):
    path = _path()
    settings = {key: value for key, choices in LIMITS.items()
                if (value := values.get(key)) in choices}
    settings.update(origins=origins, returns=returns)
    # Deadlines and destination are deliberately not remembered: tomorrow's
    # search must never silently inherit a now-expired date or narrow filter.
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.trip-preferences-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(settings, stream)
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def weekend_presets(now=None):
    now = (now or datetime.now(UK_ZONE)).astimezone(UK_ZONE)
    # On Saturday/Sunday "This weekend" means the weekend already underway.
    friday = now.date() + timedelta(days=4 - now.weekday())
    if now.weekday() < 4:
        friday = now.date() + timedelta(days=(4 - now.weekday()) % 7)
    def window(day):
        leave = max(now.replace(second=0, microsecond=0), datetime.combine(day, time(16), UK_ZONE))
        return {'leave_after': leave.strftime('%Y-%m-%dT%H:%M'),
                'return_by': datetime.combine(day + timedelta(days=3), time(9), UK_ZONE).strftime('%Y-%m-%dT%H:%M')}
    return {'weekend': window(friday), 'next-weekend': window(friday + timedelta(days=7))}


def planner_connection(connected):
    from credential_vault import CredentialVault
    try:
        saved = CredentialVault().exists()
    except (OSError, RuntimeError):
        saved = False
    state_dir = Path(os.environ.get('AYCF_STATE_DIR', Path.home() / '.local/share/aycf'))
    auth = _read(state_dir / 'android-auth.json')
    blocked = auth.get('state') == 'attention_required'
    if blocked:
        return {'label': 'Wizz needs your attention', 'detail': 'Open Menu → Wizz login settings or complete Wizz verification.', 'can_scan': connected or saved, 'tone': 'warning'}
    if saved:
        return {'label': 'Wizz login remembered', 'detail': 'AYCF checks your connection before scanning and renews it when possible.', 'can_scan': True, 'tone': 'success'}
    if connected:
        return {'label': 'Wizz session saved', 'detail': 'Availability is checked when you scan. Seats can change before booking.', 'can_scan': True, 'tone': 'success'}
    return {'label': 'Wizz setup needed', 'detail': 'Connect your Wizz wallet to start finding flights.', 'can_scan': False, 'tone': 'warning'}
