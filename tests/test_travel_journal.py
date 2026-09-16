"""Retiring AYCF Places must not touch the user's standalone or legacy data."""
from types import SimpleNamespace
import pandas as pd
import pytest
import app as web


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_JOURNAL_DB_PATH', str(tmp_path / 'travel-journal.sqlite3'))
    monkeypatch.setenv('AYCF_DB_PATH', str(tmp_path / 'scan.sqlite3'))
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path / 'config'))
    monkeypatch.setenv('AYCF_BIND_HOST', '127.0.0.1')
    monkeypatch.delenv('AYCF_APP_PASSWORD', raising=False)
    monkeypatch.setattr(web, '_cache_dir', lambda: str(tmp_path))
    monkeypatch.setattr(web, 'update_data_if_needed', lambda **_: SimpleNamespace(data_dir=str(tmp_path)))
    frame=pd.DataFrame([{'departure_from':'London','departure_to':'Budapest','data_generated':'2026-09-16T07:00:00'}])
    monkeypatch.setattr(web, 'CurrentRouteGraph', lambda _: SimpleNamespace(cities=lambda:['London','Budapest'], latest_frame=lambda:frame))
    return web.create_app().test_client()


def test_legacy_places_routes_and_assets_are_removed_without_deleting_data(client, tmp_path):
    journal = tmp_path / 'travel-journal.sqlite3'
    original = b'Preserved legacy travel database'
    journal.write_bytes(original)
    assert b'My places' not in client.get('/').data
    for path in ('/places/', '/places/api', '/places/export', '/static/places.js', '/static/places-world.svg'):
        assert client.get(path).status_code == 404
    for method in ('post', 'put', 'delete'):
        assert getattr(client, method)('/places/api', json={}).status_code == 404
    assert journal.read_bytes() == original
    assert not any(rule.endpoint.startswith('places.') for rule in client.application.url_map.iter_rules())
