import json
import pytest
from app import create_app

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_JOURNAL_DB_PATH', str(tmp_path / 'journal.sqlite3'))
    monkeypatch.setenv('AYCF_BIND_HOST', '127.0.0.1')
    monkeypatch.delenv('AYCF_APP_PASSWORD', raising=False)
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def headers(client):
    client.get('/places/')
    with client.session_transaction() as session:
        return {'X-CSRF-Token': session['csrf_token']}

def payload(**kwargs):
    return dict(country='GE', place='Mestia', status='visited', visited_on='2025-08-10', notes='Mountain paths', **kwargs)

def test_page_and_navigation(client):
    page=client.get('/places/')
    assert page.status_code == 200
    assert b'My places' in page.data
    assert b'places.js' in page.data
    assert client.get('/static/places-world.svg').status_code == 200
    assert client.get('/places/api').json == {'places': []}

def test_persistence_edit_export_delete(client):
    csrf=headers(client)
    response=client.post('/places/api',json=payload(),headers=csrf)
    assert response.status_code == 201
    ident=response.json['id']
    assert client.get('/places/api').json['places'][0]['place']=='Mestia'
    data=payload();data.update(status='wishlist', notes='<script>alert(1)</script>')
    assert client.put(f'/places/api/{ident}',json=data,headers=csrf).status_code==200
    saved=client.get('/places/export').json['places'][0]
    assert saved['visited_on']==''
    assert saved['notes']==data['notes']
    assert client.delete(f'/places/api/{ident}',headers=csrf).status_code==204
    assert client.get('/places/api').json['places']==[]
    assert client.delete(f'/places/api/{ident}',headers=csrf).status_code==404

def test_validation_and_duplicate_protection(client):
    csrf=headers(client)
    assert client.post('/places/api',json=payload()).status_code==400
    for change in ({'country':'ZZ'},{'visited_on':'3000-01-01'},{'status':'bad'},{'place':'x'*121},{'notes':[]},{'visited_on':'bad'}):
        data=payload();data.update(change)
        assert client.post('/places/api',json=data,headers=csrf).status_code==400
    assert client.post('/places/api',json=[],headers=csrf).status_code==400
    assert client.post('/places/api',json=payload(),headers=csrf).status_code==201
    assert client.post('/places/api',json=payload(),headers=csrf).status_code==409
    assert len(client.get('/places/api').json['places'])==1

def test_remote_access_requires_login(client,monkeypatch):
    monkeypatch.setenv('AYCF_APP_PASSWORD','test-password')
    for path in ('/places/','/places/api','/places/export'):
        assert client.get(path,environ_base={'REMOTE_ADDR':'192.0.2.8'}).status_code==401
