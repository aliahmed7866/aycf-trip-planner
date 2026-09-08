"""Personal travel journal, stored alongside the configured AYCF database."""
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from flask import Blueprint, abort, jsonify, render_template, request
from cache_db import default_db_path

CATALOG = json.loads((Path(__file__).parent / 'static/places-countries.json').read_text())
COUNTRIES = {c['code']: c for c in CATALOG}

@contextmanager
def connect():
    path = Path(os.environ.get('AYCF_JOURNAL_DB_PATH') or Path(default_db_path()).with_name('travel-journal.sqlite3'))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=15)
    db.row_factory = sqlite3.Row
    try:
        db.execute('''CREATE TABLE IF NOT EXISTS places (
            id INTEGER PRIMARY KEY, country TEXT NOT NULL, place TEXT NOT NULL,
            status TEXT NOT NULL, visited_on TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '', UNIQUE(country, place))''')
        yield db
        db.commit()
    finally:
        db.close()

def validate(data):
    if not isinstance(data, dict):
        raise ValueError('Enter a place to save.')
    result = {}
    for key, limit in [('country', 3), ('place', 120), ('status', 12), ('visited_on', 10), ('notes', 4000)]:
        value = data.get(key, '')
        if not isinstance(value, str) or len(value.strip()) > limit:
            raise ValueError(f'Please check {key.replace("_", " ")}.')
        result[key] = value.strip()
    if result['country'] not in COUNTRIES:
        raise ValueError('Choose a country or territory from the list.')
    if result['status'] not in ('visited', 'wishlist'):
        raise ValueError('Choose visited or wishlist.')
    if result['visited_on']:
        try:
            parsed = date.fromisoformat(result['visited_on'])
        except ValueError:
            raise ValueError('Enter a valid visit date.') from None
        if parsed > date.today():
            raise ValueError('A visit date cannot be in the future.')
    if result['status'] == 'wishlist':
        result['visited_on'] = ''
    return result

def create_journal_blueprint(csrf_ok):
    bp = Blueprint('places', __name__, url_prefix='/places')

    @bp.before_request
    def protect_changes():
        if request.method != 'GET' and not csrf_ok():
            return jsonify(error='Your session expired. Refresh the page and try again.'), 400

    @bp.get('/')
    def page():
        return render_template('places.html', countries=CATALOG)

    @bp.get('/api')
    def records():
        with connect() as db:
            rows = [dict(r) for r in db.execute('SELECT * FROM places ORDER BY visited_on DESC, id DESC')]
        response = jsonify(places=rows)
        response.headers['Cache-Control'] = 'no-store'
        return response

    @bp.route('/api', methods=['POST'])
    @bp.route('/api/<int:record_id>', methods=['PUT'])
    def save(record_id=None):
        try:
            values = validate(request.get_json(silent=True))
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        try:
            with connect() as db:
                if record_id is None:
                    cursor = db.execute('INSERT INTO places(country,place,status,visited_on,notes) VALUES(:country,:place,:status,:visited_on,:notes)', values)
                    record_id = cursor.lastrowid
                else:
                    cursor = db.execute('UPDATE places SET country=:country,place=:place,status=:status,visited_on=:visited_on,notes=:notes WHERE id=:id', dict(values, id=record_id))
                    if not cursor.rowcount:
                        abort(404)
        except sqlite3.IntegrityError:
            return jsonify(error='This place is already in your journal. Edit its existing entry.'), 409
        return jsonify(id=record_id), 201 if request.method == 'POST' else 200

    @bp.delete('/api/<int:record_id>')
    def remove(record_id):
        with connect() as db:
            if not db.execute('DELETE FROM places WHERE id=?', (record_id,)).rowcount:
                abort(404)
        return '', 204

    @bp.get('/export')
    def export():
        with connect() as db:
            rows = [dict(r) for r in db.execute('SELECT * FROM places ORDER BY id')]
        return jsonify(version=1, places=rows), 200, {'Content-Disposition': 'attachment; filename=aycf-my-places.json', 'Cache-Control': 'no-store'}

    return bp
