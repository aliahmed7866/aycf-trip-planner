"""Interactive local configuration; never prints or commits credentials."""
from getpass import getpass
from pathlib import Path
import json
import os
import re
import secrets
import sqlite3
from werkzeug.security import generate_password_hash
from app import ROOT, ART


def main():
    folder = Path(os.environ.get('MAHRUKH_INSTANCE', str(ROOT / 'instance'))).resolve()
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    folder.chmod(0o700)
    target = folder / 'config.json'
    old = json.loads(target.read_text()) if target.exists() else {}
    config = dict(old)
    config.setdefault('secret_key', secrets.token_urlsafe(48))
    while True:
        password = getpass('Admin password (12+ characters' + ('; blank keeps current' if old else '') + '): ')
        if not password and old:
            break
        if len(password) < 12:
            print('Use at least 12 characters.'); continue
        if password != getpass('Repeat password: '):
            print('Passwords did not match.'); continue
        config['admin_hash'] = generate_password_hash(password, method='pbkdf2:sha256')
        config['secret_key'] = secrets.token_urlsafe(48)
        break
    while True:
        value = input('Seller WhatsApp number, country code + digits (e.g. 923001234567); blank keeps current/disabled, - disables: ').strip()
        phone = '' if value == '-' else (re.sub(r'[\s+()-]', '', value) if value else config.get('seller_phone', ''))
        if not phone or re.fullmatch(r'[1-9]\d{9,14}', phone):
            config['seller_phone'] = phone; break
        print('Use 10–15 digits, including country code; no leading zero.')
    while True:
        raw = input(f"Local port [{config.get('port', 8085)}]: ").strip()
        try:
            port = int(raw or config.get('port', 8085))
            if not 1024 <= port <= 65535:
                raise ValueError
            config['port'] = port; break
        except ValueError:
            print('Use a port between 1024 and 65535.')
    tmp = target.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(config, stream, indent=2)
    os.replace(tmp, target)
    target.chmod(0o600)
    with sqlite3.connect(folder / 'mahrukh.sqlite3') as db:
        db.executescript((ROOT / 'schema.sql').read_text())
        if not old and db.execute('SELECT count(*) FROM products').fetchone()[0] == 0:
            seed(db)
    (folder / 'mahrukh.sqlite3').chmod(0o600)
    print(f"Mahrukh configured at http://127.0.0.1:{port}. Run python run.py to start.")
    print('If already running, restart the mahrukh service to apply configuration changes.')


def seed(db):
    samples = [
        ('Mehr · Emerald', 'Stitched / Pret', 6490, 7490, 'Cotton silk · embroidered neckline', 'An emerald shalwar kameez with botanical embroidery and a softly draped dupatta.', ['S','M','L','XL']),
        ('Gul · Rose', 'Unstitched', 4290, 0, 'Lawn · printed three-piece concept', 'Rose-toned fabrics with a delicate floral vocabulary, ready for your own silhouette.', ['Custom Unstitched']),
        ('Noor · Midnight', 'Abayas', 7990, 0, 'Nida · gold embroidered accents', 'A flowing midnight silhouette with considered gold detailing along the opening and sleeves.', ['S','M','L','XL']),
        ('Surkh · Celebration', 'Festive Wear', 15900, 18500, 'Raw silk · embroidered festive concept', 'A ruby lehenga, embroidered bodice and light dupatta for moments worth celebrating.', ['S','M','L','XL']),
        ('Chand · Ivory', 'Luxury Formals', 18900, 0, 'Organza · embroidered formal concept', 'An elongated ivory silhouette inspired by luminous evenings and the quiet beauty of heritage.', ['S','M','L','XL']),
        ('Neel · Indigo', 'Stitched / Pret', 5990, 6990, 'Cotton · botanical embroidery', 'An indigo everyday ensemble, finished with delicate botanical motifs and a matching dupatta.', ['S','M','L','XL']),
        ('Mehfil · Saffron', 'Festive Wear', 12900, 0, 'Silk blend · festive peshwas concept', 'A full, saffron skirt and embroidered bodice that bring movement and warmth to every celebration.', ['S','M','L','XL']),
        ('Shaam · Plum', 'Luxury Formals', 16900, 19500, 'Chiffon · embroidered formal concept', 'An evening palette of plum and antique gold, with a long silhouette and floating dupatta.', ['S','M','L','XL']),
    ]
    for item, art in zip(samples, ART):
        name, category, price, original, fabric, description, sizes = item
        db.execute('INSERT INTO products(name,category,price,compare_price,fabric,description,sizes,images) VALUES(?,?,?,?,?,?,?,?)',
                   (name, category, price, original, fabric, description + '\n\nIllustrated sample design. Confirm actual fabric, included pieces and measurements with the seller.', json.dumps(sizes), json.dumps(['/static/art/'+art+'.svg'])))

if __name__ == '__main__':
    main()
