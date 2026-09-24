"""Run Mahrukh using a lightweight pure-Python WSGI server."""
import json
import os
import socket
from pathlib import Path
from waitress import serve
from app import ROOT, create_app

if __name__ == '__main__':
    os.umask(0o077)
    folder = Path(os.environ.get('MAHRUKH_INSTANCE', str(ROOT / 'instance')))
    config_path = folder / 'config.json'
    if not config_path.exists():
        raise SystemExit('Run python setup.py first.')
    config = json.loads(config_path.read_text())
    port = int(os.environ.get('MAHRUKH_PORT', config.get('port', 8085)))
    # Bind the actual serving socket once: no gap between probing and serving.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        listener.bind(('127.0.0.1', port))
        listener.listen(128)
    except OSError as exc:
        listener.close()
        raise SystemExit(f'Cannot bind Mahrukh to port {port}: {exc}. Choose another port with python setup.py; other apps were not stopped.')
    print(f'Mahrukh: http://127.0.0.1:{port} | Seller studio: /admin', flush=True)
    serve(create_app(), sockets=[listener], threads=4, max_request_body_size=65536)
