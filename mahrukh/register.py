"""Register Mahrukh with the existing AYCF hub without changing other apps."""
from pathlib import Path
import json
import os
import socket
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent


def merge_registry(path, port):
    payload = json.loads(path.read_text()) if path.exists() else {'apps': []}
    if not isinstance(payload, dict) or not isinstance(payload.get('apps'), list):
        raise ValueError('Invalid hub registry; refusing to overwrite it.')
    if any(not isinstance(a, dict) or not a.get('id') for a in payload['apps']):
        raise ValueError('Invalid hub app entry; refusing to overwrite it.')
    collisions = [a['id'] for a in payload['apps'] if a['id'] != 'mahrukh' and str(a.get('port')) == str(port)]
    if collisions:
        raise ValueError(f'Port {port} is registered to {", ".join(collisions)}. Choose another port with setup.py.')
    # Preserve hub defaults on the first registration, including apps not currently running.
    entry = dict(id='mahrukh', name='Mahrukh', icon='✦', accent='amber',
                 description='Pakistani clothing, collections and WhatsApp orders', service='mahrukh',
                 port=port, health_url=f'http://127.0.0.1:{port}/health', open_url=f'http://127.0.0.1:{port}',
                 install_command=['bash', str(ROOT.parent / 'termux/install-mahrukh.sh')])
    existing = next((a for a in payload['apps'] if a['id'] == 'mahrukh'), {})
    entry = {**existing, **entry}
    payload['apps'] = [entry if a['id'] == 'mahrukh' else a for a in payload['apps']]
    if not existing:
        payload['apps'].append(entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2)
        handle.write('\n')
    os.chmod(handle.name, 0o600)
    os.replace(handle.name, path)


if __name__ == '__main__':
    data = Path(os.environ.get('MAHRUKH_INSTANCE', str(ROOT / 'instance')))
    config = json.loads((data / 'config.json').read_text())
    port = int(config.get('port', 8085))
    if '--check-port' in sys.argv:
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(('127.0.0.1', port))
            except OSError:
                raise SystemExit(f'Port {port} is in use. Run setup.py and choose a free port; no other app was stopped.')
    elif '--health' in sys.argv:
        for attempt in range(10):
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2) as response:
                    if json.load(response).get('service') == 'mahrukh':
                        print(f'Mahrukh is ready: http://127.0.0.1:{port}'); break
            except (OSError, ValueError):
                pass
            time.sleep(1)
        else:
            raise SystemExit('Mahrukh did not become healthy. Check: sv status mahrukh')
    else:
        default_path = Path(os.environ.get('AYCF_CONFIG_DIR', str(Path.home() / '.config/aycf'))) / 'apps.json'
        path = Path(os.environ.get('AYCF_ADMIN_REGISTRY', str(default_path)))
        if not path.exists():
            # Supply repository defaults to merge without writing a partial registry first.
            defaults = ROOT.parent / 'termux/apps.json.example'
            if defaults.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as f:
                    f.write(defaults.read_text())
                try:
                    merge_registry(Path(f.name), port)
                    os.replace(f.name, path)
                finally:
                    Path(f.name).unlink(missing_ok=True)
            else:
                merge_registry(path, port)
        else:
            merge_registry(path, port)
        print(f'Mahrukh registered on port {port}; other hub entries preserved.')
