"""Add new default apps while preserving installed apps and custom ports."""
import json
import os
from pathlib import Path
import sys
import tempfile


def merge(example, destination):
    defaults = json.loads(example.read_text())
    payload = json.loads(destination.read_text()) if destination.exists() else {'apps': []}
    if not isinstance(payload, dict) or not isinstance(payload.get('apps'), list):
        raise ValueError('Invalid apps registry; refusing to overwrite it.')
    known = {item['id']:item for item in defaults['apps']}
    apps = []
    for item in payload['apps']:
        # Existing custom values win; new default capabilities fill missing fields.
        apps.append({**known.pop(item['id'], {}), **item})
    payload['apps'] = apps + list(known.values())
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=destination.parent, delete=False) as f:
        json.dump(payload, f, indent=2)
        f.write('\n')
    os.replace(f.name, destination)


if __name__ == '__main__':
    merge(Path(sys.argv[1]), Path(sys.argv[2]))
