"""Read-only log exports for the authenticated System status console."""
from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import zipfile

from diagnostic_text import redact_text

LOG_FILES = {'supervisor': 'supervisor.log', 'scan': 'manual-morning.log', 'auth': 'auth-repair.log'}
MAX_LOG_BYTES = 2 * 1024 * 1024


def log_export(log_dir: Path, key: str):
    """Export an allowlisted regular file, capped to its newest 2 MiB."""
    name = LOG_FILES[key]
    metadata = {'file': name, 'exists': False, 'truncated': False}
    try:
        fd = os.open(log_dir / name, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise OSError('Not a regular log')
            metadata.update(exists=True, source_bytes=info.st_size,
                            updated_at=datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat(),
                            truncated=info.st_size > MAX_LOG_BYTES)
            offset = max(0, info.st_size - MAX_LOG_BYTES)
            handle.seek(offset)
            data = handle.read(MAX_LOG_BYTES)
            if offset:
                data = data.partition(b'\n')[2]
    except OSError:
        metadata.update(exists=False, state='missing_or_unreadable')
        return metadata, f'{name}: log missing or unreadable.\n'
    header = f"AYCF {name}\nLog last modified (UTC): {metadata['updated_at']}\n"
    if metadata['truncated']:
        header += 'TRUNCATED: only the newest 2 MiB, starting at a complete line, are included.\n'
    header += 'Common credential patterns are masked. Review before sharing.\n\n'
    return metadata, header + redact_text(data.decode('utf-8', errors='replace'))


def installed_revision(root):
    try:
        value = subprocess.check_output(['git', '--no-pager', 'rev-parse', '--short=10', 'HEAD'],
            cwd=root, text=True, stderr=subprocess.DEVNULL, timeout=2).strip()
        return value if re.fullmatch('[0-9a-f]{7,40}', value) else 'unknown'
    except (OSError, subprocess.SubprocessError):
        return 'unknown'


def make_bundle(log_dir, root, rate_limit, events):
    output = io.BytesIO()
    logs = {}
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for key, name in LOG_FILES.items():
            metadata, text = log_export(log_dir, key)
            logs[key] = metadata
            archive.writestr(name + '.txt', text)
        report = {'generated_at': datetime.now(timezone.utc).isoformat(),
                  'installed_revision': installed_revision(root),
                  'logs': logs, 'rate_limit': rate_limit, 'rate_limit_events': events,
                  'notes': [
                      'Read-only snapshot: this download does not contact Wizz or resume scans.',
                      'Installed revision describes files on disk; an older worker may still be running.',
                      'Log modification times are not proof that a task is currently running.',
                      'Missing response previews were not captured by the version that recorded the event.',
                      'Response previews are bounded and redacted, not raw HTTP captures.',
                      'Cooldown source distinguishes Wizz Retry-After from AYCF local policy.',
                      'Counts include managed attempts only; browser activity is excluded.',
                      'No environment files, session vaults, request headers or flight database are included.',
                      'Common credential patterns are masked; review before sharing.']}
        # Keep structured numeric diagnostics intact; free-text messages are
        # already bounded/redacted by the capture path.
        archive.writestr('diagnostics.json', json.dumps(report, ensure_ascii=False, indent=2))
    output.seek(0)
    return output
