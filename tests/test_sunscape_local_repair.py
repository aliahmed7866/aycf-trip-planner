"""Service repair must not require discarding unrelated local source edits."""
import os
from pathlib import Path
import subprocess
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'termux/install-sunscape.sh'


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()


@pytest.mark.parametrize('helper', [True, False])
def test_local_edits_are_preserved_during_repair(tmp_path, helper):
    root = tmp_path / 'sunscape'
    root.mkdir()
    git(root, 'init', '-b', 'main')
    (root/'termux').mkdir()
    (root/'app.py').write_text('original\n')
    (root/'termux/install-service.sh').write_text('printf "repair port=%s\\n" "${SUNSCAPE_PORT:-automatic}"\n')
    if helper:
        (root/'termux/configure.py').write_text('# recovery helper\n')
    git(root, 'add', '.')
    git(root, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'fixture')
    before = git(root, 'rev-parse', 'HEAD')
    (root/'app.py').write_text('my local edits\n')
    # No remote exists: dirty repair must not fetch or merge.
    result = subprocess.run(['bash', str(SCRIPT)], capture_output=True, text=True,
                            env={**os.environ, 'SUNSCAPE_APP_DIR':str(root), 'SUNSCAPE_PORT':'8081'})
    assert (root/'app.py').read_text() == 'my local edits\n'
    assert git(root, 'rev-parse', 'HEAD') == before
    assert not git(root, 'stash', 'list')
    if helper:
        assert result.returncode == 0, result.stderr
        assert 'repair port=automatic' in result.stdout
        assert 'Source update skipped' in result.stdout
    else:
        assert result.returncode != 0
        assert 'lacks the new Sunscape recovery helper' in result.stderr
        assert 'app.py' in result.stderr
        assert 'repair port=' not in result.stdout
