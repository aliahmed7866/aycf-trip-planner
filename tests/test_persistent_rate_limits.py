"""No live requests: durable cooldowns and real cross-process reservations."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

import wizz_rate_limit as limits


@pytest.fixture
def clock(monkeypatch):
    value = [100000.0]
    monkeypatch.setattr(limits.time, 'time', lambda: value[0])
    monkeypatch.setattr(limits.time, 'sleep', lambda seconds: value.__setitem__(0, value[0] + seconds))
    return value


def test_read_missing_state_does_not_create_file_or_directory(tmp_path, monkeypatch):
    path = tmp_path / 'missing' / 'limits.sqlite3'
    monkeypatch.setenv('AYCF_WIZZ_RATE_LIMIT_PATH', str(path))
    assert limits.rate_limit_status() == {
        'blocked': False, 'cooldown_until': 0, 'retry_at': None,
        'effective_request_interval': 1, 'last_rate_limit_at': 0, 'level': 0,
    }
    limits.check_cooldown()
    assert not path.parent.exists()


def test_dynamic_state_path_is_resolved_on_every_call(tmp_path, monkeypatch, clock):
    monkeypatch.delenv('AYCF_WIZZ_RATE_LIMIT_PATH')
    monkeypatch.setenv('AYCF_STATE_DIR', str(tmp_path / 'state-one'))
    limits.record_rate_limit(0)
    assert (tmp_path / 'state-one' / 'wizz-rate-limit.sqlite3').exists()
    monkeypatch.setenv('AYCF_STATE_DIR', str(tmp_path / 'state-two'))
    assert not limits.rate_limit_status()['blocked']
    assert not (tmp_path / 'state-two').exists()
    monkeypatch.setenv('AYCF_WIZZ_RATE_LIMIT_PATH', str(tmp_path / 'state-one' / 'wizz-rate-limit.sqlite3'))
    assert limits.rate_limit_status()['blocked']


def test_legacy_exception_has_no_fabricated_state_or_untrusted_text():
    error = limits.WizzRateLimited('secret response body')
    assert error.status == {}
    assert 'secret' not in str(error)


def test_first_429_has_local_cooldown_and_safe_status(clock):
    first = limits.record_rate_limit(120)
    assert first['blocked']
    assert first['cooldown_until'] == 100900
    assert first['level'] == 1
    assert first['effective_request_interval'] == 5
    assert first['retry_at'].endswith('+00:00')
    before = Path(os.environ['AYCF_WIZZ_RATE_LIMIT_PATH']).read_bytes()
    with pytest.raises(limits.WizzRateLimited) as caught:
        limits.check_cooldown()
    assert caught.value.status == first
    assert caught.value.args == (limits.rate_limit_message(first),)
    assert Path(os.environ['AYCF_WIZZ_RATE_LIMIT_PATH']).read_bytes() == before


def test_later_episodes_escalate_without_capping_long_server_deadlines(clock):
    expected = [(900, 5), (1800, 10), (3600, 20), (7200, 30), (14400, 30), (21600, 30), (21600, 30)]
    for index, (seconds, pace) in enumerate(expected, start=1):
        status = limits.record_rate_limit(0)
        assert status['level'] == index
        assert status['cooldown_until'] == clock[0] + seconds
        assert status['effective_request_interval'] == pace
        clock[0] = status['cooldown_until'] + 1
    status = limits.record_rate_limit(100000)
    assert status['cooldown_until'] == clock[0] + 100000
    clock[0] += limits.QUIET_SECONDS + 1
    assert limits.rate_limit_status()['blocked']
    assert limits.rate_limit_status()['level'] == 8
    assert limits.rate_limit_status()['effective_request_interval'] == 30


def test_429s_during_same_episode_extend_server_deadline_without_new_level(clock):
    first = limits.record_rate_limit(0)
    clock[0] += 10
    repeated = limits.record_rate_limit(0)
    assert repeated['level'] == 1
    assert repeated['cooldown_until'] == first['cooldown_until']
    extended = limits.record_rate_limit(3600)
    assert extended['level'] == 1
    assert extended['cooldown_until'] == clock[0] + 3600
    assert extended['last_rate_limit_at'] == clock[0]


def test_quiet_day_restores_baseline_after_cooldown_and_new_episode_restarts(clock):
    original = limits.record_rate_limit(0)
    clock[0] = original['cooldown_until'] + 1
    assert limits.rate_limit_status()['effective_request_interval'] == 5
    clock[0] = original['last_rate_limit_at'] + limits.QUIET_SECONDS
    restored = limits.check_cooldown()
    assert restored['level'] == 0
    assert restored['effective_request_interval'] == 1
    assert limits.record_rate_limit(0)['level'] == 1


def test_global_reservations_wait_for_actual_due_slot_and_poll_at_most_one_second(monkeypatch, clock):
    waits = []
    limits.wait_for_request(2.5)

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(limits.time, 'sleep', sleep)
    limits.wait_for_request(2.5)
    assert waits == [1, 1, 0.5]
    assert clock[0] == 100002.5


def test_waiting_reservation_rechecks_new_cooldown_without_allocating_future_slot(monkeypatch, clock):
    limits.wait_for_request(10)
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds
        limits.record_rate_limit(0)

    monkeypatch.setattr(limits.time, 'sleep', sleep)
    with pytest.raises(limits.WizzRateLimited):
        limits.wait_for_request(10)
    assert waits == [1]
    assert clock[0] == 100001


def _processes(code, args):
    processes = [subprocess.Popen([sys.executable, '-c', code, str(arg)], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 env=os.environ.copy()) for arg in args]
    try:
        for process in processes:
            process.stdin.write('x')
            process.stdin.flush()
        results = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            assert process.returncode == 0, stderr
            results.append(json.loads(stdout))
        return results
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate()


def test_concurrent_process_429s_form_one_durable_episode():
    started = time.time()
    code = '''import json, sys
import wizz_rate_limit as limits
sys.stdin.read(1)
print(json.dumps(limits.record_rate_limit(float(sys.argv[1]))))
'''
    outcomes = _processes(code, [0, 60, 1800, 3600])
    assert all(result['level'] == 1 for result in outcomes)
    final = limits.rate_limit_status()
    assert final['level'] == 1
    assert final['effective_request_interval'] == 5
    assert final['cooldown_until'] >= started + 3600
    assert final['cooldown_until'] <= time.time() + 3600
    # A freshly started interpreter sees the same deadline and rejects sends.
    reader = '''import json, sys
import wizz_rate_limit as limits
sys.stdin.read(1)
try:
    limits.wait_for_request()
except limits.WizzRateLimited as exc:
    print(json.dumps(exc.status))
'''
    assert _processes(reader, [0])[0] == final


def test_independent_processes_share_atomic_admission_spacing():
    # Observe the committed reservation while its transaction still excludes
    # other writers. Measuring after return includes arbitrary scheduling/fsync
    # delays, so printed timestamps can be closer despite correctly spaced slots.
    code = '''import json, sys
from contextlib import contextmanager
import wizz_rate_limit as limits
original_write = limits._write_state
admissions = []
@contextmanager
def observe_admission():
    admitted = None
    with original_write() as (conn, row):
        before = row['last_request_at']
        yield conn, row
        after = conn.execute('SELECT last_request_at FROM rate_limit WHERE id = 1').fetchone()[0]
        if after != before:
            admitted = after
    if admitted is not None:
        admissions.append(admitted)
limits._write_state = observe_admission
sys.stdin.read(1)
limits.wait_for_request(1.0)
assert len(admissions) == 1
print(json.dumps(admissions[0]))
'''
    starts = sorted(_processes(code, [0, 1, 2]))
    assert starts[1] - starts[0] >= 1.0
    assert starts[2] - starts[1] >= 1.0
