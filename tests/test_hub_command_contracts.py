"""Hub actions must use the registered installation and actual launcher contracts."""
import json
import os
from pathlib import Path
import shutil
import subprocess
from unittest.mock import Mock

import pytest
from termux import admin_hub as hub

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(hub, 'HOME', tmp_path)
    monkeypatch.setattr(hub, 'APP_ROOT', ROOT)
    monkeypatch.setattr(hub, 'STATE_DIR', tmp_path / 'state')
    monkeypatch.setattr(hub, 'LOG_DIR', tmp_path / 'logs')
    monkeypatch.setattr(hub, 'REGISTRY_PATH', tmp_path / 'apps.json')
    return tmp_path


@pytest.mark.parametrize('app_id,prefix', [('aycf','AYCF'),('sunscape','SUNSCAPE'),('expenses','EXPENSE'),('mediahub','MEDIAHUB'),('places','PLACES')])
def test_all_app_commands_receive_the_registered_checkout_and_port(isolated, monkeypatch, app_id, prefix):
    target={'id':app_id, 'working_dir':str(isolated), 'port':8099, 'update_branch':'main'}
    spawn=Mock(return_value=type('Child', (), {'pid':123})())
    monkeypatch.setattr(hub.subprocess, 'Popen', spawn)
    hub._start_command(target, ['python', 'app.py'], 'runtime.log')
    env=spawn.call_args.kwargs['env']
    assert env[prefix+'_APP_DIR'] == str(isolated)
    assert env[prefix+'_PORT'] == '8099'
    assert env['AYCF_ADMIN_REGISTRY'] == str(hub.REGISTRY_PATH)
    if app_id in {'aycf','sunscape'}:
        assert env['PORT']=='8099'
    assert spawn.call_args.kwargs['cwd']==str(isolated)
    assert spawn.call_args.kwargs['stdin']==subprocess.DEVNULL


def test_sunscape_custom_paths_ports_and_commands_survive_normalization(isolated, monkeypatch):
    row={'id':'sunscape', 'name':'Sunscape', 'working_dir':str(isolated/'weather'), 'port':8181,
         'open_url':'http://localhost:8181', 'health_url':'http://localhost:8181/health',
         'start':['python','custom.py'], 'update_command':['bash',str(isolated/'custom/termux/update-service.sh')], 'update_branch':'main'}
    monkeypatch.setattr(hub, '_sunscape_manifest', Mock(side_effect=AssertionError('Do not replace custom root')))
    normalized=hub._normalize_registry_app(row)
    for key in ('working_dir','port','health_url','open_url','start','update_command'):
        assert normalized[key]==row[key]


def test_places_update_uses_the_same_custom_launcher_as_controls(isolated):
    runner=str(isolated/'bin/my-places')
    row=hub._normalize_registry_app({'id':'places','start_command':['bash',runner,'start'],
        'update_command':['bash','$HOME/.local/bin/places','update'], 'update_branch':'main'})
    assert row['update_command']==['bash',runner,'update']


def test_registry_port_only_override_updates_default_health_and_open_urls(isolated):
    hub.REGISTRY_PATH.write_text(json.dumps({'apps':[{'id':'expenses','name':'Pocketwise','port':8182}]}))
    row=next(x for x in hub._load_registry() if x['id']=='expenses')
    assert row['port']==8182
    assert row['health_url']=='http://127.0.0.1:8182/health'
    assert row['open_url']=='http://127.0.0.1:8182'


def test_installed_manifest_port_not_hidden_by_example_defaults(isolated, monkeypatch):
    monkeypatch.setattr(hub, '_sunscape_manifest', lambda: {'port':8181,'open_url':'http://127.0.0.1:8181','health_url':'http://127.0.0.1:8181/health','working_dir':str(isolated/'sunscape')})
    row=next(x for x in hub._load_registry() if x['id']=='sunscape')
    assert row['port']==8181


def test_status_and_command_actions_share_directory_and_environment(isolated, monkeypatch):
    runner=isolated/'places';runner.touch()
    target={'id':'places','working_dir':str(isolated),'port':8184,
            'status_command':[str(runner),'status'],'restart_command':[str(runner),'restart']}
    run=Mock(return_value=subprocess.CompletedProcess([],0,'running pid=123',''))
    monkeypatch.setattr(hub.subprocess,'run',run)
    assert hub._command_status(target)[0]=='running'
    hub._command_action(target,'restart')
    for call in run.call_args_list:
        assert call.kwargs['cwd']==str(isolated)
        assert call.kwargs['env']['PLACES_PORT']=='8184'
        assert call.kwargs['env']['PLACES_APP_DIR']==str(isolated)


@pytest.mark.parametrize('name,job_id,script', [('schedule-morning.sh','2608','morning-gate.sh'),('schedule-deploy.sh','2610','auto-deploy.sh')])
def test_scheduled_triggers_use_the_actual_checkout_path(tmp_path, name, job_id, script):
    root=tmp_path/'custom-checkout';(root/'termux').mkdir(parents=True)
    for filename in (name,script):
        shutil.copyfile(ROOT/'termux'/filename,root/'termux'/filename)
    binary=tmp_path/'bin';binary.mkdir()
    scheduler=binary/'termux-job-scheduler'
    scheduler.write_text('#!/bin/sh\nprintf "%s\\n" "$@" >> "$AYCF_TEST_CAPTURE"\n')
    scheduler.chmod(0o700)
    output=tmp_path/'calls'
    env={**os.environ,'PATH':str(binary)+':'+os.environ['PATH'],'AYCF_TEST_CAPTURE':str(output)}
    env.pop('AYCF_APP_DIR',None)
    subprocess.run(['bash',str(root/'termux'/name)],env=env,check=True,capture_output=True)
    args=output.read_text().splitlines()
    assert args[args.index('--script')+1]==str(root/'termux'/script)
    assert args[args.index('--job-id')+1]==job_id
    assert args[args.index('--persisted')+1]=='true'


def test_run_web_preserves_explicit_hub_port_and_custom_checkout(tmp_path):
    root=tmp_path/'custom';(root/'termux').mkdir(parents=True)
    shutil.copyfile(ROOT/'termux/run-web.sh',root/'termux/run-web.sh')
    config=tmp_path/'config';config.mkdir();(config/'env').write_text('export PORT=8080\n')
    binary=tmp_path/'bin';binary.mkdir();python=binary/'python'
    python.write_text('#!/bin/sh\nprintf "%s|%s|%s\\n" "$PWD" "$PORT" "$*" > "$AYCF_TEST_CAPTURE"\n');python.chmod(0o700)
    output=tmp_path/'result'
    env={**os.environ,'PATH':str(binary)+':'+os.environ['PATH'],'AYCF_CONFIG_DIR':str(config),'PORT':'8190','AYCF_TEST_CAPTURE':str(output)}
    env.pop('AYCF_APP_DIR',None)
    subprocess.run(['bash',str(root/'termux/run-web.sh')],env=env,check=True)
    assert output.read_text().strip()==f'{root}|8190|watch_app.py termux/runtime.py web'
