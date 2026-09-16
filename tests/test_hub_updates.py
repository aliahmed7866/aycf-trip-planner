import fcntl
import json
from pathlib import Path
import subprocess
from unittest.mock import Mock
import pytest
from termux import admin_hub as hub
from termux import hub_update

@pytest.fixture
def setup(monkeypatch,tmp_path):
    monkeypatch.setattr(hub,'HOME',tmp_path)
    monkeypatch.setattr(hub,'STATE_DIR',tmp_path/'state')
    monkeypatch.setattr(hub,'REGISTRY_PATH',tmp_path/'apps.json')
    return tmp_path

def test_defaults_cover_all_apps_and_correct_branches(setup,monkeypatch):
    monkeypatch.setattr(hub,'APP_ROOT',Path(__file__).parents[1])
    rows={r['id']:r for r in hub._load_registry()}
    assert {k:r['update_branch'] for k,r in rows.items()}=={'aycf':'deploy/termux','sunscape':'main','expenses':'main','mediahub':'master','places':'main'}
    assert all(r['update_ready'] for r in rows.values())

@pytest.mark.parametrize('service',['sunscape','expense-manager','mediahub'])
def test_service_status_and_controls_use_supervisor_even_with_stale_match(setup,monkeypatch,service):
    monkeypatch.setattr(hub,'_service_available',lambda _:True)
    monkeypatch.setattr(hub,'_pids_for',lambda _:pytest.fail('Must not rely on obsolete process patterns'))
    monkeypatch.setattr(hub,'_health',lambda _:(True,'HTTP 200'))
    calls=[]
    def run(cmd,**kw):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd,0,'run: service: (pid 100) 2s','')
    monkeypatch.setattr(hub.subprocess,'run',run)
    target={'service':service,'health_url':'http://127.0.0.1:8094/health'}
    assert hub.app_status(target)['state']=='running'
    for action in ['start','stop','restart']:assert hub._service_action(target,action)
    assert [c[1] for c in calls]==['status','up','down','restart']
    assert all(c[2]==str(hub._service_dir(service)) for c in calls)

def test_failed_supervisor_never_falls_back_to_killing_processes(setup,monkeypatch):
    monkeypatch.setattr(hub,'_service_available',lambda _:True)
    monkeypatch.setattr(hub.subprocess,'run',lambda cmd,**kw:subprocess.CompletedProcess(cmd,1,'','supervisor unavailable'))
    with pytest.raises(RuntimeError,match='supervisor unavailable'):hub._service_action({'service':'mediahub'},'stop')

def test_updates_require_csrf_and_duplicate_job_is_blocked(setup,monkeypatch):
    target={'id':'places','name':'Places','update_ready':True}
    monkeypatch.setattr(hub,'_find_app',lambda _:target)
    start=Mock();monkeypatch.setattr(hub,'start_update',start)
    client=hub.create_app().test_client()
    client.post('/apps/places/update');start.assert_not_called()
    with client.session_transaction() as session:session['csrf_token']='token'
    response=client.post('/apps/places/update',data={'csrf_token':'token'})
    start.assert_called_once_with(target)
    assert response.location.endswith('/manage')

def test_lock_status_handles_running_and_interrupted_updates(setup):
    path=hub.update_path('places');path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'state':'running'}))
    with open(path.with_suffix('.lock'),'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert hub.update_status('places')['state']=='running'
        with pytest.raises(RuntimeError,match='already running'):hub.start_update({'id':'places','update_ready':True})
    assert hub.update_status('places')['state']=='error'

def test_worker_refuses_dirty_checkout_before_any_update(setup,monkeypatch):
    (setup/'.git').mkdir()
    monkeypatch.setattr(hub_update.subprocess,'check_output',lambda *a,**k:' M app.py\n')
    run=Mock();monkeypatch.setattr(hub_update.subprocess,'run',run)
    with open(setup/'update.log','w') as log:
        with pytest.raises(RuntimeError,match='Local file changes'):
            hub_update.update({'id':'places','working_dir':str(setup)},hub,log)
    run.assert_not_called()

def test_custom_port_and_workdir_preserved(setup,monkeypatch):
    row=hub._normalize_registry_app({'id':'expenses','working_dir':str(setup/'custom'),'port':8099,'health_url':'http://127.0.0.1:8099/health','update_branch':'main','update_command':['bash','$HOME/Expense_manager/termux/auto-deploy.sh','--once']})
    assert row['update_command']==['bash',str(setup/'custom/termux/auto-deploy.sh'),'--once']
    assert row['port']==8099

@pytest.mark.parametrize('branch,returncode,healthy,expected', [('wrong',0,True,'expected main'),('main',1,True,'Update failed'),('main',0,False,'health check failed'),('main',0,True,'success')])
def test_worker_branch_failure_and_health_reporting(setup,monkeypatch,branch,returncode,healthy,expected):
    (setup/'.git').mkdir()
    def output(cmd,**kw):
        if 'status' in cmd:return ''
        if 'branch' in cmd:return branch
        return 'abc123'
    monkeypatch.setattr(hub_update.subprocess,'check_output',output)
    calls=[]
    def run(cmd,**kw):
        calls.append((cmd,kw))
        return subprocess.CompletedProcess(cmd,returncode)
    monkeypatch.setattr(hub_update.subprocess,'run',run)
    monkeypatch.setattr(hub,'_health',lambda _:(healthy,'connection refused'))
    target={'id':'places','working_dir':str(setup),'update_branch':'main','update_command':['bash',str(setup/'places'),'update'],'port':8094}
    with open(setup/'log','w') as log:
        if expected=='success':assert hub_update.update(target,hub,log)[0]=='success'
        else:
            with pytest.raises(RuntimeError,match=expected):hub_update.update(target,hub,log)
    if branch=='main':assert calls[0][1]['env']['PLACES_PORT']=='8094'
