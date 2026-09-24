from unittest.mock import Mock
import subprocess
import pytest
from termux import admin_hub as hub
from tests.test_hub_command_contracts import isolated


@pytest.mark.parametrize('result', [0, 1])
def test_repair_uses_registered_setup_and_reports_result(isolated, monkeypatch, result):
    target={'id':'sunscape','name':'Sunscape','working_dir':str(isolated/'sunscape'),'port':8081}
    monkeypatch.setattr(hub,'_find_app',lambda app_id:target)
    monkeypatch.setattr(hub,'_install_command',lambda item:['bash',str(isolated/'setup.sh')])
    run=Mock(return_value=subprocess.CompletedProcess([],result,'','setup failed' if result else ''))
    monkeypatch.setattr(hub.subprocess,'run',run)
    client=hub.create_app().test_client()
    with client.session_transaction() as session:
        session['admin_authenticated']=True
        session['csrf_token']='repair-token'
    response=client.post('/apps/sunscape/repair',data={'csrf_token':'repair-token'})
    assert response.status_code==302
    assert run.call_args.kwargs['env']['SUNSCAPE_PORT']=='8081'
    with client.session_transaction() as session:
        message=session['_flashes'][-1][1]
    assert ('setup failed' if result else 'setup repaired') in message


def test_repair_requires_csrf(isolated,monkeypatch):
    run=Mock();monkeypatch.setattr(hub.subprocess,'run',run)
    client=hub.create_app().test_client()
    client.post('/apps/sunscape/repair')
    run.assert_not_called()
