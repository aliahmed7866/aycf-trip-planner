"""Detached, single-app updates that survive an Admin Hub restart."""
from __future__ import annotations
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def save(path, state, message):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps({'state':state,'message':message,'updated_at':int(time.time())}))
    temp.replace(path)


def update(target, hub, log):
    root=Path(target['working_dir'])
    env=hub.app_environment(target)
    app_id=target['id']
    def run(command):
        proc=subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1200)
        if proc.returncode: raise RuntimeError('Update failed. Open the update log for details.')
    if not (root/'.git').exists(): raise RuntimeError('Install this app before updating it.')
    if app_id == 'aycf':
        env['AYCF_DEPLOY_REF'] = str(target.get('update_branch', 'deploy/termux'))
        # A scheduled run can also write deploy-status.txt. Read this attempt's receipt.
        with tempfile.TemporaryDirectory(dir=hub.STATE_DIR) as scratch:
            receipt = Path(scratch) / 'result'
            env['AYCF_DEPLOY_RESULT'] = str(receipt)
            proc = subprocess.run(['bash', str(root / 'termux/auto-deploy.sh'), '--restart'],
                                  cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=1200)
            result = receipt.read_text().strip() if receipt.exists() else ''
        if result.startswith(('deferred', 'blocked')):
            return 'deferred', result
        if proc.returncode or not result.startswith('healthy '):
            raise RuntimeError('AYCF update failed: ' + (result or f'updater exited {proc.returncode} without a result') +
                               '. Open the update log for details.')
        if hub._off_file(target).exists():
            if hub.app_status(target)['state'] != 'stopped':
                raise RuntimeError('Admin Hub updated, but the stopped planner could not be confirmed.')
            return 'success', 'Admin Hub was restarted. The planner remains off. Commit ' + result.split()[1][:12]
    else:
        dirty=subprocess.check_output(['git','-c','core.fileMode=false','status','--porcelain','--untracked-files=no'],cwd=root,text=True).strip()
        if dirty: raise RuntimeError('Local file changes found. Update stopped to preserve them.')
        branch=str(target['update_branch'])
        current=subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip()
        if current!=branch: raise RuntimeError(f'Checkout is on {current or "detached HEAD"}; expected {branch}. No changes made.')
        if hub._off_file(target).exists() and app_id in {"akhlaq", "muso"}:
            run(['git', 'fetch', 'origin', branch])
            run(['git', 'merge', '--ff-only', 'FETCH_HEAD'])
            if app_id != 'akhlaq':
                run([str(root / '.venv/bin/python'), '-m', 'pip', 'install', '-r', 'requirements.txt'])
            sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=root, text=True).strip()
            if hub.app_status(target)['state'] != 'stopped':
                raise RuntimeError('Files updated, but the stopped state could not be confirmed.')
            return 'success', 'Latest changes installed. App remains switched off. Commit ' + sha
        command=hub._expand_parts(target.get('update_command'))
        if not command: raise RuntimeError('No update command configured.')
        paused = hub.service_preference(target) == "Off"
        if not paused and target.get("service"):
            # The previous hub used transient stops that only runsv remembers.
            try:
                status = (hub._service_dir(target["service"]) / "supervise/status").read_bytes()
                paused = status[17:18] == b"d"
            except FileNotFoundError:
                pass
        try:
            run(command)
            if target.get("restart_after_update") and not paused:
                hub.change_power(target, "restart")
        finally:
            if paused:
                # Older update scripts may temporarily enable the service.
                hub.stop_app(target)
        if paused:
            confirmed, observed = hub.wait_for_control(target, "stop")
            if not confirmed:
                raise RuntimeError("Update finished, but shutdown is unconfirmed: " + observed["service_text"])
            sha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=root, text=True).strip()
            return "success", "Latest changes installed. App remains off. Commit " + sha
    current = hub._find_app(app_id) if target.get("health_url") else None
    healthy,detail=hub._app_health(current or target)
    if not healthy: raise RuntimeError('Update finished but the app health check failed: '+detail)
    sha=subprocess.check_output(['git','rev-parse','--short','HEAD'],cwd=root,text=True).strip()
    return 'success','Latest changes installed; app is healthy. Commit '+sha


def main():
    import admin_hub as hub
    app_id=sys.argv[1]
    # Parent passes an already-locked file descriptor; it remains held throughout.
    lock_fd=int(sys.argv[2])
    target=hub._find_app(app_id)
    path=hub.update_path(app_id)
    if target is None: save(path,'error','App no longer exists in the registry.');return
    with open(path.with_suffix('.log'),'a',buffering=1) as log:
        save(path,'running','Pulling latest changes and checking the app…')
        try:
            state,message=update(target,hub,log)
            save(path,state,message)
        except Exception as exc:
            print(str(exc),file=log)
            save(path,'error',str(exc))
    os.close(lock_fd)

if __name__=='__main__': main()
