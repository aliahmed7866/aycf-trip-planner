"""Detached, single-app updates that survive an Admin Hub restart."""
from __future__ import annotations
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def save(path, state, message):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps({'state':state,'message':message,'updated_at':int(time.time())}))
    temp.replace(path)


def update(target, hub, log):
    root=Path(target['working_dir'])
    env={**os.environ,'GIT_TERMINAL_PROMPT':'0','GCM_INTERACTIVE':'never'}
    app_id=target['id']
    prefixes={'sunscape':'SUNSCAPE','expenses':'EXPENSE','mediahub':'MEDIAHUB','places':'PLACES','aycf':'AYCF'}
    prefix=prefixes.get(app_id)
    if prefix:
        env[prefix+'_APP_DIR']=str(root)
        if target.get('port'): env[prefix+'_PORT']=str(target['port'])
    def run(command):
        proc=subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1200)
        if proc.returncode: raise RuntimeError('Update failed. Open the update log for details.')
    if not (root/'.git').exists(): raise RuntimeError('Install this app before updating it.')
    dirty=subprocess.check_output(['git','-c','core.fileMode=false','status','--porcelain','--untracked-files=no'],cwd=root,text=True).strip()
    if dirty: raise RuntimeError('Local file changes found. Update stopped to preserve them.')
    if app_id=='aycf':
        env['AYCF_DEPLOY_REF']=str(target.get('update_branch','deploy/termux'))
        run(['bash',str(root/'termux/auto-deploy.sh')])
        log.flush()
        if 'Another deployment is already running' in Path(log.name).read_text(errors='replace'):
            return 'deferred','Another AYCF deployment is already running. Retry after it finishes.'
        status=(hub.STATE_DIR/'deploy-status.txt')
        result=status.read_text().strip() if status.exists() else ''
        if result.startswith(('deferred','blocked')): return 'deferred',result
        if not result.startswith(('healthy','current')): raise RuntimeError('Update did not confirm a healthy deployment. See the update log.')
    else:
        branch=str(target['update_branch'])
        current=subprocess.check_output(['git','branch','--show-current'],cwd=root,text=True).strip()
        if current!=branch: raise RuntimeError(f'Checkout is on {current or "detached HEAD"}; expected {branch}. No changes made.')
        command=hub._expand_parts(target.get('update_command'))
        if not command: raise RuntimeError('No update command configured.')
        run(command)
    healthy,detail=hub._health(str(target.get('health_url','')))
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
