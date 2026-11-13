#!/usr/bin/env python3
"""Actual SDK/Temporal development recovery with independent synthetic peer ledger."""
import argparse
import base64
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import tempfile
import time
import uuid
from runtime_artifact import validate

ROOT = pathlib.Path(__file__).resolve().parents[1]


def check(value, message):
    if not value:
        raise AssertionError(message)


def test(platform, platform_prepared, core_prepared):
    spec=importlib.util.spec_from_file_location('temporal_lane', pathlib.Path(platform)/'tools/temporal_lane.py')
    lane_module=importlib.util.module_from_spec(spec)
    exec(compile((pathlib.Path(platform)/'tools/temporal_lane.py').read_bytes(), str(pathlib.Path(platform)/'tools/temporal_lane.py'), 'exec'),lane_module.__dict__)
    manifest=validate(ROOT,core_prepared)
    for rel,digest in manifest['source_hashes'].items():
        check(hashlib.sha256((ROOT/rel).read_bytes()).hexdigest()==digest,'prepared runtime source mismatch')
    image=manifest['image_id']
    checks=[]
    with tempfile.TemporaryDirectory(prefix='runspindle-core-') as tmp:
        directory=pathlib.Path(tmp)
        mounts=[{'source':str(directory),'target':'/effects','read_only':False}]
        a={'version':'runspindle.runtime.v1','delivery_namespace':'synthetic','delivery_id':'delivery-1',
           'task_id':'task-1','revision':1,'predecessor_revision':None,'operation_id':'operation-1',
           'normalized_digest':'a'*64,'policy_revision':'policy-1','host_id':'offline-selected-host',
           'workspace_id':'selected-workspace','lease_epoch':1,'grant_ref':'grant-1'}
        binding={k:a[k] for k in ['task_id','revision','operation_id','policy_revision','host_id','workspace_id','lease_epoch']}
        scripts=[{'admission':copy.deepcopy(a),'authorized':True,'online':False,'native_session_id':'actual-peer-session',
                  'launch_lost_reply':True,'crash_after_launch':True,'terminal':None,'cancel_acknowledged':True}]
        def write_scripts():
            path=directory/'script.next'
            path.write_text(json.dumps(scripts));path.replace(directory/'script.json')
        write_scripts()
        def events():
            p=directory/'calls.jsonl'
            if not p.exists():return []
            # Every line is an independently fsynced activity-port request, without provider data.
            return [json.loads(line) for line in p.read_text().splitlines()]
        with lane_module.Lane(platform_prepared) as lane:
            def client(command, fail=False):
                name=f'command-{uuid.uuid4().hex}.json'
                (directory/name).write_text(json.dumps(command))
                result=lane.run_client(image,['client','http://temporal:7233',f'/effects/{name}'],mounts=mounts,deadline=25)
                if fail:return result
                return json.loads(result.stdout)
            def poll(predicate, timeout=25):
                deadline=time.monotonic()+timeout
                last=None
                while time.monotonic()<deadline:
                    try:
                        last=predicate()
                        if last:return last
                    except (subprocess.CalledProcessError, json.JSONDecodeError):pass
                    time.sleep(.1)
                raise AssertionError(f'poll deadline exceeded; last={last!r}')
            worker=lane.spawn_client(image,['worker','http://temporal:7233','/effects'],mounts=mounts)
            client({'command':'start','registry_id':'registry-v1'})
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                replies=list(pool.map(lambda _:client({'command':'admit','registry_id':'registry-v1','admission':a}),range(2)))
            job='job-'+hashlib.sha256(b'task-1\n1').hexdigest()
            check({r['job_id'] for r in replies}=={job},'concurrent delivery returned different jobs')
            check(sorted(r['status'] for r in replies)==['admitted','duplicate_delivery'],'concurrent admission not serialized')
            poll(lambda:any(e['action']=='observe_host' for e in events()))
            check(not list(directory.glob('effect-*.json')),'offline selected host caused effect')
            checks+=['concurrent_duplicate_same_job','offline_pin_zero_launch']
            conflict=copy.deepcopy(a);conflict['host_id']='alternate-host'
            scripts.append({**scripts[0],'admission':conflict});write_scripts()
            check(client({'command':'admit','registry_id':'registry-v1','admission':conflict})['status']=='delivery_conflict','conflicting delivery accepted')
            revision=copy.deepcopy(a);revision.update(delivery_id='delivery-2',revision=2,predecessor_revision=1,operation_id='operation-2')
            scripts.append({**scripts[0],'admission':revision,'online':True,'crash_after_launch':False,'terminal':'completed'})
            write_scripts()
            check(client({'command':'admit','registry_id':'registry-v1','admission':revision})['status']=='predecessor_active','active predecessor superseded')
            malformed=copy.deepcopy(a);malformed['version']='unsupported'
            check(client({'command':'admit','registry_id':'registry-v1','admission':malformed})['status']=='invalid','unknown version accepted')
            checks+=['delivery_conflict_denied','active_revision_denied','unknown_version_denied']
            scripts[0]['online']=True;write_scripts()
            receipt=directory/f'effect-{job}.json'
            poll(receipt.exists)
            poll(lambda:subprocess.check_output(['docker','inspect',worker,'--format','{{.State.ExitCode}}'],text=True).strip()=='93')
            check(sum(e['action']=='launch' for e in events())==1,'launch effect count before recovery')
            scripts[0].update(crash_after_launch=False,authorized=False);write_scripts()
            lane.restart()
            worker2=lane.spawn_client(image,['worker','http://temporal:7233','/effects'],mounts=mounts)
            poll(lambda:client({'command':'operation','job_id':job})['state']=='unknown')
            before=sum(e['action']=='launch' for e in events())
            check(before==1,'revoked authority re-launched effect after restart')
            scripts[0]['authorized']=True;write_scripts()
            snap=poll(lambda:(s if (s:=client({'command':'operation','job_id':job}))['native_session_id']=='actual-peer-session' else None))
            check(snap['binding']==binding,'native discovery changed binding')
            check(sum(e['action']=='launch' for e in events())==1,'lost launch reply blindly retried')
            checks+=['fresh_worker_and_server_database_recovery','revoked_authority_no_effect','same_observed_native_identity','lost_reply_discover_before_retry']
            wrong=copy.deepcopy(binding);wrong['lease_epoch']=2
            check(client({'command':'cancel','job_id':job,'binding':wrong})['accepted'] is False,'stale cancellation accepted')
            check(client({'command':'cancel','job_id':job,'binding':binding})['accepted'] is True,'bound cancellation denied')
            cancel=poll(lambda:(s if (s:=client({'command':'operation','job_id':job}))['cancellation']=='acknowledged' else None))
            check(cancel['state']=='acknowledged','cancel ack treated as terminal')
            check(sum(e['action']=='cancel' for e in events())==1,'duplicate cancellation effect')
            # Completion wins a cancellation race; retain actual observed outcome.
            scripts[0]['terminal']='completed';write_scripts()
            terminal=poll(lambda:(s if (s:=client({'command':'operation','job_id':job}))['state']=='observed_completed' else None))
            check(terminal['cancellation']=='observed_terminal','actual terminal race lost')
            checks+=['stale_cancel_denied','cancel_ack_not_terminal','completion_wins_cancel_race']
            poll(lambda:bool(client({'command':'registry','registry_id':'registry-v1'})['terminal']))
            check(client({'command':'admit','registry_id':'registry-v1','admission':revision})['status']=='admitted','next explicit revision after terminal denied')
            job2='job-'+hashlib.sha256(b'task-1\n2').hexdigest()
            poll(lambda:client({'command':'operation','job_id':job2})['state']=='observed_completed')
            check(client({'command':'admit','registry_id':'registry-v1','admission':a})['job_id']==job,'old duplicate replaced by current revision')
            checks+=['next_revision_after_terminal','old_duplicate_original_binding']
            # Hold an online host observation while a concurrent cancel update arrives.
            race=copy.deepcopy(a);race.update(task_id='task-race',delivery_id='race-delivery',operation_id='race-operation')
            scripts.append({**scripts[0],'admission':race,'authorized':True,'online':True,'terminal':None,'observe_barrier':True})
            write_scripts()
            race_job='job-'+hashlib.sha256(b'task-race\n1').hexdigest()
            check(client({'command':'admit','registry_id':'registry-v1','admission':race})['status']=='admitted','race admission failed')
            poll(lambda:any(e['action']=='observe_host' and e['admission']['task_id']=='task-race' for e in events()))
            race_binding={k:race[k] for k in binding}
            check(client({'command':'cancel','job_id':race_job,'binding':race_binding})['accepted'],'cancel-during-observe rejected')
            held=client({'command':'operation','job_id':race_job})
            check(held['cancellation']=='requested' and held['state']=='scheduled','cancel request was not recorded while online reply held')
            release=directory/f'observe-release-{race_job}.json'
            temporary_release=directory/f'observe-release-{race_job}.next'
            with temporary_release.open('w') as f:
                json.dump(race_binding,f);f.flush()
                import os
                os.fsync(f.fileno())
            temporary_release.replace(release)
            returned=directory/f'observe-returned-{race_job}.json'
            online_reply=poll(lambda:json.loads(returned.read_text()) if returned.exists() else None)
            check(online_reply['status']=='online' and online_reply['binding']==race_binding,'held online reply was not independently observed after release')
            stopped=poll(lambda:(s if (s:=client({'command':'operation','job_id':race_job}))['state']=='observed_cancelled' else None))
            check(stopped['native_session_id'] is None,'pre-dispatch cancellation invented native session')
            check(not any(e['action']=='launch' and e['admission']['task_id']=='task-race' for e in events()),'cancellation raced into initial launch')
            checks+=['cancel_during_online_observation_zero_launch']
            ledger=events()
            launches=[e for e in ledger if e['action']=='launch']
            check(len(launches)==2 and [e['admission']['host_id'] for e in launches]==[a['host_id']]*2,'alternate-host or duplicate dispatch')
            first_actions=[e['action'] for e in ledger if e['admission']['revision']==1]
            check(first_actions.index('launch')<first_actions.index('discover')<first_actions.index('cancel'),'reconciliation call order')
            for workflow_id in [job,job2,race_job,'registry-v1']:
                history=client({'command':'history','workflow_id':workflow_id})
                if workflow_id==race_job:
                    scheduled={}
                    completed_online=[]
                    cancel_updates=[]
                    # SDK1.2 exports History through its Rust Serde representation:
                    # snake_case event fields and an externally tagged attributes enum.
                    for event in history['events']:
                        attrs=event.get('attributes') or {}
                        if 'ActivityTaskScheduledEventAttributes' in attrs:
                            scheduled_attrs=attrs['ActivityTaskScheduledEventAttributes']
                            value=json.loads(base64.b64decode(scheduled_attrs['input']['payloads'][0]['data']))
                            scheduled[str(event['event_id'])]=value
                        if 'WorkflowExecutionUpdateAcceptedEventAttributes' in attrs:
                            cancel_updates.append(int(event['event_id']))
                        if 'ActivityTaskCompletedEventAttributes' in attrs:
                            completed_attrs=attrs['ActivityTaskCompletedEventAttributes']
                            original=scheduled[str(completed_attrs['scheduled_event_id'])]
                            value=json.loads(base64.b64decode(completed_attrs['result']['payloads'][0]['data']))
                            if original['action']=='observe_host' and value['status']=='online':
                                completed_online.append(int(event['event_id']))
                    check(len(cancel_updates)==1 and len(completed_online)==1 and cancel_updates[0]<completed_online[0],
                          f'Temporal history did not record held Online reply completion after cancel acceptance: updates={cancel_updates}, online={completed_online}')
                    race_history_order={'cancel_update_accepted_event':cancel_updates[0],'online_activity_completed_event':completed_online[0]}
                    checks+=['history_records_online_reply_after_cancel_acceptance']
                path=directory/f'history-{workflow_id}.json';path.write_text(json.dumps(history))
                result=lane.run_client(image,['replay',f'/effects/{path.name}'],mounts=mounts,deadline=40)
                check(json.loads(result.stdout)['status']=='passed','real history replay failed')
            check(events()==ledger,'history replay invoked activity ports')
            checks+=['real_history_replay_registry_and_operations','independent_launch_and_cancel_counts_and_order']
        # Lane cleanup is checked after worker failure and successful replay alike.
        for name in [lane.name,worker,worker2]:
            check(subprocess.run(['docker','inspect',name],capture_output=True,timeout=30).returncode!=0,'container leaked')
        report={'status':'passed','evidence_class':'isolated_integration','checks':checks,'image_id':image,
                'sdk_revision':manifest['sdk_revision'],'source_hashes':manifest['source_hashes'],
                'launch_count':2,'cancel_count':1,'race_observations':{'before_release':held,'online_reply_after_release':online_reply,'temporal_history_order':race_history_order},'cleanup':'passed','live_acceptance':'not_run',
                'boundary_matrix':{'core':'real','temporal':'real_dev_database','host':'scripted_activity_peer',
                                   'browser':'not_exercised','delivery':'not_exercised','provider':'not_exercised'}}
        return report


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--platform',required=True)
    p.add_argument('--platform-prepared',required=True)
    p.add_argument('--core-prepared',default=str(ROOT/'local/runtime-prepared'))
    p.add_argument('--report',default=str(ROOT/'local/runtime-tests.json'))
    a=p.parse_args()
    report=test(a.platform,a.platform_prepared,a.core_prepared)
    pathlib.Path(a.report).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
