#!/usr/bin/env python3
"""H07: durable exact-byte acceptance + Temporal outbox + scripted destination."""
import argparse
import hashlib
import json
import pathlib
import sqlite3
from contextlib import closing
import subprocess
import tempfile
import types
import uuid
from runtime_artifact import validate
from test_host_runtime import require,wait,admission
ROOT=pathlib.Path(__file__).resolve().parents[1]
def run(platform,platform_prepared,core_prepared,seed,before,observer=None):
    manifest=validate(ROOT,core_prepared);require(manifest.get('features')=='delivery-runtime','delivery-runtime not prepared')
    source=pathlib.Path(platform)/'tools/temporal_lane.py';module=types.ModuleType('verified_lane');module.__file__=str(source);exec(compile(source.read_bytes(),str(source),'exec'),module.__dict__)
    fixture=f'synthetic delivered artifact {seed}'.encode()+b'\x00\xff';expected=hashlib.sha256(fixture).hexdigest()
    a=admission(seed);binding={k:a[k] for k in ('task_id','revision','operation_id','policy_revision','host_id','workspace_id','lease_epoch')}
    artifact_script={'version':'runspindle.integration.v1','binding':binding,'artifact_id':'artifact','artifact_revision':1,'expected_sha256':expected,'validator_revision':'validator-1','authorized':True,'crash_after_stage':True,'crash_after_accept':True}
    with tempfile.TemporaryDirectory(prefix='delivery-lane-') as tmp:
        directory=pathlib.Path(tmp);mounts=[{'source':str(directory),'target':'/effects','read_only':False}]
        def write(name,value):
            p=directory/(name+'.next');p.write_text(json.dumps(value));p.replace(directory/name)
        write('artifact-script.json',artifact_script);(directory/'artifact.bin').write_bytes(fixture)
        with module.Lane(platform_prepared) as lane:
            def artifact(action,exit_code):
                name=lane.spawn_client(manifest['image_id'],['artifact','/effects',action],mounts=mounts)
                result=subprocess.run(['docker','wait',name],check=True,text=True,capture_output=True,timeout=40)
                require(int(result.stdout.strip())==exit_code,'unexpected artifact process result')
                logs=subprocess.run(['docker','logs',name],text=True,capture_output=True,check=True).stdout
                return json.loads(logs) if logs.strip() else None
            def db_rows(sql):
                with closing(sqlite3.connect('file:'+str(directory/'core.sqlite')+'?mode=rw',uri=True)) as db:
                    db.execute('PRAGMA query_only=ON')
                    return db.execute(sql).fetchall()
            artifact('stage',96)
            rows=db_rows('SELECT reference,bytes FROM artifacts');require(len(rows)==1 and rows[0][1]==fixture and hashlib.sha256(rows[0][1]).hexdigest()==expected,'actual staged bytes mismatch')
            validation=json.loads(db_rows('SELECT record FROM validations')[0][0]);require(validation['passed'] and validation['artifact']['sha256']==expected,'exact validation missing')
            require(db_rows('SELECT count(*) FROM acceptances')[0][0]==0,'staging fabricated acceptance')
            write('validation.json',validation);(directory/'artifact.bin').unlink()
            artifact_script['crash_after_stage']=False;write('artifact-script.json',artifact_script)
            artifact('accept',95)
            accepted=json.loads(db_rows('SELECT record FROM acceptances')[0][0]);require(accepted['validation']==validation,'acceptance reference changed')
            artifact_script['authorized']=False;artifact_script['crash_after_accept']=False;write('artifact-script.json',artifact_script)
            artifact('accept',1);require(db_rows('SELECT count(*) FROM acceptances')[0][0]==1,'denied accept changed state')
            artifact_script['authorized']=True;write('artifact-script.json',artifact_script)
            inspected=artifact('inspect',0);require(inspected['accepted']==accepted and inspected['sha256']==expected and not (directory/'artifact.bin').exists(),'restart regenerated or changed accepted bytes')
            request={'version':'runspindle.integration.v1','accepted':accepted,'account_id':'synthetic-account','destination_id':'synthetic-destination','delivery_operation_id':f'delivery-operation-{seed}'}
            script={'request':request,'authorized':True,'crash_before_send':before,'crash_after_send':not before,'lost_reply':True}
            write('delivery-script.json',script)
            def client(command):
                name='command-'+uuid.uuid4().hex+'.json';write(name,command)
                return json.loads(lane.run_client(manifest['image_id'],['client','http://temporal:7233','/effects/'+name],mounts=mounts,deadline=45).stdout)
            def worker():return lane.spawn_client(manifest['image_id'],['worker','http://temporal:7233','/effects'],mounts=mounts)
            def ledger():
                path=directory/'destination.sqlite'
                if not path.exists():return []
                with closing(sqlite3.connect('file:'+str(path)+'?mode=rw',uri=True)) as db:
                    db.execute('PRAGMA query_only=ON')
                    return [(action,json.loads(r)) for action,r in db.execute('SELECT action,request FROM calls ORDER BY id')]
            w=worker();workflow=f'delivery-{seed}'
            client({'command':'deliver','workflow_id':workflow,'request':request})
            state=wait(lambda:json.loads(subprocess.check_output(['docker','inspect',w],text=True))[0]['State'],lambda s:not s['Running'])
            require(state['ExitCode']==(92 if before else 94),'delivery interruption boundary missing')
            reserved=db_rows('SELECT request,state,receipt FROM outbox');require(len(reserved)==1 and json.loads(reserved[0][0])==request and reserved[0][1]=='reserved' and reserved[0][2] is None,'reservation not committed before send')
            script['crash_before_send']=False;script['crash_after_send']=False;write('delivery-script.json',script)
            lane.restart(abrupt=True);w2=worker()
            query=lambda:client({'command':'delivery','workflow_id':workflow})
            if before:
                final=wait(query,lambda s:s and s['state']=='unknown')
                require(not any(action=='send' for action,_ in ledger()),'uncertain reservation was resent')
                with closing(sqlite3.connect('file:'+str(directory/'destination.sqlite')+'?mode=rw',uri=True)) as db:require(db.execute('SELECT count(*) FROM receipts').fetchone()[0]==0,'fabricated before-send receipt')
                result={'boundary':'reservation_before_send','state':'unknown','send_calls':0,'receipt_count':0,'accepted_sha256':expected}
            else:
                final=wait(query,lambda s:s and s['state']=='delivered')
                require(final['request']==request and final['receipt']['request']==request,'delivery binding changed')
                with closing(sqlite3.connect('file:'+str(directory/'destination.sqlite')+'?mode=rw',uri=True)) as db:receipts=db.execute('SELECT receipt,bytes FROM receipts').fetchall()
                require(len(receipts)==1 and json.loads(receipts[0][0])==final['receipt'] and receipts[0][1]==fixture and hashlib.sha256(receipts[0][1]).hexdigest()==expected,'independent destination bytes/receipt mismatch')
                duplicate=workflow+'-duplicate';client({'command':'deliver','workflow_id':duplicate,'request':request})
                duplicate_final=wait(lambda:client({'command':'delivery','workflow_id':duplicate}),lambda s:s and s['state']=='delivered')
                require(duplicate_final['receipt']==final['receipt'],'duplicate delivery changed receipt')
                script['receipt_override']='conflicting-receipt';write('delivery-script.json',script)
                conflict=workflow+'-conflict';client({'command':'deliver','workflow_id':conflict,'request':request})
                def failed_activity():
                    history=client({'command':'history','workflow_id':conflict})
                    return any('ActivityTaskFailedEventAttributes' in (e.get('attributes') or {}) for e in history['events'])
                wait(failed_activity,bool)
                persisted=db_rows('SELECT state,receipt FROM outbox')[0];require(persisted[0]=='delivered' and json.loads(persisted[1])==final['receipt'],'conflicting receipt overwrote acceptance/delivery')
                require(client({'command':'delivery','workflow_id':conflict}) is None,'conflicting receipt fabricated workflow success')
                script.pop('receipt_override');write('delivery-script.json',script)
                require(db_rows('SELECT conflict FROM outbox')[0][0]==1,'receipt contradiction was not durably fenced')
                subprocess.run(['docker','kill',w2],check=True,capture_output=True);lane.restart(abrupt=True);worker()
                require(db_rows('SELECT conflict FROM outbox')[0][0]==1 and client({'command':'delivery','workflow_id':conflict}) is None,'restart cleared receipt conflict or fabricated success')
                calls=ledger();require(sum(action=='send' for action,_ in calls)==1 and all(r==request for _,r in calls),'duplicate send or changed destination')
                require(calls[0][0]=='send' and any(action=='lookup' for action,_ in calls[1:]),'delivery call order missing readback')
                history=client({'command':'history','workflow_id':workflow});write('delivery-history.json',history);count=len(ledger())
                replay=json.loads(lane.run_client(manifest['image_id'],['replay','/effects/delivery-history.json'],mounts=mounts,deadline=45).stdout)
                require(replay['status']=='passed' and len(ledger())==count,'delivery replay repeated transport')
                result={'boundary':'lost_ack_after_send','state':'delivered','send_calls':1,'receipt_count':1,'accepted_sha256':expected,'duplicate':'same_receipt','conflicting_receipt':'durably_fenced','replay':'passed'}
            require(db_rows('SELECT count(*) FROM artifacts')[0][0]==1 and db_rows('SELECT count(*) FROM acceptances')[0][0]==1 and not (directory/'artifact.bin').exists(),'delivery regenerated accepted work')
            if observer is not None:observer(directory,{'request':request,'before':before,'seed':seed,'result':result})
    return result
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--platform',required=True);p.add_argument('--platform-prepared',required=True);p.add_argument('--core-prepared',default=str(ROOT/'local/runtime-prepared'));p.add_argument('--repeat',type=int,default=2);p.add_argument('--seed',type=int,default=17);a=p.parse_args();require(a.repeat>=2,'two seeded repeats required')
    results=[[run(a.platform,a.platform_prepared,a.core_prepared,a.seed,before) for before in (False,True)] for _ in range(a.repeat)]
    require(all(r==results[0] for r in results),'normalized seeded H07 outcomes differ')
    print(json.dumps({'lane':'H07','status':'passed','evidence_class':'isolated_integration','repeat_runs':a.repeat,'seed':a.seed,'normalized_outcomes':results[0],'scripted':['destination_transport'],'Tether':'blocked_source_and_transport_dependency','live_acceptance':'not_run'}))
