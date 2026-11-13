#!/usr/bin/env python3
"""Inventory exact acquired bytes and notices before offline execution."""
import hashlib
import json
import pathlib
import subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
metadata=json.loads(subprocess.check_output(['cargo','metadata','--locked','--offline','--all-features','--format-version','1'],cwd=ROOT,text=True))
old=json.loads((ROOT/'docs/runtime-dependencies.json').read_text())
rows=[]
for package in metadata['packages']:
    if package['name']=='runspindle-core':continue
    base=pathlib.Path(package['manifest_path']).parent
    row={k:package.get(k) for k in ('name','version','license','license_file','source')}
    row['notice_files']=[{'path':str(p.relative_to(base)),'sha256':sha(p)} for p in sorted(base.iterdir()) if p.is_file() and p.name.upper().startswith(('LICENSE','NOTICE','COPYING'))]
    if str(package['source']).startswith('registry+'):
        archives=list((pathlib.Path.home()/'.cargo/registry/cache').glob('*/'+package['name']+'-'+package['version']+'.crate'))
        assert len(archives)==1,package['name']
        row['crate_sha256']=sha(archives[0]);assert 'checksum = "'+row['crate_sha256']+'"' in (ROOT/'Cargo.lock').read_text()
    if package['name']=='libsqlite3-sys':
        row['bundled_sources']={str(p.relative_to(base)):sha(p) for p in sorted(base.glob('sqlite3/*')) if p.is_file()}
    if package['name'] in ('runspindle-host','rusqlite','libsqlite3-sys'):
        notices=ROOT/'licenses/durable';notices.mkdir(parents=True,exist_ok=True)
        for p in base.rglob('*'):
            if p.is_file() and p.name.upper().startswith(('LICENSE','NOTICE','COPYING')):
                (notices/(package['name']+'-'+package['version']+'-'+str(p.relative_to(base)).replace('/','_'))).write_bytes(p.read_bytes())
    rows.append(row)
old['dependencies']=rows;old['cargo_lock_sha256']=sha(ROOT/'Cargo.lock')
(ROOT/'docs/runtime-dependencies.json').write_text(json.dumps(old,indent=2)+'\n')
print(json.dumps({'status':'pinned','dependencies':len(rows),'cargo_lock_sha256':old['cargo_lock_sha256']}))
