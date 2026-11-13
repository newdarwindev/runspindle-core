#!/usr/bin/env python3
"""A fabricated send acknowledgement must fail without independent readback."""
import json
import pathlib
import shutil
import subprocess
import tempfile
from mutation_target import isolated_target
ROOT=pathlib.Path(__file__).resolve().parents[1]
source=(ROOT/'src/outbox.rs').read_text();old='if observed.len() == 1 && observed[0] == reply {';assert source.count(old)==1
pins=json.loads((ROOT/'temporal/toolchain.json').read_text())
with isolated_target(ROOT,'fabricated-receipt') as target, tempfile.TemporaryDirectory(prefix='core-receipt-mutation-') as directory:
    root=pathlib.Path(directory)
    for name in ('Cargo.toml','Cargo.lock','src','tests'):
        p=ROOT/name;q=root/name
        shutil.copytree(p,q) if p.is_dir() else shutil.copyfile(p,q)
    (root/'src/outbox.rs').write_text(source.replace(old,'if true {'))
    prefix=['docker','run','--rm','--pull','never','--network','none','-v',f'{root}:/source:ro','-v',f'{target}:/work/local/linux-target','-v',f'{pathlib.Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro','-v',f'{pathlib.Path.home()}/.cargo/git:/usr/local/cargo/git:ro','-w','/source',pins['builder_image_id']]
    subprocess.run(prefix+['cargo','clean','-p','runspindle-core','--target-dir','/work/local/linux-target'],check=True,capture_output=True,text=True,timeout=60)
    run=subprocess.run(prefix+['cargo','test','--locked','--offline','--features','delivery-runtime','--target-dir','/work/local/linux-target','--test','outbox','fabricated_delivery_receipts_do_not_pass_readback','--','--exact'],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=300)
    assert run.returncode and 'test fabricated_delivery_receipts_do_not_pass_readback ... FAILED' in run.stdout and 'could not compile' not in run.stdout,run.stdout[-5000:]
print(json.dumps({'status':'passed','evidence_class':'component_mutation','mutation':'fabricated_delivery_receipt','behavioral_failure_observed':True,'qualification_target_unchanged':True}))
