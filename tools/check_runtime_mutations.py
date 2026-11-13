#!/usr/bin/env python3
"""Deliberately break real registry code in throwaway copies; no source edits."""
import json
import pathlib
import shutil
import subprocess
import tempfile

ROOT=pathlib.Path(__file__).resolve().parents[1]


def main():
    pins=json.loads((ROOT/'temporal/toolchain.json').read_text())
    mutations=[
        ('capacity_disabled','if self.deliveries.len() >= CAPACITY {','if false {','known_duplicates_reconcile_at_capacity_without_eviction_or_new_delivery'),
        ('delivery_conflict_ignored','if old == &a {','if true {','conflicts_revisions_and_malformed_inputs_preserve_state'),
        ('old_operation_reused','.any(|old| old.binding.operation_id == a.binding.operation_id)','.any(|_old| false)','any_prior_operation_id_reuse_is_rejected_after_terminal_revisions'),
    ]
    observed=[]
    for name,old,new,test in mutations:
        with tempfile.TemporaryDirectory(prefix=f'core-mutation-{name}-',dir=ROOT/'local') as tmp:
            directory=pathlib.Path(tmp)
            (directory/'local/linux-target').mkdir(parents=True)
            for f in ['Cargo.toml','Cargo.lock']:
                shutil.copyfile(ROOT/f,directory/f)
            for f in ['src','tests']:
                shutil.copytree(ROOT/f,directory/f)
            cargo=directory/'Cargo.toml'
            text=cargo.read_text().replace('[features]\n','[features]\nmutation-no-cli = []\n')
            # These registry-only tests do not exercise the CLI. Avoid linking an
            # unused executable; the actual runtime source/dependencies stay intact.
            text=text.replace('required-features = ["temporal-runtime"]','required-features = ["mutation-no-cli"]')
            cargo.write_text(text)
            path=directory/'src/runtime.rs'
            original=path.read_text()
            if original.count(old)!=1:
                raise RuntimeError(f'mutation anchor changed: {name}')
            path.write_text(original.replace(old,new))
            result=subprocess.run(['docker','run','--rm','--pull','never','--network','none',
                '-v',f'{directory}:/work:ro','-v',f'{ROOT}/local/linux-target:/work/local/linux-target',
                '-v',f'{pathlib.Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro','-w','/work',
                pins['builder_image_id'],'cargo','test','--locked','--offline','--features','temporal-runtime',
                '--target-dir','local/linux-target','--test','runtime',test,'--','--exact'],capture_output=True,text=True,timeout=240)
            output=result.stdout+result.stderr
            detected=result.returncode!=0 and f'test {test} ... FAILED' in output and 'could not compile' not in output
            if not detected:
                raise AssertionError(f'{name} was not behaviorally detected:\n{output[-4000:]}')
            observed.append({'mutation':name,'test':test,'detected':True})
    print(json.dumps({'status':'passed','evidence_class':'component','mutations':observed}))


if __name__=='__main__':
    main()
