#!/usr/bin/env python3
"""Run Rust checks in the exact prepared builder without network acquisition."""
import argparse
import json
import pathlib
import subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('command',choices=['test','clippy','clean']);p.add_argument('--features',default='host-runtime');p.add_argument('--test');a=p.parse_args()
pins=json.loads((ROOT/'temporal/toolchain.json').read_text())
args=['docker','run','--rm','--pull','never','--network','none','-v',f'{ROOT}:/work:ro','-v',f'{ROOT}/local/linux-target:/work/local/linux-target','-v',f'{pathlib.Path.home()}/.cargo/registry:/usr/local/cargo/registry:ro','-v',f'{pathlib.Path.home()}/.cargo/git:/usr/local/cargo/git:ro','-w','/work',pins['builder_image_id'],'cargo',a.command,'--locked','--offline','--features',a.features,'--target-dir','local/linux-target']
if a.command=='clean':args=args[:args.index('cargo')]+['cargo','clean','-p','runspindle-core','--target-dir','local/linux-target']
if a.test:args+=['--test',a.test]
if a.command=='clippy':args+=['--all-targets','--','-D','warnings']
subprocess.run(args,check=True,timeout=1800)
