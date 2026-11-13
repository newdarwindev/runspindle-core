#!/usr/bin/env python3
"""Explicit acquisition, followed by offline build. No service is started here."""
import argparse
import hashlib
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(args, **kw):
    return subprocess.run(list(map(str, args)), check=True, text=True, timeout=kw.pop('timeout',1800), **kw)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def source_hashes():
    paths = [ROOT/'Cargo.toml', ROOT/'Cargo.lock', ROOT/'temporal/toolchain.json']
    paths += sorted((ROOT/'src').rglob('*.rs'))
    return {str(p.relative_to(ROOT)): sha(p) for p in paths}


def verify_git_dependency(base, source):
    expected = source.rsplit('#', 1)[-1]
    if not re.fullmatch(r'[0-9a-f]{40}', expected):
        raise RuntimeError('dependency source lacks exact Git revision')
    actual = subprocess.check_output(['git','rev-parse','HEAD'],cwd=base,text=True).strip()
    if actual != expected:
        raise RuntimeError('cached host checkout differs from locked revision')
    untracked = subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=base,text=True).splitlines()
    if any(path != '.cargo-ok' for path in untracked):
        raise RuntimeError('untracked files in pinned host checkout')
    for relative in subprocess.check_output(['git','ls-files','src','Cargo.toml','Cargo.lock'],cwd=base,text=True).splitlines():
        expected_bytes = subprocess.check_output(['git','show',actual+':'+relative],cwd=base)
        if (base/relative).read_bytes() != expected_bytes:
            raise RuntimeError('cached pinned host library bytes changed')


def prepare(directory, acquire=False, features="temporal-runtime"):
    directory = pathlib.Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    pins = json.loads((ROOT/'temporal/toolchain.json').read_text())
    engine = subprocess.check_output(['docker','info','--format','{{.OSType}} {{.Architecture}}'],text=True).strip()
    if engine != f"{pins['engine_os']} {pins['engine_architecture']}":
        raise RuntimeError('unsupported Docker engine target; no emulation/floating fallback')
    deps = json.loads((ROOT/'docs/runtime-dependencies.json').read_text())
    if deps['cargo_lock_sha256'] != sha(ROOT/'Cargo.lock') or len(deps['dependencies']) < 200:
        raise RuntimeError('dependency lock/license inventory not qualified')
    metadata = json.loads(subprocess.check_output(['cargo','metadata','--locked','--offline','--all-features','--format-version','1'],cwd=ROOT,text=True))
    for package in metadata['packages']:
        base = pathlib.Path(package['manifest_path']).parent
        row = next((r for r in deps['dependencies'] if r['name']==package['name'] and r['version']==package['version']),None)
        if package['name']=='libsqlite3-sys':
            if not row or any(sha(base/p)!=digest for p,digest in row['bundled_sources'].items()):
                raise RuntimeError('bundled SQLite byte pin mismatch')
        if package['name']=='runspindle-host':
            verify_git_dependency(base, package['source'])
    if sha(ROOT/'licenses/temporal-sdk-MIT.txt') != pins['sdk']['license_sha256']:
        raise RuntimeError('SDK license mismatch')
    builderfile = directory/'Dockerfile.builder'
    version = pins['protoc_debian_version']
    builderfile.write_text(f"FROM {pins['rust_image']}\nRUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler={version} libprotobuf-dev={version} && rm -rf /var/lib/apt/lists/*\n\nRUN rustup component add clippy --toolchain {pins['rust_version']}-aarch64-unknown-linux-gnu\n")
    if sha(builderfile) != pins['builder_recipe_sha256']:
        raise RuntimeError('builder recipe does not match reviewed pins')
    if acquire:
        # The acquisition phase alone can use the network.
        run(['docker', 'pull', pins['rust_image']])
        run(['cargo', 'fetch', '--locked'], cwd=ROOT)
        archive = directory/'sdk-rust-1.2.0.tar.gz'
        if not archive.exists():
            run(['curl', '--fail', '--location', '--max-time', '600', '--retry', '2', pins['sdk']['archive'], '--output', archive])
        if sha(archive) != pins['sdk']['archive_sha256']:
            raise RuntimeError('SDK source archive mismatch')
        present = subprocess.run(['docker','image','inspect',pins['builder_image_id']],capture_output=True).returncode == 0
        if not present:
            run(['docker', 'build', '--pull=false', '-t', 'runspindle-core-builder:candidate', '-f', builderfile, directory])
            candidate = subprocess.check_output(['docker','image','inspect','runspindle-core-builder:candidate','--format','{{.Id}}'],text=True).strip()
            if candidate != pins['builder_image_id']:
                raise RuntimeError('acquired builder differs from reviewed immutable ID; qualification blocked')
    builder = subprocess.check_output(['docker','image','inspect',pins['builder_image_id'],'--format','{{.Id}}'],text=True).strip()
    if builder != pins['builder_image_id']:
        raise RuntimeError('builder image differs from reviewed immutable ID')
    run(['docker','image','inspect',pins['rust_image']],stdout=subprocess.DEVNULL)
    protoc = subprocess.check_output(['docker','run','--rm','--pull','never','--network','none',builder,'sha256sum','/usr/bin/protoc'],text=True).split()[0]
    if protoc != pins['protoc_sha256']:
        raise RuntimeError('prepared protoc byte mismatch')
    clippy = subprocess.check_output(['docker','run','--rm','--pull','never','--network','none',builder,'sha256sum','/usr/local/rustup/toolchains/1.94.1-aarch64-unknown-linux-gnu/bin/clippy-driver'],text=True).split()[0]
    if clippy != pins['clippy_driver_sha256']:
        raise RuntimeError('prepared Clippy byte mismatch')
    archive = directory/'sdk-rust-1.2.0.tar.gz'
    if not archive.exists():
        archive.write_bytes((ROOT/'local/upstream/sdk-rust-1.2.0.tar.gz').read_bytes())
    if sha(archive) != pins['sdk']['archive_sha256']:
        raise RuntimeError('SDK source archive mismatch')
    # The prepared builder is always invoked by immutable image ID and without egress.
    target = ROOT/'local/linux-target'
    target.mkdir(parents=True, exist_ok=True)
    registry = pathlib.Path.home()/'.cargo/registry'
    before_sources = source_hashes()
    run(['docker','run','--rm','--pull','never','--network','none',
         '-v',f'{ROOT}:/work:ro','-v',f'{target}:/work/local/linux-target', '-v',f'{registry}:/usr/local/cargo/registry:ro', '-v',f'{pathlib.Path.home()}/.cargo/git:/usr/local/cargo/git:ro',
         '-w','/work',builder,'cargo','build','--locked','--offline','--features',features,'--target-dir','local/linux-target'])
    if before_sources != source_hashes():
        raise RuntimeError('source changed while building')
    binary = target/'debug/temporal-runtime'
    # Copy exact locked dependency notices to the local prepared artifact.
    metadata = json.loads(subprocess.check_output(['cargo','metadata','--locked','--offline','--all-features','--format-version','1'],cwd=ROOT,text=True))
    notice_dir = directory/'notices'
    notice_dir.mkdir(exist_ok=True)
    for package in metadata['packages']:
        if package['name']=='runspindle-core':
            continue
        base = pathlib.Path(package['manifest_path']).parent
        for path in base.iterdir():
            if path.is_file() and path.name.upper().startswith(('LICENSE','NOTICE','COPYING')):
                dest = notice_dir/f"{package['name']}-{package['version']}-{path.name}"
                dest.write_bytes(path.read_bytes())
    # Stable context path; manifests hash the exact image and binary, not a tag.
    (directory/'temporal-runtime').write_bytes(binary.read_bytes())
    (directory/'temporal-runtime').chmod(0o755)
    (directory/'runtime-dependencies.json').write_bytes((ROOT/'docs/runtime-dependencies.json').read_bytes())
    fingerprint = hashlib.sha256(json.dumps(source_hashes(),sort_keys=True).encode()).hexdigest()
    labels = f'LABEL org.runspindle.source_sha256={fingerprint} org.runspindle.binary_sha256={sha(binary)} org.runspindle.cargo_lock_sha256={sha(ROOT/"Cargo.lock")}\n'
    dockerfile = directory/'Dockerfile'
    dockerfile.write_text(f"FROM {pins['rust_image']}\n{labels}COPY temporal-runtime /usr/local/bin/temporal-runtime\nCOPY notices /third-party-notices\nCOPY runtime-dependencies.json /third-party-notices/inventory.json\nENTRYPOINT [\"/usr/local/bin/temporal-runtime\"]\n")
    run(['docker','build','--network','none','--pull=false','-t','runspindle-core-runtime:prepared','-f',dockerfile,directory])
    image = subprocess.check_output(['docker','image','inspect','runspindle-core-runtime:prepared','--format','{{.Id}}'],text=True).strip()
    manifest = {'schema_version':1,'features':features,'evidence_class':'prepared_dependency_artifacts','image_id':image,
                'builder_image_id':builder,'source_hashes':source_hashes(),'source_sha256':fingerprint,'binary_sha256':sha(binary),
                'cargo_lock_sha256':sha(ROOT/'Cargo.lock'),'sdk_revision':pins['sdk']['revision'],
                'sdk_version':pins['sdk']['version'],'rust_version':pins['rust_version'],
                'dependency_inventory_sha256':sha(ROOT/'docs/runtime-dependencies.json'),
                'isolation':'runtime must use platform internal network; no published ports; pull never',
                'operational_acceptance':'not_run'}
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest))
    return manifest


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--directory',default=str(ROOT/'local/runtime-prepared'))
    p.add_argument('--acquire',action='store_true',help='explicit network preparation only')
    p.add_argument('--features',choices=['temporal-runtime','host-runtime','delivery-runtime'],default='temporal-runtime')
    a=p.parse_args()
    prepare(a.directory,a.acquire,a.features)
