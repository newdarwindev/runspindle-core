"""Choose a pinned native builder; do not emulate or substitute architectures."""
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def select(pins=None, engine=None, *, require_image=True):
    pins = pins or json.loads((ROOT / 'temporal/toolchain.json').read_text())
    engine = engine or subprocess.check_output(
        ['docker', 'info', '--format', '{{.OSType}} {{.Architecture}}'], text=True
    ).strip()
    parts = engine.split()
    if len(parts) != 2 or parts[0] != 'linux':
        raise RuntimeError('unsupported Docker engine target; no emulation or fallback')
    architecture = {'amd64': 'x86_64', 'x86_64': 'x86_64', 'arm64': 'aarch64', 'aarch64': 'aarch64'}.get(parts[1])
    key = f'linux/{architecture}'
    builder = pins.get('builders', {}).get(key)
    if architecture is None or builder is None:
        raise RuntimeError('no pinned native builder for Docker engine target')
    for field in ('builder_image_id', 'builder_recipe_sha256', 'protoc_sha256',
                  'clippy_driver_sha256', 'clippy_target'):
        if not builder.get(field):
            raise RuntimeError(f'incomplete prepared builder pin: {field}')
    if not require_image:
        return key, builder
    observed = subprocess.check_output(
        ['docker', 'image', 'inspect', builder['builder_image_id'],
         '--format', '{{.Id}} {{.Architecture}}'], text=True
    ).strip()
    image_parts = observed.split()
    image_arch = {'amd64': 'x86_64', 'x86_64': 'x86_64', 'arm64': 'aarch64', 'aarch64': 'aarch64'}.get(image_parts[-1] if image_parts else '')
    if len(image_parts) != 2 or image_parts[0] != builder['builder_image_id'] or image_arch != architecture:
        raise RuntimeError('prepared builder ID or architecture mismatch')
    return key, builder
