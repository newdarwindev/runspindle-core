"""Private copy-on-write build targets keep mutant outputs out of qualification."""
import contextlib
import pathlib
import shutil
import subprocess
import sys
import tempfile

@contextlib.contextmanager
def isolated_target(root, name):
    seed=(root/'local/linux-target').resolve()
    def fingerprint():
        return {str(p.relative_to(seed)):(p.stat().st_size,p.stat().st_mtime_ns) for p in seed.rglob('*') if p.is_file()}
    before=fingerprint()
    with tempfile.TemporaryDirectory(prefix='isolated-'+name+'-') as directory:
        target=pathlib.Path(directory)/'target'
        if sys.platform=='darwin':
            subprocess.run(['cp','-cR',str(seed),str(target)],check=True,timeout=120)
        else:
            shutil.copytree(seed,target)
        # The caller also cleans the component package before compiling the
        # mutation. Dependency artifacts are reusable; component outputs are not.
        try:
            yield target
        finally:
            if fingerprint()!=before:
                raise AssertionError('qualification target changed during mutation; discard results')
