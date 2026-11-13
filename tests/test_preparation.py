"""Synthetic Git-cache negatives, without mutating the real dependency checkout."""
import importlib.util
import pathlib
import subprocess
import tempfile
import unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('prepare_runtime',ROOT/'tools/prepare_temporal_runtime.py')
prepare=importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)
class Pins(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
        self.git('init','-q');self.git('config','user.name','Synthetic');self.git('config','user.email','synthetic@example.invalid')
        (self.root/'Cargo.toml').write_text('original synthetic manifest\n');(self.root/'.gitignore').write_text('synthetic-ignore\n');self.git('add','.');self.git('commit','-qm','synthetic first')
        self.sha=self.git('rev-parse','HEAD').strip();self.source='git+https://example.invalid/component#'+self.sha
    def tearDown(self):self.temp.cleanup()
    def git(self,*args):return subprocess.check_output(['git',*args],cwd=self.root,text=True)
    def test_clean_wrong_head_rejects(self):
        (self.root/'Cargo.toml').write_text('changed synthetic manifest\n');self.git('commit','-qam','synthetic second')
        with self.assertRaisesRegex(RuntimeError,'locked revision'):prepare.verify_git_dependency(self.root,self.source)
    def test_tracked_bytes_and_untracked_build_script_reject(self):
        prepare.verify_git_dependency(self.root,self.source)
        (self.root/'build.rs').write_text('synthetic untracked build\n')
        with self.assertRaisesRegex(RuntimeError,'untracked'):prepare.verify_git_dependency(self.root,self.source)
        (self.root/'build.rs').unlink();(self.root/'Cargo.toml').write_text('tampered\n')
        with self.assertRaisesRegex(RuntimeError,'bytes changed'):prepare.verify_git_dependency(self.root,self.source)
    def test_ignored_build_script_and_modified_ignore_reject(self):
        (self.root/'.gitignore').write_text('build.rs\n')
        (self.root/'build.rs').write_text('synthetic ignored build script\n')
        with self.assertRaises(RuntimeError):prepare.verify_git_dependency(self.root,self.source)
    def test_unknown_revision_rejects(self):
        with self.assertRaisesRegex(RuntimeError,'exact Git revision'):prepare.verify_git_dependency(self.root,'git+https://example.invalid/component#main')

class MutationTargets(unittest.TestCase):
    def test_mutant_output_is_disposable_and_cannot_change_qualification(self):
        import sys
        sys.path.insert(0,str(ROOT/'tools'))
        from mutation_target import isolated_target
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);seed=root/'local/linux-target';seed.mkdir(parents=True)
            (seed/'artifact').write_bytes(b'original compiled dependency fixture')
            with isolated_target(root,'synthetic') as target:
                self.assertNotEqual(target.resolve(),seed.resolve())
                (target/'artifact').write_bytes(b'mutant')
                self.assertEqual((seed/'artifact').read_bytes(),b'original compiled dependency fixture')
            self.assertFalse(target.exists())

if __name__=='__main__':unittest.main()
