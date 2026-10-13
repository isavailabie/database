#!/usr/bin/env python3
"""Small infrastructure checks: timeout descendants, failed comparisons, and exit status."""
import json
from pathlib import Path
import sys
import subprocess
import uuid
import tempfile
import unittest
from runner import run
from compare import compare

class RunnerTests(unittest.TestCase):
    def test_timeout_terminates_descendant(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);pidfile=root/'child.pid'
            code=('import subprocess,time,pathlib,sys; '
                  'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                  f'pathlib.Path({str(pidfile)!r}).write_text(str(p.pid)); time.sleep(60)')
            result=run([sys.executable,'-c',code],root/'log',1)
            self.assertEqual(result['status'],'timeout')
            pid=int(pidfile.read_text());stat=Path(f'/proc/{pid}/stat')
            self.assertTrue(not stat.exists() or stat.read_text().split()[2]=='Z')
    def test_rejects_recursive_source_copy(self):
        root=Path(__file__).resolve().parents[1]
        source=root
        output=source/('forbidden-output-'+uuid.uuid4().hex)
        result=subprocess.run([sys.executable,str(root/'benchmark/runner.py'),'--kind','baseline',
                               '--source',str(source),'--output',str(output)],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)
        self.assertIn('outside the submitted source tree',result.stderr)
        self.assertFalse(output.exists())
    def test_nonzero_is_not_pass(self):
        with tempfile.TemporaryDirectory() as td:
            result=run([sys.executable,'-c','raise SystemExit(7)'],Path(td)/'log',5)
            self.assertEqual(result['status'],'fail');self.assertEqual(result['returncode'],7)
    def test_comparison_rejects_failed_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);a=root/'a';b=root/'b';a.mkdir();b.mkdir()
            (a/'summary.json').write_text(json.dumps({'status':'pass'}))
            (b/'summary.json').write_text(json.dumps({'status':'fail'}))
            with self.assertRaises(ValueError):compare(a,b)
    def test_comparison_rejects_mismatched_inputs(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);a=root/'a';b=root/'b';a.mkdir();b.mkdir()
            for p,size in [(a,256),(b,128)]:
                (p/'summary.json').write_text(json.dumps({'status':'pass','kind':'baseline','value_bytes':size,'seed':1,'sanitize':False}))
            with self.assertRaises(ValueError):compare(a,b)
if __name__=='__main__':unittest.main()
