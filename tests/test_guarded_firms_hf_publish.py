import importlib.util
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

P = Path(__file__).resolve().parents[1] / 'scripts/bulk'
sys.path.insert(0, str(P))
spec = importlib.util.spec_from_file_location('firms_historical_hf_publish', P / 'firms_historical_hf_publish.py')
f = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = f
spec.loader.exec_module(f)


class HFUploadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.raw = root / 'x.csv'
        self.audit = root / 'x.audit.json'
        self.raw.write_bytes(b'latitude,longitude\n22,106\n')
        self.audit.write_text('{"verified":true}', encoding='utf-8')
        self.entries = (
            f.Entry(self.raw, f.PREFIX + '/VIIRS_NOAA20_SP/2023/x.csv', *f.digest(self.raw), False),
            f.Entry(self.audit, f.PREFIX + '/VIIRS_NOAA20_SP/2023/x.audit.json', *f.digest(self.audit), True),
        )
        self.remote = {}
        self.phases = []

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, *, private=True):
        def listing(bucket, prefix, recursive):
            return [SimpleNamespace(path=k, type='file') for k in self.remote]
        def batch(bucket, add):
            self.phases.append([name for _, name in add])
            for path, key in add:
                self.remote[key] = Path(path).read_bytes()
        def download(bucket, files):
            for key, out in files:
                Path(out).write_bytes(self.remote[key])
        return f.publish(entries=self.entries,
                         bucket_info_fn=lambda bucket: SimpleNamespace(private=private, size=sum(len(x) for x in self.remote.values())),
                         list_tree_fn=listing, batch_fn=batch, download_fn=download)

    def test_build_entries_audits_real_csv_and_summary(self):
        export = Path(self.tmp.name) / 'export'
        root = export / 'firms_vn_viirs_sp'
        objects = []
        for source in f.SOURCE_CHOICES:
            task = {'source': source, 'start': '2023-03-01', 'days': 1, 'end': '2023-03-01'}
            csv, audit = f.paths(root, task)
            csv.parent.mkdir(parents=True, exist_ok=True)
            blob = b'latitude,longitude,acq_date,acq_time,frp,satellite\n21,106,2023-03-01,0500,4,NOAA\n'
            csv.write_bytes(blob)
            sha = hashlib.sha256(blob).hexdigest()
            audit.write_text(json.dumps({'source': source, 'start': task['start'],
                                         'days': 1, 'size_bytes': len(blob),
                                         'sha256': sha, 'row_count': 1}), encoding='utf-8')
            objects.append({'source': source, 'start': task['start'], 'days': 1,
                            'size_bytes': len(blob), 'sha256': sha, 'rows': 1})
        summary = {'verified': True, 'source_system': 'NASA FIRMS Area API',
                   'start': '2023-03-01', 'end': '2023-03-01', 'objects': objects,
                   'files': 3, 'rows': 3, 'bytes': sum(x['size_bytes'] for x in objects)}
        (export/'summary.json').write_text(json.dumps(summary), encoding='utf-8')
        found = f.build_entries(export=export)
        self.assertEqual(len(found), 6)
        self.assertEqual(sum(e.is_audit for e in found), 3)
        found[0].local.write_bytes(b'corrupted')
        with self.assertRaises(ValueError):
            f.build_entries(export=export)

    def test_private_hash_verified_two_phase_and_resume(self):
        self.assertEqual(self.call(), (2, 0))
        self.assertEqual(len(self.phases), 2)
        self.assertTrue(self.phases[0][0].endswith('x.csv'))
        self.assertTrue(self.phases[1][0].endswith('x.audit.json'))
        self.assertEqual(self.call(), (0, 2))

    def test_refuse_public(self):
        with self.assertRaises(RuntimeError):
            self.call(private=False)

    def test_refuse_different_remote(self):
        self.remote[self.entries[0].remote] = b'corrupt'
        with self.assertRaises(RuntimeError):
            self.call()

    def test_refuse_audit_without_csv(self):
        self.remote[self.entries[1].remote] = self.audit.read_bytes()
        with self.assertRaises(RuntimeError):
            self.call()


if __name__ == '__main__':
    unittest.main()
