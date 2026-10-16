import json
import os
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from src.ingestion.dotenv import load_project_env
from src.ingestion.era5 import (
    DATASET, IngestionError, cds_request, ingest_era5,
    inspect_grib_framing, validate_bbox, variables_for_request,
)

SAMPLE_FILE = Path(__file__).resolve().parents[1] / 'data' / 'sample_era5_vietnam_day.grib'
VARS = ['2m_temperature', '2m_dewpoint_temperature', '10m_u_component_of_wind',
        '10m_v_component_of_wind', 'total_precipitation']
BBOX = dict(north=24, west=102, south=8, east=110)
DAY = date(2025, 1, 15)


def grib1(field: bytes = b'abcdefgh') -> bytes:
    length = 8 + len(field) + 4
    return b'GRIB' + length.to_bytes(3, 'big') + b'\x01' + field + b'7777'


class ERA5IngestionTests(unittest.TestCase):
    def test_grib_real_sample_edition1_24_messages(self):
        if SAMPLE_FILE.exists():
            self.assertEqual(inspect_grib_framing(SAMPLE_FILE), 24)

    def test_grib1_two_messages(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'two.grib'; p.write_bytes(grib1() + grib1())
            self.assertEqual(inspect_grib_framing(p), 2)

    def test_grib2_message(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'one.grib'
            p.write_bytes(b'GRIB' + b'\x00\x00\x00\x02' + (24).to_bytes(8, 'big') + b'abcd7777')
            self.assertEqual(inspect_grib_framing(p), 1)

    def test_bad_grib_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'bad.grib'
            for payload in [b'PK\x03\x04garbage'*5, grib1()[:-1], grib1() + b'x', grib1()[:-4] + b'XXXX']:
                p.write_bytes(payload)
                with self.assertRaises(IngestionError): inspect_grib_framing(p)

    def test_cds_request_5_vars_24_hours_bbox_order(self):
        req = cds_request(DAY, area=validate_bbox(BBOX), variables=VARS)
        self.assertEqual(req['area'], [24.0, 102.0, 8.0, 110.0])
        self.assertEqual(req['time'][0], '00:00')
        self.assertEqual(req['time'][-1], '23:00')
        self.assertEqual(len(req['time']), 24)
        self.assertEqual(req['variable'], VARS)
        self.assertEqual(req['download_format'], 'unarchived')

    def test_variable_subset_and_invalid(self):
        self.assertEqual(variables_for_request(VARS, ['2m_temperature']), ['2m_temperature'])
        for bad in [[], ['bad_variable'], ['2m_temperature']*2]:
            with self.assertRaises(ValueError): variables_for_request(VARS, bad)

    def test_missing_eccodes_no_false_success(self):
        from src.ingestion.era5 import inspect_grib_semantics
        with patch.dict('sys.modules', {'eccodes': None}):
            with self.assertRaises(IngestionError) as c:
                inspect_grib_semantics(Path('irrelevant.grib'), day=DAY, variables=['2m_temperature'])
        self.assertEqual(c.exception.code, 'missing_eccodes')

    def test_live_request_output_manifest_mode_and_checksum(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); calls=[]
            def fake_download(ds,req,target):
                calls.append(req);Path(target).write_bytes(grib1())
            def fake_validate(path,*,day,variables):
                self.assertEqual(path.read_bytes(), grib1())
                self.assertEqual(day,DAY)
                self.assertEqual(variables,['2m_temperature'])
                return 24  # semantic inspection mocked to test plumbing only
            summary=ingest_era5(root=root, region_id='VN_BBOX', bbox=BBOX,
                start='2025-01-15',end='2025-01-16',config_variables=VARS,
                selected_variables=['2m_temperature'],downloader=fake_download,
                validator=fake_validate)
            self.assertEqual((summary.days, summary.messages, summary.failures),(1,24,0))
            self.assertEqual(calls[0]['year'], ['2025'])
            manif=list(root.rglob('*.manifest.json'))
            self.assertEqual(len(manif),1)
            obj=json.loads(manif[0].read_text())
            self.assertEqual(obj['row_count'],24)
            self.assertEqual(obj['source'],'era5')
            self.assertEqual(obj['acquisition_mode'],'live')
            self.assertEqual(obj['format'],'grib')
            self.assertTrue((root/obj['file']).exists())
            self.assertEqual(len(obj['sha256']),64)
            self.assertFalse(list(root.rglob('*.download')))

    def test_replay_separate_from_live_and_no_downloader(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); sample=root/'sample.grib';sample.write_bytes(grib1())
            def no_network(*args):raise AssertionError('replay called CDS')
            summary=ingest_era5(root=root,region_id='VN_BBOX',bbox=BBOX,
                start='2025-01-15',end='2025-01-16',config_variables=VARS,
                selected_variables=['2m_temperature'],replay_file=sample,
                downloader=no_network,validator=lambda path,**kw: 24)
            self.assertTrue(summary.ok)
            manifest=json.loads(next(root.rglob('*.manifest.json')).read_text())
            self.assertEqual(manifest['acquisition_mode'],'replay')
            self.assertEqual((root/manifest['file']).read_bytes(),grib1())

    def test_error_manifest_no_secret_in_failure(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            def error(*args):raise IngestionError('cds_retrieval_failed')
            summary=ingest_era5(root=root,region_id='VN_BBOX',bbox=BBOX,
                start='2025-01-15',end='2025-01-16',config_variables=VARS,
                downloader=error)
            self.assertFalse(summary.ok)
            obj=json.loads(next(root.rglob('*.manifest.json')).read_text())
            self.assertEqual(obj['status'],'failed')
            self.assertIsNone(obj['file'])
            self.assertEqual(obj['error_type'],'cds_retrieval_failed')
            self.assertFalse(list(root.rglob('*.grib')))

    def test_no_silent_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);data=b'alpha'
            def fetch(*args):Path(args[-1]).write_bytes(data)
            opts=dict(root=root,region_id='VN_BBOX',bbox=BBOX,start='2025-01-15',end='2025-01-16',
                config_variables=VARS,selected_variables=['2m_temperature'],
                downloader=fetch,validator=lambda *args, **kw:24)
            self.assertTrue(ingest_era5(**opts).ok)
            old_manifest=json.loads(next(root.rglob('*.manifest.json')).read_text())
            data=b'beta'
            summary=ingest_era5(**opts)
            self.assertFalse(summary.ok)
            self.assertEqual(json.loads(next(root.rglob('*.manifest.json')).read_text()),old_manifest)

    def test_invalid_request_windows_and_replay_2days(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p=root/'x.grib';p.write_bytes(grib1())
            for s,e in [('2025-01-15','2025-01-15'),('2025-01-15','2025-02-20')]:
                with self.assertRaises(ValueError):ingest_era5(root=root,region_id='VN_BBOX',bbox=BBOX,
                    start=s,end=e,config_variables=VARS,replay_file=p)
            with self.assertRaises(ValueError):ingest_era5(root=root,region_id='VN_BBOX',bbox=BBOX,
                start='2025-01-15',end='2025-01-17',config_variables=VARS,replay_file=p)

    def test_loader_imports_only_whitelisted_secrets_no_override(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'.env').write_text('OPENAQ_API_KEY=from_file\nFIRMS_MAP_KEY="other"\nBAD_KEY=danger\n')
            with patch.dict(os.environ, {'OPENAQ_API_KEY':'from_process'},clear=True):
                load_project_env(root)
                self.assertEqual(os.environ['OPENAQ_API_KEY'],'from_process')
                self.assertEqual(os.environ['FIRMS_MAP_KEY'],'other')
                self.assertNotIn('BAD_KEY',os.environ)

    def test_metadata_validation_fake_eccodes(self):
        from src.ingestion.era5 import inspect_grib_semantics
        import types
        objs=[{'s':'2t','h':h} for h in range(24)]
        fake=types.SimpleNamespace()
        def next_field(stream):return objs.pop(0) if objs else None
        fake.codes_grib_new_from_file=next_field
        fake.codes_get=lambda o,k:({'shortName':o['s'],'validityDate':20250115,'validityTime':o['h']*100,'numberOfPoints':9})[k]
        fake.codes_release=lambda obj:None
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.grib';p.write_bytes(b'dummy')
            with patch.dict('sys.modules',{'eccodes':fake}):
                self.assertEqual(inspect_grib_semantics(p,day=DAY,variables=['2m_temperature']),24)
                objs[:]=[{'s':'2t','h':0}]*24
                with self.assertRaises(IngestionError):inspect_grib_semantics(p,day=DAY,variables=['2m_temperature'])


if __name__ == '__main__': unittest.main()
