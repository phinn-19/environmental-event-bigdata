"""No-network tests of the mock-only execution rehearsal; no provider APIs."""
from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from src.ingestion.backfill import plan_backfill
from src.ingestion.backfill_audit import AuditResult
from src.ingestion.backfill_execute import main, rehearse_execution

CONFIG = {
    'regions': {'HANOI': {'north': 21.25, 'west': 105.5, 'south': 20.75, 'east': 106.0}},
    'ingestion': {
        'openaq': {'page_limit': 100, 'pilot_sensor_ids': [21632]},
        'era5': {'variables': ['2m_temperature', '2m_dewpoint_temperature',
                              '10m_u_component_of_wind', '10m_v_component_of_wind',
                              'total_precipitation']},
    },
}


class RehearsalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.items = plan_backfill(source='openaq', region_id='HANOI',
                                   start='2024-11-01', end='2024-11-04',
                                   sensor_ids=(21632,), max_plan_items=3)
        self.calls: list[str] = []

    def audit_map(self, statuses):
        def audit(*, root, item, config):
            self.assertEqual(root, self.root)
            self.assertIs(config, CONFIG)
            value = statuses[item.start]
            if callable(value):
                value = value()
            return AuditResult(item, value, 'test')
        return audit

    def run_work(self, statuses, **kw):
        kw.setdefault('approve_simulation', True)
        kw.setdefault('mock_dispatcher', lambda item: self.calls.append(item.start))
        return rehearse_execution(root=self.root, items=self.items, config=CONFIG,
                                  auditor=self.audit_map(statuses), **kw)

    def test_candidate_only_rehearsed_not_complete(self):
        result = self.run_work({'2024-11-01': 'complete', '2024-11-02': 'not_found',
                                '2024-11-03': 'complete'})
        self.assertEqual(result.state, 'simulated')
        self.assertEqual(self.calls, ['2024-11-02'])
        self.assertEqual(len(result.simulated), 1)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_review_blocks_all_callbacks(self):
        result = self.run_work({'2024-11-01': 'not_found', '2024-11-02': 'review',
                                '2024-11-03': 'not_found'})
        self.assertEqual(result.state, 'blocked_review')
        self.assertEqual(self.calls, [])

    def test_budget_blocks_all_callbacks(self):
        result = self.run_work(dict.fromkeys((i.start for i in self.items), 'not_found'),
                               max_candidates=2)
        self.assertEqual(result.state, 'blocked_budget')
        self.assertFalse(self.calls)

    def test_no_approval_blocks_callback(self):
        result = self.run_work(dict.fromkeys((i.start for i in self.items), 'not_found'),
                               approve_simulation=False)
        self.assertEqual(result.state, 'approval_required')
        self.assertFalse(self.calls)

    def test_nothing_to_do(self):
        result = self.run_work(dict.fromkeys((i.start for i in self.items), 'complete'))
        self.assertEqual(result.state, 'nothing_to_do')
        self.assertEqual(result.simulated, ())

    def test_changed_to_review_before_dispatch_aborts(self):
        n = iter(('not_found', 'review'))
        result = self.run_work({'2024-11-01': lambda: next(n),
                                '2024-11-02': 'complete', '2024-11-03': 'complete'})
        self.assertEqual(result.state, 'aborted_state_change')
        self.assertFalse(self.calls)

    def test_changed_to_complete_before_dispatch_aborts(self):
        n = iter(('not_found', 'complete'))
        result = self.run_work({'2024-11-01': lambda: next(n),
                                '2024-11-02': 'complete', '2024-11-03': 'complete'})
        self.assertEqual(result.state, 'aborted_state_change')
        self.assertFalse(self.calls)

    def test_second_candidate_drift_stops_remaining(self):
        n = iter(('not_found', 'review'))
        result = self.run_work({'2024-11-01': 'not_found',
                                '2024-11-02': lambda: next(n),
                                '2024-11-03': 'not_found'})
        self.assertEqual(result.state, 'aborted_state_change')
        self.assertEqual(self.calls, ['2024-11-01'])
        self.assertEqual(len(result.simulated), 1)

    def test_mock_failure_propagates_and_stops(self):
        def fail(_):
            raise RuntimeError('simulation failure')
        with self.assertRaisesRegex(RuntimeError, 'simulation failure'):
            self.run_work(dict.fromkeys((i.start for i in self.items), 'not_found'),
                          mock_dispatcher=fail)
        self.assertFalse(self.calls)

    def test_mock_required_for_approved_run(self):
        with self.assertRaisesRegex(ValueError, 'mock_dispatcher'):
            self.run_work(dict.fromkeys((i.start for i in self.items), 'not_found'),
                          mock_dispatcher=None)

    def test_approval_must_be_bool(self):
        with self.assertRaisesRegex(ValueError, 'boolean'):
            self.run_work(dict.fromkeys((i.start for i in self.items), 'not_found'),
                          approve_simulation=1)

    def test_invalid_status_fails_closed(self):
        with self.assertRaises(ValueError):
            self.run_work({'2024-11-01': 'partial', '2024-11-02': 'complete',
                           '2024-11-03': 'complete'})
        self.assertFalse(self.calls)

    def test_cli_no_network_or_raw_writes_even_with_approval(self):
        with patch('src.ingestion.backfill_execute.ROOT', self.root), \
             patch('src.ingestion.backfill_execute.validate_config', return_value=CONFIG), \
             patch('src.ingestion.openaq.ingest_openaq', side_effect=AssertionError('network')), \
             patch('src.ingestion.firms.ingest_firms', side_effect=AssertionError('network')), \
             patch('src.ingestion.era5.ingest_era5', side_effect=AssertionError('network')), \
             patch('sys.stdout', new_callable=StringIO) as out:
            rc = main(['--source', 'openaq', '--region', 'HANOI',
                       '--sensor-id', '21632', '--start', '2024-11-02',
                       '--end', '2024-11-03', '--approve-simulation',
                       '--format', 'json', '--config', 'ignored.yaml'])
        self.assertEqual(rc, 0)
        data = json.loads(out.getvalue())
        self.assertEqual(data['simulated_count'], 1)
        self.assertEqual(data['network_calls'], 0)
        self.assertEqual(data['raw_writes'], 0)
        self.assertFalse(data['live_execution_authorized'])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_cli_approval_required_no_mock_called(self):
        with patch('src.ingestion.backfill_execute.ROOT', self.root), \
             patch('src.ingestion.backfill_execute.validate_config', return_value=CONFIG), \
             patch('sys.stdout', new_callable=StringIO) as out:
            rc = main(['--source', 'openaq', '--region', 'HANOI', '--sensor-id', '21632',
                       '--start', '2024-11-02', '--end', '2024-11-03',
                       '--format', 'json', '--config', 'ignored.yaml'])
        self.assertEqual(rc, 0)
        data = json.loads(out.getvalue())
        self.assertEqual(data['state'], 'approval_required')
        self.assertEqual(data['simulated_count'], 0)


if __name__ == '__main__':
    unittest.main()
