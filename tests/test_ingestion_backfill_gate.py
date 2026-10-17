"""Fail-closed, read-only tests of the Sprint 06 execution preflight gate."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.ingestion.backfill import PlanItem, plan_backfill
from src.ingestion.backfill_audit import AuditResult
from src.ingestion.backfill_gate import evaluate_gate, main

CONFIG = {
    'regions': {
        'HANOI': {'north': 21.25, 'west': 105.5, 'south': 20.75, 'east': 106.0},
    },
    'ingestion': {
        'openaq': {'page_limit': 100, 'pilot_sensor_ids': [21632]},
        'era5': {'variables': [
            '2m_temperature', '2m_dewpoint_temperature',
            '10m_u_component_of_wind', '10m_v_component_of_wind',
            'total_precipitation',
        ]},
    },
}


class ExecutionGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.items = plan_backfill(
            source='openaq', region_id='HANOI',
            start='2024-10-31', end='2024-11-03',
            sensor_ids=(21632,), max_plan_items=3,
        )

    def gate(self, statuses, *, max_candidates=3):
        def audit(*, root, item, config):
            self.assertEqual(root, self.root)
            self.assertIs(config, CONFIG)
            return AuditResult(item, statuses[item.start], 'test_reason')
        return evaluate_gate(root=self.root, items=self.items,
                             config=CONFIG, max_candidates=max_candidates, auditor=audit)

    def test_live_replay_missing_map_to_skip_hold_candidate(self):
        r = self.gate({'2024-10-31': 'complete', '2024-11-01': 'review',
                       '2024-11-02': 'not_found'})
        self.assertEqual([d.action for d in r.decisions], ['skip', 'hold', 'candidate'])
        self.assertEqual(r.state, 'blocked_review')
        self.assertFalse(r.is_unblocked)

    def test_one_review_blocks_entire_plan_even_with_candidate(self):
        r = self.gate({'2024-10-31': 'not_found', '2024-11-01': 'review',
                       '2024-11-02': 'not_found'})
        self.assertEqual(r.state, 'blocked_review')
        self.assertEqual(r.candidate_count, 2)

    def test_all_complete_no_work(self):
        r = self.gate(dict.fromkeys((i.start for i in self.items), 'complete'))
        self.assertEqual(r.state, 'nothing_to_do')
        self.assertEqual(r.candidate_count, 0)
        self.assertFalse(r.is_unblocked)

    def test_candidate_within_budget_unblocked_but_not_executed(self):
        r = self.gate({'2024-10-31': 'complete', '2024-11-01': 'complete',
                       '2024-11-02': 'not_found'}, max_candidates=1)
        self.assertEqual(r.state, 'ready_for_manual_review')
        self.assertTrue(r.is_unblocked)
        self.assertEqual(r.candidate_count, 1)
        self.assertFalse(any(self.root.iterdir()))

    def test_candidate_budget_exceeded_blocks_all(self):
        r = self.gate(dict.fromkeys((i.start for i in self.items), 'not_found'),
                      max_candidates=2)
        self.assertEqual(r.state, 'blocked_budget')
        self.assertFalse(r.is_unblocked)

    def test_budget_zero_rejected(self):
        with self.assertRaises(ValueError):
            self.gate({}, max_candidates=0)

    def test_budget_above_hard_cap_rejected(self):
        with self.assertRaises(ValueError):
            self.gate({}, max_candidates=101)

    def test_bool_budget_rejected(self):
        with self.assertRaises(ValueError):
            self.gate({}, max_candidates=True)

    def test_empty_plan_rejected(self):
        with self.assertRaises(ValueError):
            evaluate_gate(root=self.root, items=[], config=CONFIG)

    def test_duplicate_plan_items_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            evaluate_gate(root=self.root, items=[self.items[0], self.items[0]], config=CONFIG)

    def test_unknown_audit_status_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'unexpected audit status'):
            self.gate(dict.fromkeys((i.start for i in self.items), 'partial'))

    def test_mismatched_audit_identity_fail_closed(self):
        def wrong_identity(*, root, item, config):
            return AuditResult(self.items[0], 'complete', 'bad')
        with self.assertRaisesRegex(ValueError, 'different plan identity'):
            evaluate_gate(root=self.root, items=self.items[1:], config=CONFIG, auditor=wrong_identity)

    def test_audit_failure_propagates_instead_of_becoming_candidate(self):
        def fail(*, root, item, config):
            raise OSError('bad manifest')
        with self.assertRaises(OSError):
            evaluate_gate(root=self.root, items=self.items, config=CONFIG, auditor=fail)

    def test_default_real_auditor_no_artifacts_no_download(self):
        from src.ingestion.backfill import PlanItem
        item = PlanItem('openaq', 'HANOI', '2024-11-02', '2024-11-03', sensor_id=21632)
        with patch('src.ingestion.openaq.http_get', side_effect=AssertionError('network')):
            result = evaluate_gate(root=self.root, items=[item], config=CONFIG)
        self.assertEqual(result.state, 'ready_for_manual_review')
        self.assertEqual(list(self.root.iterdir()), [])

    def test_cli_read_only_failure_safe(self):
        # Existing config is loaded by the CLI; override the root to an empty temp
        # location, and no network or files should be created.
        with patch('src.ingestion.backfill_gate.ROOT', self.root), \
             patch('src.ingestion.backfill_gate.validate_config', return_value=CONFIG):
            from io import StringIO
            with patch('sys.stdout', new_callable=StringIO) as output:
                rc = main(['--source', 'openaq', '--region', 'HANOI',
                           '--sensor-id', '21632', '--start', '2024-11-02',
                           '--end', '2024-11-03', '--format', 'json',
                           '--config', 'ignored.yaml'])
        self.assertEqual(rc, 0)
        data = json.loads(output.getvalue())
        self.assertFalse(data['execution_authorized'])
        self.assertTrue(data['no_download'])
        self.assertEqual(data['decisions'][0]['action'], 'candidate')
        self.assertEqual(list(self.root.iterdir()), [])


if __name__ == '__main__':
    unittest.main()
