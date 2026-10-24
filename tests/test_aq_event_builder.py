
"""Synthetic tests for the AQ event builder (no Spark needed)."""

import unittest
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from src.common.validation import (
    read_json,
    timestamp,
    validate_config,
    validate_events,
)

from src.events.aq_detection import (
    detect_aq_candidates,
    group_consecutive_candidates,
)

from src.events.aq_event_builder import build_aq_events


ROOT = Path(__file__).resolve().parents[1]


class TestAqEventBuilder(unittest.TestCase):

    def setUp(self):
        self.config = validate_config(
            ROOT / "configs/project.yaml"
        )

        self.observations = read_json(
            ROOT / "fixtures/curated/air_quality_context_hourly.json"
        )

        self.baselines = read_json(
            ROOT / "fixtures/curated/air_quality_baseline.json"
        )

    def make_hour(self, hour, value=70.0, baseline=32.0):
        """Create a new synthetic hour without changing fixtures."""

        obs = deepcopy(self.observations[1])
        base = deepcopy(self.baselines[0])

        start = f"2024-11-01T{hour:02d}:00:00Z"
        end = timestamp(start) + timedelta(hours=1)

        obs["timestamp_utc"] = start
        obs["period_end_utc"] = end.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        obs["value_ugm3"] = value
        obs["has_flags"] = False

        base["timestamp_utc"] = start
        base["baseline_ugm3"] = baseline

        return obs, base

    def make_candidates(self, *pairs):
        observations = self.observations + [
            obs for obs, _ in pairs
        ]
        baselines = self.baselines + [
            base for _, base in pairs
        ]

        return detect_aq_candidates(
            observations,
            baselines,
            self.config
        )

    def make_events(self, *pairs):
        candidates = self.make_candidates(*pairs)

        groups = group_consecutive_candidates(
            candidates,
            self.config
        )

        return build_aq_events(
            groups,
            region_id="HANOI"
        )

    # TEST 1: Chi mot candidate thi khong co event
    def test_one_candidate_produces_no_event(self):
        self.assertEqual(self.make_events(), [])

    # TEST 2: Hai gio lien tiep tao mot event hop le
    def test_two_hours_make_one_contract_valid_event(self):
        events = self.make_events(
            self.make_hour(12, 70.0)
        )

        self.assertEqual(validate_events(events), 1)

        event = events[0]

        self.assertEqual(
            event["event_type"], "AIR_QUALITY_ANOMALY"
        )
        self.assertEqual(
            event["start_time_utc"],
            "2024-11-01T11:00:00Z"
        )
        self.assertEqual(
            event["end_time_utc"],
            "2024-11-01T13:00:00Z"
        )

        metrics = event["metrics"]

        self.assertEqual(metrics["peak_ugm3"], 70.0)
        self.assertEqual(metrics["mean_ugm3"], 65.0)
        self.assertEqual(metrics["baseline_ugm3"], 32.0)
        self.assertEqual(metrics["duration_hours"], 2.0)

        self.assertEqual(len(event["evidence_ids"]), 2)
        self.assertEqual(event["severity"], "undetermined")
        self.assertIsNone(event["weather_context"])

    # TEST 3: Baseline cua event phai dung median
    def test_three_hours_use_median_baseline(self):
        events = self.make_events(
            self.make_hour(12, value=100.0, baseline=40.0),
            self.make_hour(13, value=130.0, baseline=80.0),
        )

        self.assertEqual(len(events), 1)

        metrics = events[0]["metrics"]

        self.assertEqual(metrics["baseline_ugm3"], 40.0)
        self.assertAlmostEqual(
            metrics["mean_ugm3"],
            (60 + 100 + 130) / 3
        )
        self.assertEqual(metrics["peak_ugm3"], 130.0)
        self.assertEqual(metrics["duration_hours"], 3.0)

    # TEST 4: Thieu gio thi khong tao event
    def test_gap_does_not_make_event(self):
        self.assertEqual(
            self.make_events(self.make_hour(13)),
            []
        )

    # TEST 5: Hai nhom rieng co ID khac nhau va on dinh
    def test_separate_groups_have_unique_stable_ids(self):
        hours = (
            self.make_hour(12),
            self.make_hour(14),
            self.make_hour(15),
        )

        first_run = self.make_events(*hours)
        second_run = self.make_events(*hours)

        self.assertEqual(len(first_run), 2)
        self.assertNotEqual(
            first_run[0]["event_id"],
            first_run[1]["event_id"]
        )
        self.assertEqual(
            [event["event_id"] for event in first_run],
            [event["event_id"] for event in second_run],
        )

    # TEST 6: Builder khong chap nhan hai gio khong lien tiep
    def test_builder_rejects_nonconsecutive_hours(self):
        candidates = self.make_candidates(
            self.make_hour(13)
        )

        with self.assertRaises(ValueError):
            build_aq_events([candidates], "HANOI")

    # TEST 7: Khong gom hai sensor khac nhau
    def test_builder_rejects_mixed_sensors(self):
        candidates = self.make_candidates(
            self.make_hour(12)
        )

        invalid_group = deepcopy(candidates)
        invalid_group[1]["observation"]["sensor_id"] = (
            "another-sensor"
        )

        with self.assertRaises(ValueError):
            build_aq_events([invalid_group], "HANOI")

    # TEST 8: Khong chap nhan nhom chi co mot gio
    def test_builder_rejects_one_hour_group(self):
        with self.assertRaises(ValueError):
            build_aq_events(
                [[self.make_candidates()[0]]],
                "HANOI"
            )

    # TEST 9: Region khong duoc rong
    def test_builder_rejects_empty_region(self):
        with self.assertRaises(ValueError):
            build_aq_events([], "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
