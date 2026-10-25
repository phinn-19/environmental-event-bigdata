
"""Tests for AQ candidate detection and consecutive grouping."""

import unittest
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from src.common.validation import (
    read_json,
    timestamp,
    validate_config,
)

from src.events.aq_detection import (
    detect_aq_candidates,
    group_consecutive_candidates,
)


ROOT = Path(__file__).resolve().parents[1]


class TestAqDetection(unittest.TestCase):

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

    # Tao du lieu synthetic de test
    def make_hour(
        self,
        time_str,
        value=70.0,
        sensor_id="21632",
        flagged=False
    ):
        obs = deepcopy(self.observations[1])

        obs["sensor_id"] = sensor_id
        obs["timestamp_utc"] = time_str

        obs["period_end_utc"] = (
            timestamp(time_str) + timedelta(hours=1)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        obs["value_ugm3"] = value
        obs["has_flags"] = flagged

        # Khong sao chep weather context cu
        for field in (
            "weather_grid_id",
            "weather_timestamp_utc",
            "weather_distance_km",
            "temperature_c",
            "wind_u_ms",
            "wind_v_ms",
            "precipitation_mm_1h"
        ):
            obs[field] = None

        base = deepcopy(self.baselines[0])
        base["sensor_id"] = sensor_id
        base["timestamp_utc"] = time_str

        return obs, base

    # Ham ho tro phat hien candidate
    def detect_with(self, *new_pairs):

        observations = self.observations + [
            obs for obs, _ in new_pairs
        ]

        baselines = self.baselines + [
            base for _, base in new_pairs
        ]

        return detect_aq_candidates(
            observations,
            baselines,
            self.config
        )

    # ===== 8 TEST CANDIDATE =====

    def test_original_fixture_has_one_candidate(self):
        result = self.detect_with()

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0]["observation"]["timestamp_utc"],
            "2024-11-01T11:00:00Z"
        )
        self.assertEqual(result[0]["baseline_ugm3"], 32.0)

    def test_adding_synthetic_hour_creates_two_candidates(self):
        result = self.detect_with(
            self.make_hour("2024-11-01T12:00:00Z")
        )

        self.assertEqual(len(result), 2)

    def test_flagged_is_not_candidate(self):
        self.observations[1]["has_flags"] = True
        self.assertEqual(self.detect_with(), [])

    def test_unusable_baseline_is_not_candidate(self):
        self.baselines[0]["is_usable"] = False
        self.assertEqual(self.detect_with(), [])

    def test_not_enough_samples_is_not_candidate(self):
        self.baselines[0]["sample_count"] = 9
        self.assertEqual(self.detect_with(), [])

    def test_missing_baseline_is_not_candidate(self):
        result = detect_aq_candidates(
            self.observations,
            [],
            self.config
        )
        self.assertEqual(result, [])

    def test_duplicate_observation_raises_error(self):
        with self.assertRaises(ValueError):
            detect_aq_candidates(
                self.observations + [self.observations[1]],
                self.baselines,
                self.config
            )

    def test_duplicate_baseline_raises_error(self):
        with self.assertRaises(ValueError):
            detect_aq_candidates(
                self.observations,
                self.baselines + [self.baselines[0]],
                self.config
            )

    # ===== 6 TEST GOM EVENT MOI =====

    def test_single_candidate_is_not_an_event(self):
        candidates = self.detect_with()

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(groups, [])

    def test_two_consecutive_hours_one_group(self):
        candidates = self.detect_with(
            self.make_hour("2024-11-01T12:00:00Z")
        )

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)

        times = [
            item["observation"]["timestamp_utc"]
            for item in groups[0]
        ]

        self.assertEqual(times, [
            "2024-11-01T11:00:00Z",
            "2024-11-01T12:00:00Z"
        ])

    def test_missing_hour_breaks_group(self):
        # 11h va 13h cach nhau 2 gio
        candidates = self.detect_with(
            self.make_hour("2024-11-01T13:00:00Z")
        )

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(groups, [])

    def test_different_sensor_cannot_form_group(self):
        # 11h sensor 21632, 12h sensor B
        candidates = self.detect_with(
            self.make_hour(
                "2024-11-01T12:00:00Z",
                sensor_id="B"
            )
        )

        self.assertEqual(len(candidates), 2)

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(groups, [])

    def test_three_consecutive_hours_one_group(self):
        candidates = self.detect_with(
            self.make_hour("2024-11-01T12:00:00Z"),
            self.make_hour(
                "2024-11-01T13:00:00Z",
                value=80.0
            )
        )

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 3)

    def test_flagged_hour_breaks_group(self):
        # 12h co flag nen khong duoc noi 11h voi 13h
        candidates = self.detect_with(
            self.make_hour(
                "2024-11-01T12:00:00Z",
                flagged=True
            ),
            self.make_hour("2024-11-01T13:00:00Z")
        )

        self.assertEqual(len(candidates), 2)

        groups = group_consecutive_candidates(
            candidates, self.config
        )

        self.assertEqual(groups, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
