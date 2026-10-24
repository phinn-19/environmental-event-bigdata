
"""Unit tests for Fire Detection foundations (synthetic fixtures)."""

import unittest
from copy import deepcopy
from pathlib import Path

from src.common.validation import read_json, validate_config

from src.events.fire_detection import (
    filter_fire_candidates,
    haversine_distance_km,
    are_fire_neighbors,
)


ROOT = Path(__file__).resolve().parents[1]


class TestFireDetectionFoundation(unittest.TestCase):

    def setUp(self):
        self.config = validate_config(
            ROOT / "configs/project.yaml"
        )

        self.hotspots = read_json(
            ROOT / "fixtures/curated/fire_hotspots_context.json"
        )

    def test_fixture_has_two_eligible_hotspots(self):
        candidates, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(len(candidates), 2)
        self.assertEqual(counts["total"], 2)
        self.assertEqual(counts["accepted"], 2)

    def test_low_confidence_is_excluded_and_counted(self):
        self.hotspots[0]["confidence_raw"] = "l"

        candidates, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(counts["rejected_confidence"], 1)

    def test_missing_confidence_is_excluded(self):
        self.hotspots[0]["confidence_raw"] = None

        _, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(counts["rejected_confidence"], 1)

    def test_high_confidence_allowed_with_null_frp_and_weather(self):
        self.hotspots[0]["confidence_raw"] = "H"
        self.hotspots[0]["frp_mw"] = None

        candidates, _ = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(len(candidates), 2)

    def test_duplicate_hotspot_id_is_an_error(self):
        duplicate = deepcopy(self.hotspots[0])

        with self.assertRaises(ValueError):
            filter_fire_candidates(
                self.hotspots + [duplicate],
                self.config
            )

    def test_invalid_coordinates_are_excluded(self):
        self.hotspots[0]["latitude"] = 120

        _, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(counts["rejected_invalid"], 1)

    def test_invalid_timestamp_is_excluded(self):
        self.hotspots[0]["timestamp_utc"] = "bad date"

        _, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(counts["rejected_invalid"], 1)

    def test_invalid_frp_is_excluded_but_null_is_allowed(self):
        self.hotspots[0]["frp_mw"] = -1

        _, counts = filter_fire_candidates(
            self.hotspots, self.config
        )

        self.assertEqual(counts["rejected_invalid"], 1)

    def test_haversine_same_point_is_zero(self):
        self.assertAlmostEqual(
            haversine_distance_km(21, 105, 21, 105),
            0.0
        )

    def test_haversine_one_degree_latitude(self):
        self.assertAlmostEqual(
            haversine_distance_km(0, 0, 1, 0),
            111.195,
            places=2
        )

    def test_two_fixture_hotspots_are_neighbors(self):
        self.assertTrue(
            are_fire_neighbors(
                *self.hotspots,
                self.config
            )
        )

    def test_far_apart_hotspots_are_not_neighbors(self):
        self.hotspots[1]["latitude"] = 20.0

        self.assertFalse(
            are_fire_neighbors(
                *self.hotspots,
                self.config
            )
        )

    def test_more_than_24_hours_are_not_neighbors(self):
        self.hotspots[1]["timestamp_utc"] = (
            "2024-11-02T06:18:00Z"
        )

        self.assertFalse(
            are_fire_neighbors(
                *self.hotspots,
                self.config
            )
        )

    def test_exactly_24_hours_is_allowed(self):
        self.hotspots[1]["timestamp_utc"] = (
            "2024-11-02T06:16:00Z"
        )

        self.assertTrue(
            are_fire_neighbors(
                *self.hotspots,
                self.config
            )
        )

    def test_same_hotspot_id_is_not_its_own_neighbor(self):
        self.assertFalse(
            are_fire_neighbors(
                self.hotspots[0],
                self.hotspots[0],
                self.config
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
