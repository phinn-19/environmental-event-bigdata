
"""Synthetic tests for fire event building."""

import unittest
from copy import deepcopy
from pathlib import Path

from src.common.validation import read_json, validate_events
from src.events.fire_event_builder import build_fire_events


ROOT = Path(__file__).resolve().parents[1]


class TestFireEventBuilder(unittest.TestCase):

    def setUp(self):
        self.hotspots = read_json(
            ROOT / "fixtures/curated/fire_hotspots_context.json"
        )

    def build(self, clusters=None):
        return build_fire_events(
            [self.hotspots] if clusters is None else clusters,
            region_id="VN_BBOX",
        )

    def test_fixture_makes_valid_event(self):
        events = self.build()

        self.assertEqual(validate_events(events), 1)

        event = events[0]

        self.assertEqual(
            event["event_type"], "FIRE_HOTSPOT_CLUSTER"
        )
        self.assertEqual(
            event["start_time_utc"],
            "2024-11-01T06:16:00Z"
        )
        self.assertEqual(
            event["end_time_utc"],
            "2024-11-01T06:18:00Z"
        )

        self.assertAlmostEqual(
            event["centroid_latitude"], 12.50036
        )
        self.assertAlmostEqual(
            event["centroid_longitude"], 108.50036
        )

        metrics = event["metrics"]

        self.assertEqual(metrics["hotspot_count"], 2)
        self.assertEqual(metrics["max_frp_mw"], 8.2)
        self.assertAlmostEqual(metrics["sum_frp_mw"], 13.0)
        self.assertEqual(metrics["satellites"], ["N20"])

        self.assertEqual(
            event["evidence_ids"],
            ["synthetic-hotspot-001", "synthetic-hotspot-002"]
        )
        self.assertIsNone(event["weather_context"])

    def test_empty_clusters_return_empty_events(self):
        self.assertEqual(self.build([]), [])

    def test_input_order_does_not_change_event(self):
        event1 = self.build()[0]
        event2 = self.build(
            [list(reversed(self.hotspots))]
        )[0]

        self.assertEqual(event1, event2)

    def test_all_null_frp_stays_null(self):
        rows = deepcopy(self.hotspots)

        for row in rows:
            row["frp_mw"] = None

        event = self.build([rows])[0]

        self.assertIsNone(event["metrics"]["max_frp_mw"])
        self.assertIsNone(event["metrics"]["sum_frp_mw"])

    def test_partial_null_frp_only_sums_available_values(self):
        rows = deepcopy(self.hotspots)
        rows[0]["frp_mw"] = None

        event = self.build([rows])[0]

        self.assertEqual(event["metrics"]["max_frp_mw"], 8.2)
        self.assertEqual(event["metrics"]["sum_frp_mw"], 8.2)

    def test_identical_timestamp_keeps_end_exclusive(self):
        rows = deepcopy(self.hotspots)
        rows[1]["timestamp_utc"] = rows[0]["timestamp_utc"]

        event = self.build([rows])[0]

        self.assertEqual(
            event["end_time_utc"],
            "2024-11-01T06:17:00Z"
        )

    def test_unique_satellites_sorted(self):
        rows = deepcopy(self.hotspots)
        rows[0]["satellite"] = "N21"

        event = self.build([rows])[0]

        self.assertEqual(
            event["metrics"]["satellites"],
            ["N20", "N21"]
        )

    def test_single_hotspot_rejected(self):
        with self.assertRaises(ValueError):
            self.build([[self.hotspots[0]]])

    def test_duplicate_hotspot_rejected(self):
        with self.assertRaises(ValueError):
            self.build([[
                self.hotspots[0],
                self.hotspots[0]
            ]])

    def test_same_id_across_clusters_rejected(self):
        rows = deepcopy(self.hotspots)

        with self.assertRaises(ValueError):
            self.build([rows, rows])

    def test_invalid_coordinates_rejected(self):
        rows = deepcopy(self.hotspots)
        rows[0]["longitude"] = 200

        with self.assertRaises(ValueError):
            self.build([rows])

    def test_negative_frp_rejected(self):
        rows = deepcopy(self.hotspots)
        rows[0]["frp_mw"] = -1

        with self.assertRaises(ValueError):
            self.build([rows])

    def test_empty_region_rejected(self):
        with self.assertRaises(ValueError):
            build_fire_events([], region_id="")


if __name__ == "__main__":
    unittest.main(verbosity=2)
