
"""Unit tests for ST-DBSCAN fire clustering.

All test data are synthetic, not verified real fires.
"""

import unittest
from copy import deepcopy
from pathlib import Path

from src.common.validation import read_json, validate_config
from src.events.fire_detection import (
    are_fire_neighbors,
    cluster_fire_candidates,
    filter_fire_candidates,
)


ROOT = Path(__file__).resolve().parents[1]


class TestFireClustering(unittest.TestCase):

    def setUp(self):
        """Load config and original synthetic fixtures."""

        self.config = validate_config(
            ROOT / "configs/project.yaml"
        )

        self.fixtures = read_json(
            ROOT / "fixtures/curated/fire_hotspots_context.json"
        )

    def make_hotspot(
        self,
        hotspot_id,
        longitude=108.50,
        latitude=12.50,
        time="2024-11-01T06:00:00Z",
    ):
        """Create a synthetic hotspot from a valid fixture."""

        row = deepcopy(self.fixtures[0])

        row["hotspot_id"] = hotspot_id
        row["longitude"] = longitude
        row["latitude"] = latitude
        row["timestamp_utc"] = time
        row["confidence_raw"] = "n"

        return row

    @staticmethod
    def cluster_ids(clusters):
        """Convert cluster rows into lists of IDs for comparisons."""

        return [
            [item["hotspot_id"] for item in cluster]
            for cluster in clusters
        ]

    # TEST 1: Hai hotspot fixture tao mot cum
    def test_two_fixture_hotspots_make_one_cluster(self):

        candidates, counts = filter_fire_candidates(
            self.fixtures,
            self.config
        )

        clusters = cluster_fire_candidates(
            candidates,
            self.config
        )

        self.assertEqual(counts["accepted"], 2)
        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0]), 2)

    # TEST 2: Mot diem don le khong du tao cum
    def test_single_hotspot_is_noise(self):

        a = self.make_hotspot("A")

        clusters = cluster_fire_candidates(
            [a],
            self.config
        )

        self.assertEqual(clusters, [])

    # TEST 3: Hai diem qua xa nhau khong tao cum
    def test_far_hotspots_do_not_cluster(self):

        a = self.make_hotspot("A", latitude=12.50)
        b = self.make_hotspot("B", latitude=13.50)

        clusters = cluster_fire_candidates(
            [a, b],
            self.config
        )

        self.assertEqual(clusters, [])

    # TEST 4: Hai diem cach nhau hon 24 gio
    def test_time_gap_breaks_neighbor_relation(self):

        a = self.make_hotspot(
            "A",
            time="2024-11-01T00:00:00Z"
        )

        b = self.make_hotspot(
            "B",
            time="2024-11-02T01:00:00Z"
        )

        clusters = cluster_fire_candidates(
            [a, b],
            self.config
        )

        self.assertEqual(clusters, [])

    # TEST 5: A-B va B-C gan nhau, A-C qua xa
    def test_spatial_chain_forms_one_cluster(self):

        a = self.make_hotspot("A", longitude=108.50)
        b = self.make_hotspot("B", longitude=108.53)
        c = self.make_hotspot("C", longitude=108.56)

        self.assertTrue(
            are_fire_neighbors(a, b, self.config)
        )

        self.assertTrue(
            are_fire_neighbors(b, c, self.config)
        )

        self.assertFalse(
            are_fire_neighbors(a, c, self.config)
        )

        clusters = cluster_fire_candidates(
            [a, b, c],
            self.config
        )

        self.assertEqual(
            self.cluster_ids(clusters),
            [["A", "B", "C"]]
        )

    # TEST 6: Cum noi chuoi co the dai hon 24 gio
    def test_temporal_chain_can_exceed_24_hours(self):

        a = self.make_hotspot(
            "A",
            time="2024-11-01T00:00:00Z"
        )

        b = self.make_hotspot(
            "B",
            time="2024-11-01T20:00:00Z"
        )

        c = self.make_hotspot(
            "C",
            time="2024-11-02T16:00:00Z"
        )

        # A va C cach nhau 40 gio
        self.assertFalse(
            are_fire_neighbors(a, c, self.config)
        )

        clusters = cluster_fire_candidates(
            [a, b, c],
            self.config
        )

        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0]), 3)

    # TEST 7: Hai nhom o hai khu vuc khac nhau
    def test_two_separate_clusters(self):

        a = self.make_hotspot("A", longitude=108.50)
        b = self.make_hotspot("B", longitude=108.51)

        c = self.make_hotspot("C", longitude=109.50)
        d = self.make_hotspot("D", longitude=109.51)

        clusters = cluster_fire_candidates(
            [a, b, c, d],
            self.config
        )

        self.assertEqual(
            self.cluster_ids(clusters),
            [["A", "B"], ["C", "D"]]
        )

    # TEST 8: Dao thu tu dau vao khong doi ket qua
    def test_input_order_does_not_change_clusters(self):

        a = self.make_hotspot("A", longitude=108.50)
        b = self.make_hotspot("B", longitude=108.51)
        c = self.make_hotspot("C", longitude=109.50)
        d = self.make_hotspot("D", longitude=109.51)

        first = cluster_fire_candidates(
            [a, b, c, d],
            self.config
        )

        second = cluster_fire_candidates(
            [d, b, a, c],
            self.config
        )

        self.assertEqual(
            self.cluster_ids(first),
            self.cluster_ids(second)
        )

    # TEST 9: Thay doi min_samples sang 3
    def test_min_samples_three(self):

        custom_config = deepcopy(self.config)
        custom_config["algorithms"]["fire"]["min_samples"] = 3

        a = self.make_hotspot("A", longitude=108.50)
        b = self.make_hotspot("B", longitude=108.53)
        c = self.make_hotspot("C", longitude=108.56)

        # Chi hai diem thi chua du mat do
        self.assertEqual(
            cluster_fire_candidates(
                [a, b],
                custom_config
            ),
            []
        )

        # Ba diem voi B la core point
        clusters = cluster_fire_candidates(
            [a, b, c],
            custom_config
        )

        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0]), 3)

    # TEST 10: ID trung nhau phai bao loi
    def test_duplicate_hotspot_id_is_rejected(self):

        a = self.make_hotspot("A")
        duplicate = deepcopy(a)

        with self.assertRaises(ValueError):
            cluster_fire_candidates(
                [a, duplicate],
                self.config
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
