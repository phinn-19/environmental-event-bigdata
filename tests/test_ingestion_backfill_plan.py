"""Pure offline backfill plan tests; zero API access."""
import unittest

from src.ingestion.backfill import plan_backfill


class BackfillPlanTests(unittest.TestCase):
    def test_openaq_days_and_sensors(self):
        items = plan_backfill(source="openaq", region_id="HANOI",
                              start="2024-11-01", end="2024-11-04",
                              sensor_ids=(21632, 21633))
        self.assertEqual(len(items), 6)
        self.assertEqual(items[0].sensor_id, 21632)
        self.assertEqual(items[0].start, "2024-11-01")
        self.assertEqual(items[0].end, "2024-11-02")
        self.assertEqual(items[-1].end, "2024-11-04")

    def test_firms_chunks_five_days_max(self):
        items = plan_backfill(source="firms", region_id="VN_BBOX",
                              start="2024-10-30", end="2024-11-07",
                              product="VIIRS_NOAA20_SP")
        self.assertEqual([(x.start, x.end) for x in items], [
            ("2024-10-30", "2024-11-04"),
            ("2024-11-04", "2024-11-07"),
        ])

    def test_era5_day_jobs(self):
        items = plan_backfill(source="era5", region_id="HANOI",
                              start="2024-11-01", end="2024-11-04")
        self.assertEqual(len(items), 3)
        self.assertTrue(all(x.product is None and x.sensor_id is None for x in items))

    def test_end_exclusive(self):
        self.assertEqual(len(plan_backfill(source="era5", region_id="HANOI",
                                           start="2024-11-01", end="2024-11-02")), 1)

    def test_reject_equal_and_reverse_dates(self):
        for start, end in (("2024-11-01", "2024-11-01"),
                           ("2024-11-03", "2024-11-01")):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                plan_backfill(source="era5", region_id="HANOI", start=start, end=end)

    def test_reject_malformed_dates(self):
        for bad_date in ("2024-1-01", "2024-02-30", "tomorrow"):
            with self.subTest(date=bad_date), self.assertRaises(ValueError):
                plan_backfill(source="era5", region_id="HANOI",
                              start=bad_date, end="2024-11-02")

    def test_budget_guard(self):
        with self.assertRaisesRegex(ValueError, "exceeding"):
            plan_backfill(source="era5", region_id="HANOI",
                          start="2024-01-01", end="2024-03-01", max_plan_items=10)

    def test_invalid_sensor_and_product_options(self):
        with self.assertRaises(ValueError):
            plan_backfill(source="openaq", region_id="HANOI", start="2024-11-01",
                          end="2024-11-02", sensor_ids=(21632, 21632))
        with self.assertRaises(ValueError):
            plan_backfill(source="firms", region_id="VN_BBOX", start="2024-11-01",
                          end="2024-11-02")
        with self.assertRaises(ValueError):
            plan_backfill(source="era5", region_id="HANOI", start="2024-11-01",
                          end="2024-11-02", product="VIIRS_NOAA20_SP")

    def test_budget_type_and_ceiling(self):
        for invalid in (0, -1, True, 10001):
            with self.subTest(cap=invalid), self.assertRaises(ValueError):
                plan_backfill(source="era5", region_id="HANOI",
                              start="2024-11-01", end="2024-11-02",
                              max_plan_items=invalid)


if __name__ == "__main__":
    unittest.main()
