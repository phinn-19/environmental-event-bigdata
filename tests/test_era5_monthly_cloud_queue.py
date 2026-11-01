"""Pure offline tests: no provider calls, no credentials, no uploads."""
import unittest
from unittest import mock

from scripts.bulk import era5_monthly_cloud_queue as q


class GuardTests(unittest.TestCase):
    def test_audited_range_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "SCOPE OVERLAPS"):
            q.make_plan("2024-01", "2024-03")

    def test_historical_months_reverse(self):
        self.assertEqual(q.make_plan("2023-09", "2023-11"),
                         ["2023-11", "2023-10", "2023-09"])

    def test_monthly_request_has_hourly_five_variables(self):
        p = q.core.plan_month("2023-11")
        self.assertEqual(p["expected_fields"], 30 * 24 * 5)
        self.assertEqual(len(p["request"]["day"]), 30)
        self.assertEqual(p["request"]["area"], [24, 102, 8, 110])
        self.assertEqual(p["request"]["data_format"], "grib")

    def test_state_rejects_wrong_request(self):
        s = dict(q.core.plan_month("2023-11"))
        s["request_sha256"] = "not-same"
        with self.assertRaisesRegex(RuntimeError, "mismatch"):
            q.scrub_state(s, "2023-11")

    def test_submit_saves_intent_before_external_call(self):
        events = []
        remote = mock.Mock(request_id="11111111-2222-3333-4444-555555555555", status="accepted")
        client = mock.Mock()
        client.submit.return_value = remote
        class Cloud:
            def write_state(self, m, state):
                events.append(("state", state["status"]))
        with (mock.patch.object(q, "put_cds_rc"),
              mock.patch.object(q.core, "credentials_client", return_value=client),
              mock.patch.object(q.core, "assert_same_remote_request")):
            # check that the first state was persisted before client.submit
            def checked_submit(*args):
                self.assertEqual(events, [("state", "SUBMISSION_INTENT_NO_ID")])
                return remote
            client.submit.side_effect = checked_submit
            q.submit_one(Cloud(), "2023-11")
        self.assertEqual(events, [("state", "SUBMISSION_INTENT_NO_ID"),
                                  ("state", "SUBMITTED")])

    def test_no_retry_for_state_without_id(self):
        plan = q.core.plan_month("2023-11")
        s = {**plan, "status": "SUBMISSION_INTENT_NO_ID"}
        with self.assertRaisesRegex(RuntimeError, "NEVER|ADOPT|INTENT"):
            q.progress_one(mock.Mock(), "2023-11", s, set())

    def test_plan_main_no_network(self):
        with (mock.patch("sys.argv", ["x", "plan", "--from-month", "2023-10",
                                          "--to-month", "2023-11"]),
              mock.patch.object(q, "PrivateBucket", side_effect=AssertionError("NO CLOUD"))):
            q.main()


if __name__ == "__main__":
    unittest.main()
