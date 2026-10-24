"""Offline tests: no real Gemini/Groq API calls and no API keys required."""

import json
import unittest
from copy import deepcopy
from pathlib import Path

from src.dashboard.services.llm_service import (
    LLMServiceError,
    MAX_EVIDENCE_IDS,
    build_prompt,
    event_fingerprint,
    explain_event,
)

ROOT = Path(__file__).resolve().parents[1]


class HTTPProblem(Exception):
    def __init__(self, status_code):
        self.status_code = status_code


class GeminiProblem(Exception):
    def __init__(self, code):
        self.code = code


class TestLLMService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = json.loads(
            (ROOT / "fixtures/events/events_mock.json").read_text(encoding="utf-8")
        )

    def setUp(self):
        self.event = deepcopy(self.fixtures[0])
        self.calls = []

    def primary(self, key, model, prompt):
        self.calls.append("gemini")
        return "Gemini: dữ liệu đã được diễn giải."

    def secondary(self, key, model, prompt):
        self.calls.append("groq")
        return "Groq: dữ liệu đã được diễn giải."

    def explain(self, **overrides):
        kwargs = {
            "gemini_api_key": "dummy-gemini-key",
            "groq_api_key": "dummy-groq-key",
            "gemini_call": self.primary,
            "groq_call": self.secondary,
        }
        kwargs.update(overrides)
        return explain_event(self.event, **kwargs)

    def test_gemini_primary_success(self):
        result = self.explain()
        self.assertEqual(self.calls, ["gemini"])
        self.assertEqual(result.provider, "Gemini")
        self.assertFalse(result.used_fallback)

    def test_fallback_on_gemini_429(self):
        def rate_limited(*_):
            self.calls.append("gemini")
            raise GeminiProblem(429)
        result = self.explain(gemini_call=rate_limited)
        self.assertEqual(self.calls, ["gemini", "groq"])
        self.assertEqual(result.provider, "Groq")
        self.assertTrue(result.used_fallback)

    def test_fallback_on_503(self):
        def unavailable(*_):
            raise HTTPProblem(503)
        result = self.explain(gemini_call=unavailable)
        self.assertTrue(result.used_fallback)

    def test_no_fallback_on_invalid_key_401(self):
        def invalid(*_):
            raise GeminiProblem(401)
        with self.assertRaises(LLMServiceError):
            self.explain(gemini_call=invalid)
        self.assertEqual(self.calls, [])

    def test_no_fallback_on_403(self):
        def invalid(*_):
            raise GeminiProblem(403)
        with self.assertRaises(LLMServiceError):
            self.explain(gemini_call=invalid)
        self.assertEqual(self.calls, [])

    def test_no_fallback_when_disabled(self):
        def limited(*_):
            self.calls.append("gemini")
            raise HTTPProblem(429)
        with self.assertRaises(LLMServiceError):
            self.explain(gemini_call=limited, allow_fallback=False)
        self.assertEqual(self.calls, ["gemini"])

    def test_no_fallback_when_key_missing(self):
        def limited(*_):
            raise HTTPProblem(429)
        with self.assertRaises(LLMServiceError):
            self.explain(gemini_call=limited, groq_api_key="")

    def test_fallback_failure_is_explicit(self):
        def limited(*_):
            raise HTTPProblem(429)
        def also_limited(*_):
            raise HTTPProblem(429)
        with self.assertRaisesRegex(LLMServiceError, "Cả Gemini và Groq"):
            self.explain(gemini_call=limited, groq_call=also_limited)

    def test_no_primary_key(self):
        with self.assertRaisesRegex(LLMServiceError, "GEMINI_API_KEY"):
            self.explain(gemini_api_key="")

    def test_no_fallback_when_primary_empty(self):
        def empty(*_):
            self.calls.append("gemini")
            return ""
        with self.assertRaises(LLMServiceError):
            self.explain(gemini_call=empty)
        self.assertEqual(self.calls, ["gemini"])

    def test_weather_null_is_preserved(self):
        prompt = build_prompt(self.event)
        self.assertIn('"weather_context": null', prompt)
        self.assertIn('"source": "OpenAQ"', prompt)

    def test_fire_metrics_are_preserved(self):
        prompt = build_prompt(deepcopy(self.fixtures[1]))
        self.assertIn('"hotspot_count": 2', prompt)
        self.assertIn('"source": "NASA FIRMS"', prompt)

    def test_evidence_ids_are_bounded(self):
        self.event["evidence_ids"] = [f"e-{i}" for i in range(40)]
        prompt = build_prompt(self.event)
        self.assertIn('"evidence_count": 40', prompt)
        self.assertIn(f'"e-{MAX_EVIDENCE_IDS-1}"', prompt)
        self.assertNotIn(f'"e-{MAX_EVIDENCE_IDS}"', prompt)

    def test_invalid_event_rejected_before_any_network(self):
        self.event.pop("event_type")
        with self.assertRaises(Exception):
            self.explain()
        self.assertEqual(self.calls, [])

    def test_changed_event_changes_fingerprint(self):
        before = event_fingerprint(self.event)
        self.event["metrics"]["peak_ugm3"] = 95
        self.assertNotEqual(before, event_fingerprint(self.event))


if __name__ == "__main__":
    unittest.main(verbosity=2)
