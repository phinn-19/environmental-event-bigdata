"""Opt-in OpenAQ one-day backfill with offline gate and stage-then-publish.

No ERA5/FIRMS live dispatch. Entire day is validated in a private staging tree
before publication. No existing Raw/manifest is intentionally replaced.

Security limitations: this is single-writer best-effort coordination for this
runner; direct ingestion CLI and other machines do NOT honor its local lock.
Use a single ingestion writer per shared data root.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

from src.common.validation import validate_config
from .backfill import ROOT, PlanItem, plan_backfill
from .backfill_audit import audit_item
from .backfill_gate import evaluate_gate
from .dotenv import load_project_env
from .openaq import IngestionError, _NoRedirect, _paths, ingest_openaq


MAX_HTTP_REQUESTS_HARD = 10
MAX_RUN_BYTES_HARD = 16 * 1024 * 1024


@dataclass(frozen=True)
class LiveResult:
    state: str  # blocked_* | approval_required | skipped | published | failed
    requests: int
    downloaded_bytes: int
    pages: int
    reason: str


def _validate_limits(*, max_http_requests: int, max_bytes: int) -> None:
    if type(max_http_requests) is not int or not 1 <= max_http_requests <= MAX_HTTP_REQUESTS_HARD:
        raise ValueError('max_http_requests must be 1..10')
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_RUN_BYTES_HARD:
        raise ValueError('max_bytes must be 1..16777216')


def execute_one_openaq_day(
    *, root: Path, item: PlanItem, config: dict, approve_live: bool,
    api_key: str | None = None, max_http_requests: int = 3,
    max_bytes: int = 2 * 1024 * 1024,
    transport: Callable | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> LiveResult:
    """Preflight, recheck, stage, validate and publish exactly one day/sensor.

    Network is POSSIBLE only with approve_live=True and supplied API key.
    Transport injection permits real-adapter tests with zero HTTP calls.
    No automatic downloads for review/complete. Exception details are never
    reflected in user-visible output; error reason is a sanitized code.
    """
    _validate_limits(max_http_requests=max_http_requests, max_bytes=max_bytes)
    if type(approve_live) is not bool:
        raise ValueError('approve_live must be boolean')
    if item.source != 'openaq' or type(item.sensor_id) is not int or item.sensor_id <= 0 or item.product is not None:
        raise ValueError('v1 supports OpenAQ hourly only')
    expected = plan_backfill(source='openaq', region_id=item.region_id,
                             start=item.start, end=item.end,
                             sensor_ids=(item.sensor_id,), max_plan_items=1)
    if expected != (item,):
        raise ValueError('v1 supports exactly one UTC day')
    if item.region_id not in config['regions']:
        raise ValueError('unknown region ID')
    gate = evaluate_gate(root=root, items=expected, config=config, max_candidates=1)
    if gate.state == 'nothing_to_do':
        return LiveResult('skipped', 0, 0, 0, 'already_complete')
    if gate.state != 'ready_for_manual_review':
        return LiveResult('blocked_review', 0, 0, 0, gate.decisions[0].reason)
    if not approve_live:
        return LiveResult('approval_required', 0, 0, 0, 'dry_run_only')
    if not api_key:
        raise ValueError('OPENAQ_API_KEY is missing')

    staging_parent = root / 'data/raw/openaq/_staging'
    locks = root / 'data/raw/openaq/_locks'
    # Avoid filesystem mutation before approval; a local lock prevents two
    # instances of this executor from publishing the same day concurrently.
    locks.mkdir(parents=True, exist_ok=True)
    lock = locks / f'sensor={item.sensor_id}-day={item.start}.lock'
    try:
        with lock.open('x', encoding='ascii') as stream:
            stream.write('local_exclusive_lock\n')
    except FileExistsError:
        return LiveResult('blocked_review', 0, 0, 0, 'active_or_stale_lock')

    calls = 0
    bytes_received = 0
    try:
        # Re-audit AFTER lock, directly before any possible provider call.
        status = audit_item(root=root, item=item, config=config)
        if status.status != 'not_found':
            return LiveResult('blocked_review', 0, 0, 0, 'candidate_changed_after_lock')
        page_limit = config['ingestion']['openaq'].get('page_limit', 100)
        if type(page_limit) is not int or not 1 <= page_limit <= 1000:
            raise ValueError('invalid OpenAQ page_limit')
        staging_parent.mkdir(parents=True, exist_ok=True)

        def bounded_transport(url: str, headers: dict, timeout: float) -> bytes:
            nonlocal calls, bytes_received
            if calls >= max_http_requests:
                raise IngestionError('request_budget_exceeded')
            calls += 1  # includes retries, not just successful HTTP responses
            if transport is None:
                # Limit bytes DURING real HTTP read; do not buffer an unbounded response.
                req = urllib.request.Request(url, headers=headers, method='GET')
                opener = urllib.request.build_opener(_NoRedirect)
                with opener.open(req, timeout=timeout) as response:
                    if response.status != 200:
                        raise IngestionError(f'http_{response.status}')
                    payload = response.read(max_bytes - bytes_received + 1)
            else:
                payload = transport(url, headers, timeout)
            if type(payload) is not bytes:
                raise IngestionError('invalid_http_payload')
            bytes_received += len(payload)
            if bytes_received > max_bytes:
                raise IngestionError('byte_budget_exceeded')
            return payload

        with tempfile.TemporaryDirectory(prefix='day-', dir=staging_parent) as tempdir:
            stage_root = Path(tempdir)
            try:
                summary = ingest_openaq(
                    root=stage_root, region_id=item.region_id,
                    sensor_ids=[item.sensor_id], start=item.start, end=item.end,
                    api_key=api_key, page_limit=page_limit,
                    max_pages=max_http_requests, max_retries=0,
                    overwrite=False, transport=bounded_transport, sleep=sleep,
                )
            except (OSError, IngestionError, TypeError, ValueError):
                return LiveResult('failed', calls, bytes_received, 0, 'staged_ingestion_error')
            if not summary.ok:
                return LiveResult('failed', calls, bytes_received, summary.pages, 'staged_ingestion_failed')
            checked = audit_item(root=stage_root, item=item, config=config)
            if checked.status != 'complete':
                return LiveResult('failed', calls, bytes_received, summary.pages, 'staged_audit_not_complete')
            # Snapshot and verify the same candidate once more before publishing.
            if audit_item(root=root, item=item, config=config).status != 'not_found':
                return LiveResult('blocked_review', calls, bytes_received, summary.pages,
                                  'candidate_changed_before_publish')
            day = date.fromisoformat(item.start)
            pending: list[tuple[Path, Path]] = []
            for n in range(1, summary.pages + 1):
                source_raw, source_manifest = _paths(stage_root, item.sensor_id, day, n)
                target_raw, target_manifest = _paths(root, item.sensor_id, day, n)
                if not source_raw.is_file() or not source_manifest.is_file():
                    return LiveResult('failed', calls, bytes_received, summary.pages, 'incomplete_stage')
                if target_raw.exists() or target_manifest.exists():
                    return LiveResult('blocked_review', calls, bytes_received, summary.pages,
                                      'destination_not_empty')
                pending.extend(((source_raw, target_raw), (source_manifest, target_manifest)))
            written: list[Path] = []
            try:
                # Exclusive-create, NEVER replace preexisting Raw or manifest.
                # A crash mid-publication yields REVIEW until manually repaired.
                for src, dest in pending:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with src.open('rb') as read_from, dest.open('xb') as write_to:
                        written.append(dest)
                        shutil.copyfileobj(read_from, write_to, length=1024 * 1024)
                if audit_item(root=root, item=item, config=config).status != 'complete':
                    raise OSError('published_artifact_failed_audit')
            except (OSError, ValueError):
                # Roll back only paths exclusively created by this invocation.
                for dest in reversed(written):
                    dest.unlink(missing_ok=True)
                return LiveResult('failed', calls, bytes_received, summary.pages, 'publish_failed')
            return LiveResult('published', calls, bytes_received, summary.pages,
                              'staged_and_verified')
    finally:
        lock.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='OpenAQ-only one-day guarded live backfill.')
    parser.add_argument('--region', required=True)
    parser.add_argument('--sensor-id', required=True, type=int)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--max-http-requests', type=int, default=3)
    parser.add_argument('--max-bytes', type=int, default=2 * 1024 * 1024)
    parser.add_argument('--approve-live', action='store_true',
                        help='EXPLICITLY authorize live OpenAQ API access for one day only')
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        config = validate_config(config_path)
        items = plan_backfill(source='openaq', region_id=args.region,
                              start=args.start, end=args.end,
                              sensor_ids=(args.sensor_id,), max_plan_items=1)
        if args.approve_live:
            load_project_env(ROOT)
        result = execute_one_openaq_day(root=ROOT, item=items[0], config=config,
                                        approve_live=args.approve_live,
                                        api_key=os.getenv('OPENAQ_API_KEY') if args.approve_live else None,
                                        max_http_requests=args.max_http_requests,
                                        max_bytes=args.max_bytes)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        # Do not interpolate exception (provider tokens/URLs may be embedded).
        print('OPENAQ GUARDED BACKFILL ERROR: invalid plan, configuration, or filesystem state', file=sys.stderr)
        return 2
    print(f'OPENAQ GUARDED BACKFILL: {result.state.upper()}')
    print(f'REQUESTS: {result.requests}/{args.max_http_requests}; BYTES: {result.downloaded_bytes}/{args.max_bytes}')
    print(f'PAGES: {result.pages}; REASON: {result.reason}')
    if not args.approve_live:
        print('DRY RUN ONLY — no API requests or Raw writes.')
    elif result.state == 'published':
        print('One OpenAQ day published after stage validation; verify with offline Audit.')
    return 0 if result.state in ('skipped', 'approval_required', 'published') else 2


if __name__ == '__main__':
    raise SystemExit(main())
