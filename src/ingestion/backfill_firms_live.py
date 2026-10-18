"""Guarded single-window NASA FIRMS ingestion using the existing CSV adapter.

Offline unless explicitly approved. For one <=5-day SP/NRT window, stage and
validate a complete provider response, then exclusively publish Raw+manifest.
This tool never rewrites existing provider artifacts and never prints API keys.

A local lock protects only this runner, not the legacy CLI or other computers.
Run only one writer per data root; crash-during-publish may require review.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

from src.common.validation import validate_config
from .backfill import FIRMS_PRODUCTS, ROOT, PlanItem, plan_backfill
from .backfill_audit import audit_item
from .backfill_gate import evaluate_gate
from .dotenv import load_project_env
from .firms import _NoRedirect, _paths, ingest_firms, valid_bbox
from .openaq import IngestionError

MAX_RUN_BYTES_HARD = 16 * 1024 * 1024


@dataclass(frozen=True)
class FIRMSLiveResult:
    state: str  # skipped | approval_required | blocked_review | failed | published
    requests: int
    downloaded_bytes: int
    chunks: int
    reason: str


def execute_one_firms_window(
    *, root: Path, item: PlanItem, config: dict, approve_live: bool,
    api_key: str | None = None, max_bytes: int = 2 * 1024 * 1024,
    transport: Callable[[str, float], bytes] | None = None,
) -> FIRMSLiveResult:
    """One bounded HTTP attempt, zero retries, <=5 days, with staging.

    Injected transport can exercise the real FIRMS adapter without network.
    The same NOT_FOUND audit is checked after the lock and before publishing.
    """
    if type(approve_live) is not bool:
        raise ValueError('approve_live must be boolean')
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_RUN_BYTES_HARD:
        raise ValueError('max_bytes must be 1..16777216')
    if (item.source != 'firms' or item.product not in FIRMS_PRODUCTS
            or item.sensor_id is not None):
        raise ValueError('FIRMS requires SP/NRT product and no sensor ID')
    # Only one EXACT <=5-day planner item is accepted (not a batch).
    planned = plan_backfill(source='firms', region_id=item.region_id,
                            start=item.start, end=item.end,
                            product=item.product, max_plan_items=1)
    if planned != (item,):
        raise ValueError('FIRMS guard supports one <=5-day window only')
    if item.region_id not in config['regions']:
        raise ValueError('unknown region ID')
    bbox = valid_bbox(config['regions'][item.region_id])
    decisions = evaluate_gate(root=root, items=planned, config=config, max_candidates=1)
    if decisions.state == 'nothing_to_do':
        return FIRMSLiveResult('skipped', 0, 0, 0, 'already_complete')
    if decisions.state != 'ready_for_manual_review':
        return FIRMSLiveResult('blocked_review', 0, 0, 0, decisions.decisions[0].reason)
    if not approve_live:
        return FIRMSLiveResult('approval_required', 0, 0, 0, 'dry_run_only')
    if not api_key:
        raise ValueError('FIRMS_MAP_KEY is missing')

    source_root = root / 'data/raw/nasa_firms'
    locks = source_root / '_locks'
    locks.mkdir(parents=True, exist_ok=True)
    # Request identity includes product, region, bbox, dates and mode.
    first = date.fromisoformat(item.start)
    days = (date.fromisoformat(item.end) - first).days
    _, _, key = _paths(root, item.product, item.region_id, first, days, bbox, 'live')
    lock = locks / f'{key}.lock'
    try:
        with lock.open('x', encoding='ascii') as stream:
            stream.write('local_exclusive_lock\n')
    except FileExistsError:
        return FIRMSLiveResult('blocked_review', 0, 0, 0, 'active_or_stale_lock')

    requests = 0
    downloaded = 0
    try:
        if audit_item(root=root, item=item, config=config).status != 'not_found':
            return FIRMSLiveResult('blocked_review', 0, 0, 0, 'candidate_changed_after_lock')

        def bounded_transport(url: str, timeout: float) -> bytes:
            nonlocal requests, downloaded
            if requests >= 1:
                raise IngestionError('request_budget_exceeded')
            requests += 1
            if transport is None:
                request = urllib.request.Request(
                    url, headers={'Accept': 'text/csv', 'User-Agent': 'EnvironmentalEventBigData/0.1'},
                    method='GET')
                opener = urllib.request.build_opener(_NoRedirect)
                with opener.open(request, timeout=timeout) as response:
                    if response.status != 200:
                        raise IngestionError(f'http_{response.status}')
                    data = response.read(max_bytes + 1)
            else:
                data = transport(url, timeout)
            if type(data) is not bytes:
                raise IngestionError('invalid_http_payload')
            downloaded += len(data)
            if downloaded > max_bytes:
                raise IngestionError('byte_budget_exceeded')
            return data

        staging = source_root / '_staging'
        staging.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='firms-', dir=staging) as tmp:
            stage_root = Path(tmp)
            try:
                summary = ingest_firms(
                    root=stage_root, region_id=item.region_id, bbox=config['regions'][item.region_id],
                    product=item.product, start=item.start, end=item.end, api_key=api_key,
                    chunk_days=5, max_retries=0, timeout=30.0, overwrite=False,
                    transport=bounded_transport,
                )
            except (OSError, IngestionError, TypeError, ValueError):
                return FIRMSLiveResult('failed', requests, downloaded, 0, 'staged_ingestion_error')
            if not summary.ok or summary.chunks != 1:
                return FIRMSLiveResult('failed', requests, downloaded, summary.chunks,
                                       'staged_ingestion_failed')
            if audit_item(root=stage_root, item=item, config=config).status != 'complete':
                return FIRMSLiveResult('failed', requests, downloaded, summary.chunks,
                                       'staged_audit_not_complete')
            if audit_item(root=root, item=item, config=config).status != 'not_found':
                return FIRMSLiveResult('blocked_review', requests, downloaded, summary.chunks,
                                       'candidate_changed_before_publish')

            src_raw, src_manifest, _ = _paths(stage_root, item.product, item.region_id,
                                               first, days, bbox, 'live')
            dst_raw, dst_manifest, _ = _paths(root, item.product, item.region_id,
                                               first, days, bbox, 'live')
            if not src_raw.is_file() or not src_manifest.is_file():
                return FIRMSLiveResult('failed', requests, downloaded, summary.chunks,
                                       'incomplete_stage')
            if dst_raw.exists() or dst_manifest.exists():
                return FIRMSLiveResult('blocked_review', requests, downloaded, summary.chunks,
                                       'destination_not_empty')
            written: list[Path] = []
            try:
                for src, dest in ((src_raw, dst_raw), (src_manifest, dst_manifest)):
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with src.open('rb') as reader, dest.open('xb') as writer:
                        written.append(dest)
                        shutil.copyfileobj(reader, writer, length=1024 * 1024)
                if audit_item(root=root, item=item, config=config).status != 'complete':
                    raise OSError('published_artifact_failed_audit')
            except (OSError, ValueError):
                for path in reversed(written):
                    path.unlink(missing_ok=True)
                return FIRMSLiveResult('failed', requests, downloaded, summary.chunks,
                                       'publish_failed')
            return FIRMSLiveResult('published', requests, downloaded, summary.chunks,
                                   'staged_and_verified')
    finally:
        lock.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Single-window guarded NASA FIRMS backfill.')
    parser.add_argument('--region', required=True)
    parser.add_argument('--product', choices=FIRMS_PRODUCTS, required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--max-bytes', type=int, default=2 * 1024 * 1024)
    parser.add_argument('--approve-live', action='store_true',
                        help='Explicitly authorize ONE live FIRMS API attempt (<=5 UTC days)')
    args = parser.parse_args(argv)
    try:
        path = Path(args.config)
        if not path.is_absolute():
            path = ROOT / path
        config = validate_config(path)
        item, = plan_backfill(source='firms', region_id=args.region,
                              product=args.product, start=args.start, end=args.end,
                              max_plan_items=1)
        if args.approve_live:
            load_project_env(ROOT)
        result = execute_one_firms_window(
            root=ROOT, item=item, config=config, approve_live=args.approve_live,
            api_key=os.getenv('FIRMS_MAP_KEY') if args.approve_live else None,
            max_bytes=args.max_bytes,
        )
    except (OSError, KeyError, TypeError, ValueError):
        print('FIRMS GUARDED BACKFILL ERROR: invalid plan, configuration, or filesystem state',
              file=sys.stderr)
        return 2
    print(f'FIRMS GUARDED BACKFILL: {result.state.upper()}')
    print(f'REQUESTS: {result.requests}/1; BYTES: {result.downloaded_bytes}/{args.max_bytes}')
    print(f'CHUNKS: {result.chunks}; REASON: {result.reason}')
    if not args.approve_live:
        print('DRY RUN ONLY — no API requests or Raw writes.')
    elif result.state == 'published':
        print('One FIRMS window published after stage validation; verify with offline Audit.')
    return 0 if result.state in ('skipped', 'approval_required', 'published') else 2


if __name__ == '__main__':
    raise SystemExit(main())
