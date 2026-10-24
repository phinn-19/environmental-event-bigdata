"""Explicit, bounded Daily Ingestion -> verified Hugging Face Raw publication.

One manual invocation is NOT an unattended schedule. Preview is always offline.
Existing COMPLETE Raw may be published without repeating provider requests.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from src.common.validation import validate_config
from .backfill import ROOT, FIRMS_PRODUCTS, SOURCES
from .backfill_audit import audit_item
from .backfill_gate import evaluate_gate
from .cloud_publish import (
    DEFAULT_PREFIX, MAX_TOTAL_BYTES, Entry, publish, verified_entries,
)
from .daily_execute import DailyExecutionResult, execute_daily
from .daily_plan import plan_daily

DEFAULT_BUCKET = 'CourseworkBigData/env-event-lake'
MAX_CLOUD_OBJECTS = 64
MAX_CLOUD_BYTES_HARD = 128 * 1024 * 1024


@dataclass(frozen=True)
class WorkflowResult:
    state: str
    reason: str
    candidates: int
    complete: int
    objects: int = 0
    bytes_ready: int = 0
    acquired: int = 0
    cloud_new: int = 0
    cloud_reused: int = 0


def _valid_destination(bucket: str, prefix: str) -> bool:
    return (isinstance(bucket, str) and
            re.fullmatch(r'[^/\s]+/[^/\s]+', bucket) is not None and
            isinstance(prefix, str) and
            re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/-]*', prefix) is not None and
            all(s not in ('', '.', '..') for s in prefix.split('/')))


def _prepare(*, items, root: Path, source: str, region: str, config: dict,
             prefix: str, max_cloud_bytes: int,
             entry_builder: Callable[..., tuple[Entry, ...]]) -> tuple[Entry, ...]:
    entries: list[Entry] = []
    for item in items:
        entries.extend(entry_builder(
            root=root, source=source, region=region,
            start=item.start, end=item.end, sensor_id=item.sensor_id,
            product=item.product, config=config, prefix=prefix,
            max_bytes=max_cloud_bytes,
        ))
        if len(entries) > MAX_CLOUD_OBJECTS:
            raise ValueError('cloud object budget exceeded')
        if sum(e.size for e in entries) > max_cloud_bytes:
            raise ValueError('cloud byte budget exceeded')
    if len({e.remote for e in entries}) != len(entries):
        raise ValueError('duplicate cloud keys')
    return tuple(entries)


def run_workflow(
    *, root: Path, config: dict, source: str, region: str,
    as_of: str, lag_days: int, lookback_days: int = 1,
    sensor_ids: tuple[int, ...] = (), product: str | None = None,
    max_candidates: int = 1, max_bytes: int | None = None,
    max_http_requests: int = 3, max_cloud_bytes: int = MAX_TOTAL_BYTES,
    bucket: str = DEFAULT_BUCKET, prefix: str = DEFAULT_PREFIX,
    approve_live: bool = False, approve_upload: bool = False,
    auditor: Callable = audit_item,
    executor: Callable[..., DailyExecutionResult] = execute_daily,
    entry_builder: Callable[..., tuple[Entry, ...]] = verified_entries,
    uploader: Callable[..., tuple[int, int]] = publish,
) -> WorkflowResult:
    """Fail closed if ANY plan item is HOLD, exceeds budget, or fails audit.

    Approval is not persisted. Never download a source without also approving
    cloud publication; cloud-only retry is allowed for all-COMPLETE windows.
    """
    if (type(approve_live) is not bool or type(approve_upload) is not bool or
            type(max_candidates) is not int or not 1 <= max_candidates <= 3 or
            type(max_http_requests) is not int or not 1 <= max_http_requests <= 10 or
            type(max_cloud_bytes) is not int or not 1 <= max_cloud_bytes <= MAX_CLOUD_BYTES_HARD or
            (max_bytes is not None and
             (type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024))):
        raise ValueError('invalid approval or execution/upload budgets')
    if source not in SOURCES or region not in config['regions']:
        raise ValueError('unsupported source or region')
    if not _valid_destination(bucket, prefix):
        raise ValueError('invalid bucket/prefix')
    if source == 'openaq' and not sensor_ids:
        raise ValueError('OpenAQ sensor IDs must be explicit')

    items = plan_daily(
        source=source, region_id=region, as_of=as_of,
        lag_days=lag_days, lookback_days=lookback_days,
        sensor_ids=sensor_ids, product=product, max_plan_items=31,
    )
    initial = evaluate_gate(
        root=root, items=items, config=config,
        max_candidates=max_candidates, auditor=auditor,
    )
    count_complete = sum(d.action == 'skip' for d in initial.decisions)
    if initial.state in ('blocked_review', 'blocked_budget'):
        return WorkflowResult(initial.state, 'whole_batch_gate_blocked',
                              initial.candidate_count, count_complete)

    # Check COMPLETE items locally in preview too, without any cloud/API calls.
    ready = _prepare(
        items=[d.item for d in initial.decisions if d.action == 'skip'],
        root=root, source=source, region=region, config=config,
        prefix=prefix, max_cloud_bytes=max_cloud_bytes,
        entry_builder=entry_builder,
    )
    if not approve_live and not approve_upload:
        return WorkflowResult('preview', 'offline_only', initial.candidate_count,
                              count_complete, len(ready), sum(e.size for e in ready))
    if approve_live != approve_upload and (approve_live or initial.candidate_count):
        return WorkflowResult('approval_required', 'live_requires_both_approvals',
                              initial.candidate_count, count_complete,
                              len(ready), sum(e.size for e in ready))
    if not approve_upload:
        return WorkflowResult('approval_required', 'upload_approval_missing',
                              initial.candidate_count, count_complete,
                              len(ready), sum(e.size for e in ready))

    acquired = 0
    if initial.candidate_count:
        # Existing guarded executor performs a second gate under its local lock,
        # and audits each completed acquisition. It may block/partially publish.
        result = executor(
            root=root, config=config, source=source, region_id=region,
            as_of=as_of, lag_days=lag_days, lookback_days=lookback_days,
            sensor_ids=sensor_ids, product=product,
            approve_live=True, max_candidates=max_candidates,
            max_bytes=max_bytes, max_http_requests=max_http_requests,
        )
        acquired = result.published
        if result.state not in ('published', 'nothing_to_do'):
            return WorkflowResult('failed', f'daily_executor_{result.state}',
                                  initial.candidate_count, count_complete,
                                  acquired=acquired)

    # Re-audit ALL planned days, not only newly acquired ones. Allows cloud-only
    # retries, but refuses to publish if an item has since become incomplete.
    final = evaluate_gate(
        root=root, items=items, config=config,
        max_candidates=max_candidates, auditor=auditor,
    )
    if final.state != 'nothing_to_do':
        return WorkflowResult('blocked_review', 'post_execute_plan_not_complete',
                              final.candidate_count,
                              sum(d.action == 'skip' for d in final.decisions),
                              acquired=acquired)
    ready = _prepare(
        items=items, root=root, source=source, region=region,
        config=config, prefix=prefix, max_cloud_bytes=max_cloud_bytes,
        entry_builder=entry_builder,
    )
    if not ready:
        return WorkflowResult('failed', 'no_verified_cloud_objects', 0,
                              len(items), acquired=acquired)
    # The Sprint 09 publisher does private-bucket check, existing-object
    # readback SHA-256, Raw-before-Manifest writes, and post-upload readback.
    new, reused = uploader(bucket, ready)
    return WorkflowResult('published', 'all_local_and_cloud_objects_verified',
                          0, len(items), len(ready), sum(e.size for e in ready),
                          acquired, new, reused)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--as-of', required=True, help='UTC date YYYY-MM-DD')
    parser.add_argument('--lag-days', type=int, required=True)
    parser.add_argument('--lookback-days', type=int, default=1)
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids')
    parser.add_argument('--product', choices=FIRMS_PRODUCTS)
    parser.add_argument('--max-candidates', type=int, default=1)
    parser.add_argument('--max-bytes', type=int, help='Per-source-request acquisition byte cap')
    parser.add_argument('--max-http-requests', type=int, default=3)
    parser.add_argument('--max-cloud-bytes', type=int, default=MAX_TOTAL_BYTES)
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    parser.add_argument('--prefix', default=DEFAULT_PREFIX)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--approve-live', action='store_true',
                        help='Explicitly authorize bounded provider API calls')
    parser.add_argument('--approve-upload', action='store_true',
                        help='Explicitly authorize verified HF cloud publication')
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        config = validate_config(config_path)
        result = run_workflow(
            root=ROOT, config=config, source=args.source, region=args.region,
            as_of=args.as_of, lag_days=args.lag_days,
            lookback_days=args.lookback_days,
            sensor_ids=tuple(args.sensor_ids or ()), product=args.product,
            max_candidates=args.max_candidates, max_bytes=args.max_bytes,
            max_http_requests=args.max_http_requests,
            max_cloud_bytes=args.max_cloud_bytes,
            bucket=args.bucket, prefix=args.prefix,
            approve_live=args.approve_live, approve_upload=args.approve_upload,
        )
    except (ValueError, KeyError, TypeError, OSError) as exc:
        # No provider exception/body, filesystem path, or credential leak.
        print(f'DAILY CLOUD WORKFLOW: STOP ({type(exc).__name__})', file=sys.stderr)
        return 2
    except Exception as exc:
        print(f'DAILY CLOUD WORKFLOW: STOP ({type(exc).__name__})', file=sys.stderr)
        return 2
    print(f'DAILY CLOUD WORKFLOW: {result.state.upper()} ({result.reason})')
    print(f'LOCAL COMPLETE: {result.complete}; PENDING: {result.candidates}; '
          f'ACQUIRED THIS RUN: {result.acquired}')
    print(f'CLOUD OBJECTS READY: {result.objects}; BYTES: {result.bytes_ready}')
    if result.state == 'published':
        print(f'CLOUD VERIFIED: new={result.cloud_new}, reused_verified={result.cloud_reused}')
    if result.state == 'preview':
        print('DRY RUN ONLY — NO PROVIDER/CLOUD CALLS, SECRET READS OR RAW WRITES')
    if result.state not in ('preview', 'published'):
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
