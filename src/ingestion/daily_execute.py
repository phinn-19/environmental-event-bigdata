"""Opt-in, bounded daily ingestion coordinator for existing guarded runners.

NOT a scheduler and NEVER calls Processing or Dashboard. Dry-run is the default.
A whole-batch HOLD or candidate-budget violation fails closed before I/O.
The single-writer lock coordinates only this CLI on one filesystem; it cannot
protect against the legacy CLI or writers on other machines.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.common.validation import validate_config
from .backfill import ROOT, FIRMS_PRODUCTS, SOURCES, PlanItem
from .backfill_audit import AuditResult, audit_item
from .backfill_gate import evaluate_gate
from .daily_plan import plan_daily
from .dotenv import load_project_env


MAX_EXECUTE_CANDIDATES = 3


@dataclass(frozen=True)
class DailyExecutionResult:
    state: str
    candidates: int
    published: int
    skipped: int
    reason: str
    completed: tuple[str, ...] = ()


def _guarded_dispatch(*, root: Path, item: PlanItem, config: dict,
                      max_bytes: int, max_http_requests: int) -> str:
    """Dispatch ONE request via the previously reviewed stage/publish runners."""
    if item.source == 'openaq':
        from .backfill_live import execute_one_openaq_day
        key = os.environ.get('OPENAQ_API_KEY')
        result = execute_one_openaq_day(
            root=root, item=item, config=config, approve_live=True,
            api_key=key, max_http_requests=max_http_requests,
            max_bytes=max_bytes,
        )
    elif item.source == 'firms':
        from .backfill_firms_live import execute_one_firms_window
        key = os.environ.get('FIRMS_MAP_KEY')
        result = execute_one_firms_window(
            root=root, item=item, config=config, approve_live=True,
            api_key=key, max_bytes=max_bytes,
        )
    elif item.source == 'era5':
        from .backfill_era5_live import execute_one_era5_day
        result = execute_one_era5_day(
            root=root, item=item, config=config, approve_live=True,
            max_bytes=max_bytes,
        )
    else:
        raise ValueError('unsupported source')
    return result.state


def execute_daily(
    *, root: Path, config: dict, source: str, region_id: str,
    as_of: str, lag_days: int, lookback_days: int = 3,
    sensor_ids: tuple[int, ...] = (), product: str | None = None,
    approve_live: bool = False, max_candidates: int = 2,
    max_bytes: int | None = None, max_http_requests: int = 3,
    executor: Callable[..., str] = _guarded_dispatch,
    auditor: Callable[..., AuditResult] = audit_item,
) -> DailyExecutionResult:
    """Run at most `max_candidates` one-day acquisitions, sequentially.

    In dry run nothing is created. Approval is per invocation, not a durable
    schedule or permission for subsequent runs. Failures stop the batch.
    """
    if type(approve_live) is not bool:
        raise ValueError('approve_live must be bool')
    if type(max_candidates) is not int or not 1 <= max_candidates <= MAX_EXECUTE_CANDIDATES:
        raise ValueError('max_candidates must be 1..3')
    if type(max_http_requests) is not int or not 1 <= max_http_requests <= 10:
        raise ValueError('max_http_requests must be 1..10')
    if max_bytes is None:
        max_bytes = 8 * 1024 * 1024 if source == 'era5' else 2 * 1024 * 1024
    if type(max_bytes) is not int or not 1 <= max_bytes <= 16 * 1024 * 1024:
        raise ValueError('max_bytes must be 1..16777216')
    if source not in SOURCES or region_id not in config['regions']:
        raise ValueError('unknown source or region')
    if source == 'openaq' and not sensor_ids:
        raise ValueError('OpenAQ sensor IDs must be explicitly selected')
    items = plan_daily(
        source=source, region_id=region_id, as_of=as_of,
        lag_days=lag_days, lookback_days=lookback_days,
        sensor_ids=sensor_ids, product=product, max_plan_items=31,
    )
    initial = evaluate_gate(root=root, items=items, config=config,
                            max_candidates=max_candidates, auditor=auditor)
    skipped = sum(d.action == 'skip' for d in initial.decisions)
    if initial.state in ('blocked_review', 'blocked_budget'):
        return DailyExecutionResult(initial.state, initial.candidate_count, 0,
                                    skipped, 'whole_batch_gate_blocked')
    if initial.state == 'nothing_to_do':
        return DailyExecutionResult('nothing_to_do', 0, 0, skipped,
                                    'all_requests_complete')
    if not approve_live:
        return DailyExecutionResult('approval_required', initial.candidate_count,
                                    0, skipped, 'offline_preview_only')

    today = datetime.now(timezone.utc).date()
    if as_of > today.isoformat():
        raise ValueError('future as_of date is not permitted for live execution')

    # Do not load secrets or mutate filesystem until approved, fully gated.
    lock_dir = root / 'data' / 'raw' / '_locks'
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock = lock_dir / 'daily_ingestion.lock'
    try:
        with lock.open('x', encoding='ascii') as stream:
            stream.write('single_local_daily_writer\n')
    except FileExistsError:
        return DailyExecutionResult('blocked_review', initial.candidate_count,
                                    0, skipped, 'daily_writer_active_or_stale_lock')

    published: list[str] = []
    try:
        second = evaluate_gate(root=root, items=items, config=config,
                               max_candidates=max_candidates, auditor=auditor)
        if second != initial:
            return DailyExecutionResult('blocked_review', initial.candidate_count,
                                        0, skipped, 'plan_changed_after_lock')
        # .env read only happens after explicit approval and locking.
        if executor is _guarded_dispatch and source in ('openaq', 'firms'):
            load_project_env(root)
        for decision in second.decisions:
            if decision.action != 'candidate':
                continue
            item = decision.item
            # Re-audit even after the batch gate, just before each dispatch.
            current = auditor(root=root, item=item, config=config)
            if current.status != 'not_found':
                return DailyExecutionResult('blocked_review', initial.candidate_count,
                                            len(published), skipped,
                                            'candidate_changed_before_dispatch', tuple(published))
            try:
                state = executor(root=root, item=item, config=config,
                                 max_bytes=max_bytes, max_http_requests=max_http_requests)
            except Exception:
                # Do not reveal provider URLs, keys, or sensitive error text.
                return DailyExecutionResult('failed', initial.candidate_count,
                                            len(published), skipped,
                                            'guarded_executor_exception', tuple(published))
            if state != 'published':
                return DailyExecutionResult('failed', initial.candidate_count,
                                            len(published), skipped,
                                            f'guarded_executor_{state}', tuple(published))
            verified = auditor(root=root, item=item, config=config)
            if verified.status != 'complete':
                return DailyExecutionResult('failed', initial.candidate_count,
                                            len(published), skipped,
                                            'post_publish_audit_not_complete', tuple(published))
            published.append(f'{item.start}->{item.end}' +
                             (f' sensor={item.sensor_id}' if item.sensor_id else '') +
                             (f' product={item.product}' if item.product else ''))
        return DailyExecutionResult('published', initial.candidate_count,
                                    len(published), skipped, 'all_candidates_verified',
                                    tuple(published))
    finally:
        lock.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Bounded daily incremental executor. OFFLINE unless --approve-live.')
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--as-of', required=True, help='UTC date; never infer local time')
    parser.add_argument('--lag-days', type=int, required=True)
    parser.add_argument('--lookback-days', type=int, default=3)
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids')
    parser.add_argument('--product', choices=FIRMS_PRODUCTS)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--max-candidates', type=int, default=2)
    parser.add_argument('--max-bytes', type=int, help='Per request; ERA5 cap is POST-download')
    parser.add_argument('--max-http-requests', type=int, default=3,
                        help='Per OpenAQ day including failed attempts')
    parser.add_argument('--approve-live', action='store_true',
                        help='Explicitly allow at most max-candidates live requests')
    args = parser.parse_args(argv)
    try:
        cfg_path = Path(args.config)
        if not cfg_path.is_absolute():
            cfg_path = ROOT / cfg_path
        config = validate_config(cfg_path)
        result = execute_daily(
            root=ROOT, config=config, source=args.source, region_id=args.region,
            as_of=args.as_of, lag_days=args.lag_days,
            lookback_days=args.lookback_days,
            sensor_ids=tuple(args.sensor_ids or ()), product=args.product,
            approve_live=args.approve_live, max_candidates=args.max_candidates,
            max_bytes=args.max_bytes, max_http_requests=args.max_http_requests,
        )
    except Exception:
        print('DAILY EXECUTOR ERROR: invalid config, gate or runtime state', file=sys.stderr)
        return 2
    print(f'DAILY EXECUTOR: {result.state.upper()}')
    print(f'CANDIDATES: {result.candidates}; PUBLISHED: {result.published}; '
          f'SKIPPED: {result.skipped}; REASON: {result.reason}')
    for item in result.completed:
        print(f'VERIFIED: {item}')
    if not args.approve_live:
        print('DRY RUN ONLY — NO API REQUESTS, RAW WRITES OR SECRET READS')
    if result.state in ('blocked_review', 'blocked_budget', 'failed'):
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
