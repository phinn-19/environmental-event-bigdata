"""Read-only incremental ingestion preview for one UTC scheduling tick.

This is an audit/gate preview, NOT a scheduler or automatic executor.
Provider data availability must be set externally through --lag-days;
never assume that yesterday's observations are published for every source.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

from src.common.validation import validate_config
from .backfill import FIRMS_PRODUCTS, ROOT, SOURCES, PlanItem, _date, plan_backfill
from .backfill_gate import evaluate_gate


MAX_LOOKBACK_DAYS = 31
MAX_LAG_DAYS = 365


def plan_daily(
    *, source: str, region_id: str, as_of: str, lag_days: int,
    lookback_days: int = 3, sensor_ids: tuple[int, ...] = (),
    product: str | None = None, max_plan_items: int = 31,
) -> tuple[PlanItem, ...]:
    """Plan stable one-day UTC requests for a bounded, lagged rolling window.

    E.g. as_of=2026-10-20, lag_days=2, lookback_days=3 yields
    [2026-10-16, 2026-10-19); latest covered day is 2026-10-18.

    FIRMS uses one-day requests to avoid a moving 5-day chunk identity.
    If older multi-day FIRMS manifests overlap, the existing auditor must
    classify them REVIEW; this function never authorizes a duplicate fetch.
    """
    if type(lag_days) is not int or not 1 <= lag_days <= MAX_LAG_DAYS:
        raise ValueError('lag_days must be 1..365 UTC days')
    if type(lookback_days) is not int or not 1 <= lookback_days <= MAX_LOOKBACK_DAYS:
        raise ValueError('lookback_days must be 1..31')
    if type(max_plan_items) is not int or not 1 <= max_plan_items <= 100:
        raise ValueError('max_plan_items must be 1..100')
    try:
        after = _date(as_of) - timedelta(days=lag_days - 1)
        first = after - timedelta(days=lookback_days)
    except OverflowError as exc:
        raise ValueError('dates exceed supported range') from exc

    if source != 'firms':
        return plan_backfill(
            source=source, region_id=region_id,
            start=first.isoformat(), end=after.isoformat(),
            sensor_ids=sensor_ids, product=product,
            max_plan_items=max_plan_items,
        )

    if sensor_ids or product not in FIRMS_PRODUCTS:
        raise ValueError('FIRMS requires a supported --product and no sensors')
    if lookback_days > max_plan_items:
        raise ValueError('daily FIRMS requests exceed max_plan_items')
    items = []
    for i in range(lookback_days):
        day = first + timedelta(days=i)
        (item,) = plan_backfill(
            source='firms', region_id=region_id, product=product,
            start=day.isoformat(),
            end=(day + timedelta(days=1)).isoformat(),
            max_plan_items=1,
        )
        items.append(item)
    return tuple(items)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='OFFLINE daily incremental preview: no scheduler, APIs, or Raw writes.'
    )
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--as-of', required=True, help='UTC calendar date YYYY-MM-DD')
    parser.add_argument('--lag-days', type=int, required=True,
                        help='Conservative delay from as-of (1=yesterday is latest eligible)')
    parser.add_argument('--lookback-days', type=int, default=3)
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids')
    parser.add_argument('--product', choices=FIRMS_PRODUCTS)
    parser.add_argument('--max-plan-items', type=int, default=31)
    parser.add_argument('--max-candidates', type=int, default=3)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--format', choices=('text', 'json'), default='text')
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        config = validate_config(config_path)
        if args.region not in config['regions']:
            raise ValueError('unknown region')
        sensor_ids = tuple(args.sensor_ids or ())
        if args.source == 'openaq' and not sensor_ids:
            sensor_ids = tuple(config['ingestion']['openaq']['pilot_sensor_ids'])
        items = plan_daily(
            source=args.source, region_id=args.region, as_of=args.as_of,
            lag_days=args.lag_days, lookback_days=args.lookback_days,
            sensor_ids=sensor_ids, product=args.product,
            max_plan_items=args.max_plan_items,
        )
        result = evaluate_gate(
            root=ROOT, items=items, config=config,
            max_candidates=args.max_candidates,
        )
    except (ValueError, TypeError, KeyError, OSError) as exc:
        print(f'DAILY PLAN ERROR: {exc}', file=sys.stderr)
        return 2
    if args.format == 'json':
        print(json.dumps({
            'mode': 'offline_incremental_preview',
            'download_authorized': False, 'no_raw_writes': True,
            'as_of_utc': args.as_of, 'lag_days': args.lag_days,
            'lookback_days': args.lookback_days,
            'state': result.state, 'candidate_count': result.candidate_count,
            'max_candidates': result.max_candidates,
            'decisions': [asdict(d) for d in result.decisions],
        }, indent=2))
    else:
        print('DAILY INCREMENTAL PREVIEW — OFFLINE ONLY')
        print(f'Source={args.source}; region={args.region}; '
              f'as_of_utc={args.as_of}; lag_days={args.lag_days}; '
              f'lookback_days={args.lookback_days}')
        for i, decision in enumerate(result.decisions, 1):
            item = decision.item
            extra = (f' sensor={item.sensor_id}' if item.sensor_id is not None else
                     f' product={item.product}' if item.product else '')
            print(f'{i:03d}  {item.start} -> {item.end} '
                  f'{decision.action.upper()} {decision.reason}{extra}')
        print(f'STATE: {result.state.upper()}')
        print(f'CANDIDATES: {result.candidate_count}/{result.max_candidates}')
        print('NO LIVE REQUESTS AUTHORIZED. NO NETWORK OR RAW WRITES.')
    return 2 if result.state in ('blocked_review', 'blocked_budget') else 0


if __name__ == '__main__':
    raise SystemExit(main())
