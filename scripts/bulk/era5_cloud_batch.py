#!/usr/bin/env python3
"""Bounded ERA5 VN_BBOX daily raw GRIB acquisition into existing verified HF V1.

3 *one-day* requests per scheduled run, NOT an unsafe 30-day CDS request.
Each day restores the exact cloud Raw/Manifest before any network acquisition.
Schedule is a finite calendar campaign; absent runs require manual replay.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import os
import subprocess
import sys
from typing import Callable, Mapping

BUCKET = 'CourseworkBigData/env-event-lake'
PREFIX = 'lake/v1'
START = date(2024, 11, 13)
FIRST_TICK = date(2026, 10, 30)
LAST_ALLOWED_LAG_DAYS = 7
MAX_DAYS = 3


def scheduled_days(*, today: date, first: date | None = None,
                   days: int = 3) -> tuple[date, ...]:
    if type(days) is not int or not 1 <= days <= MAX_DAYS:
        raise ValueError('ERA5 batch must have 1..3 days')
    if first is None:
        shift = (today - FIRST_TICK).days
        if shift < 0:
            return ()
        first = START + timedelta(days=shift * MAX_DAYS)
    if first < START or first > today - timedelta(days=LAST_ALLOWED_LAG_DAYS):
        return ()
    end = min(first + timedelta(days=days), today - timedelta(days=LAST_ALLOWED_LAG_DAYS - 1))
    return tuple(first + timedelta(days=i) for i in range((end - first).days))


def execute(*, today: date, first: date | None, days: int, mode: str,
            approve_api: bool, approve_cloud: bool,
            env: Mapping[str, str] | None = None,
            runner: Callable[..., object] = subprocess.run) -> int:
    if mode not in ('preview', 'live'):
        raise ValueError('unsupported mode')
    targets = scheduled_days(today=today, first=first, days=days)
    if mode == 'live':
        if not (approve_api and approve_cloud):
            raise ValueError('ERA5 API and cloud approvals required')
        env = env if env is not None else os.environ
        if any(not env.get(s) for s in ('HF_READ_TOKEN', 'HF_WRITE_TOKEN', 'CDSAPI_KEY')):
            raise ValueError('missing credentials')
    elif approve_api or approve_cloud:
        raise ValueError('approvals require live')
    else:
        env = env if env is not None else os.environ
    safe_env = {k: v for k, v in env.items() if k not in (
        'HF_TOKEN', 'HF_READ_TOKEN', 'HF_WRITE_TOKEN', 'CDSAPI_KEY',
        'FIRMS_MAP_KEY', 'OPENAQ_API_KEY', 'CDSAPI_URL')}
    for day in targets:
        print(f'ERA5 {mode.upper()} VN_BBOX UTC={day}', flush=True)
        common = (
            sys.executable, '-m', 'src.ingestion.daily_cloud_workflow',
            '--source', 'era5', '--region', 'VN_BBOX',
            '--as-of', (day + timedelta(days=2)).isoformat(),
            '--lag-days', '2', '--lookback-days', '1', '--max-candidates', '1',
            '--max-http-requests', '1', '--max-bytes', '8388608',
            '--max-cloud-bytes', '16777216', '--bucket', BUCKET, '--prefix', PREFIX,
        )
        if mode == 'preview':
            runner(common, check=True, env=safe_env)
            continue
        restore = (
            sys.executable, '-m', 'src.ingestion.cloud_restore',
            '--bucket', BUCKET, '--prefix', PREFIX,
            '--scope-source', 'era5', '--scope-day', day.isoformat(),
            '--max-bytes', '16777216', '--approve-download',
        )
        runner(restore, check=True,
               env=dict(safe_env, HF_TOKEN=env['HF_READ_TOKEN']))
        runner(common + ('--approve-live', '--approve-upload'), check=True,
               env=dict(safe_env, HF_TOKEN=env['HF_WRITE_TOKEN'],
                        CDSAPI_KEY=env['CDSAPI_KEY'],
                        CDSAPI_URL='https://cds.climate.copernicus.eu/api'))
    print(f'ERA5 {mode.upper()} COMPLETE: {len(targets)} dates; source audit and HF readback are authoritative',
          flush=True)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('preview', 'live'), default='preview')
    parser.add_argument('--start', help='optional manual UTC first day YYYY-MM-DD')
    parser.add_argument('--days', type=int, default=3)
    parser.add_argument('--approve-api', action='store_true')
    parser.add_argument('--approve-cloud', action='store_true')
    args = parser.parse_args(argv)
    try:
        first = date.fromisoformat(args.start) if args.start else None
        return execute(today=datetime.now(timezone.utc).date(), first=first,
                       days=args.days, mode=args.mode,
                       approve_api=args.approve_api, approve_cloud=args.approve_cloud)
    except (ValueError, OSError, subprocess.CalledProcessError):
        print('ERA5 BULK STOP: guard, CDS or audited HF write failed (check job logs)', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
