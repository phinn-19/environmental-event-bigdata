"""Guarded 1..7-day OpenAQ cloud backfill using reviewed V1 Raw/Manifest pipeline.

No source or cloud mutation in plan/preview. Live requires explicit approvals,
provider-specific license approval, and existing repo source modules. No changes
are made to any existing daily ingestion workflow.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Mapping

BUCKET = 'CourseworkBigData/env-event-lake'
PREFIX = 'lake/v1'
SENSOR_ID = 13502154  # Reviewed HANOI PM2.5 pilot sensor from daily_live_tick.py
REGION = 'HANOI'


def plan_days(start: str, days: int, *, today: date | None = None) -> tuple[date, ...]:
    if type(days) is not int or not 1 <= days <= 7:
        raise ValueError('days must be 1..7')
    try:
        first = date.fromisoformat(start)
    except (TypeError, ValueError) as exc:
        raise ValueError('invalid start UTC date') from exc
    if first.isoformat() != start:
        raise ValueError('noncanonical start UTC date')
    last_allowed = (today or datetime.now(timezone.utc).date()) - timedelta(days=2)
    if first < date(2023, 1, 1) or first + timedelta(days=days - 1) > last_allowed:
        raise ValueError('outside reviewed historical range or too close to present')
    return tuple(first + timedelta(days=i) for i in range(days))


def run(*, start: str, days: int, mode: str, approve_api: bool,
        approve_cloud: bool, env: Mapping[str, str] | None = None,
        runner: Callable[..., object] = subprocess.run,
        today: date | None = None) -> int:
    if mode not in ('plan', 'preview', 'live'):
        raise ValueError('unsupported mode')
    target_days = plan_days(start, days, today=today)
    if mode != 'live' and (approve_api or approve_cloud):
        raise ValueError('approval flags require live mode')
    if mode == 'live':
        if not (approve_api and approve_cloud):
            raise ValueError('both live approvals are required')
        env = env if env is not None else os.environ
        if env.get('OPENAQ_REDISTRIBUTION_APPROVED') != 'true':
            raise ValueError('provider-specific license clearance is absent')
        for secret in ('HF_READ_TOKEN', 'HF_WRITE_TOKEN', 'OPENAQ_API_KEY'):
            if not env.get(secret):
                raise ValueError('required credential is absent')
    else:
        env = env if env is not None else os.environ
    for day in target_days:
        print(f'{mode.upper()}: source=openaq sensor={SENSOR_ID} UTC_day={day}', flush=True)
        if mode == 'plan':
            continue
        as_of = (day + timedelta(days=2)).isoformat()
        common = (
            sys.executable, '-m', 'src.ingestion.daily_cloud_workflow',
            '--source', 'openaq', '--region', REGION,
            '--sensor-id', str(SENSOR_ID),
            '--as-of', as_of, '--lag-days', '2', '--lookback-days', '1',
            '--max-candidates', '1', '--max-http-requests', '3',
            '--max-bytes', '2097152', '--max-cloud-bytes', '4194304',
            '--bucket', BUCKET, '--prefix', PREFIX,
        )
        if mode == 'preview':
            # Offline source plan; do not pass credentials to preview process.
            clean = {k: v for k, v in env.items() if k not in (
                'HF_TOKEN', 'HF_READ_TOKEN', 'HF_WRITE_TOKEN', 'OPENAQ_API_KEY',
                'FIRMS_MAP_KEY', 'CDSAPI_KEY', 'CDSAPI_URL')}
            runner(common, check=True, env=clean)
            continue
        # Restore exact remote day first, so reruns will skip existing audited
        # Raw/Manifest instead of repeating API requests.
        restore = (
            sys.executable, '-m', 'src.ingestion.cloud_restore',
            '--bucket', BUCKET, '--prefix', PREFIX,
            '--scope-source', 'openaq', '--scope-day', day.isoformat(),
            '--approve-download', '--max-bytes', '4194304',
        )
        clean = {k: v for k, v in env.items() if k not in (
            'HF_TOKEN', 'HF_READ_TOKEN', 'HF_WRITE_TOKEN', 'OPENAQ_API_KEY',
            'FIRMS_MAP_KEY', 'CDSAPI_KEY', 'CDSAPI_URL')}
        read_env = dict(clean, HF_TOKEN=env['HF_READ_TOKEN'])
        runner(restore, check=True, env=read_env)
        live_env = dict(clean, HF_TOKEN=env['HF_WRITE_TOKEN'], OPENAQ_API_KEY=env['OPENAQ_API_KEY'])
        runner(common + ('--approve-live', '--approve-upload'), check=True, env=live_env)
    print(f'{mode.upper()} COMPLETE: {len(target_days)} UTC dates; V1 checks authoritative', flush=True)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True)
    parser.add_argument('--days', type=int, default=3)
    parser.add_argument('--mode', choices=('plan', 'preview', 'live'), default='plan')
    parser.add_argument('--approve-api', action='store_true')
    parser.add_argument('--approve-cloud', action='store_true')
    args = parser.parse_args(argv)
    try:
        return run(start=args.start, days=args.days, mode=args.mode,
                   approve_api=args.approve_api, approve_cloud=args.approve_cloud)
    except (ValueError, OSError, subprocess.CalledProcessError):
        print('GUARDED OPENAQ BACKFILL STOP: invalid scope, gate or child process', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
