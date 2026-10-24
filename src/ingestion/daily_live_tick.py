"""Bounded one-day cloud restore -> preview -> optionally acquire+publish.

This command is not a scheduler. Existing ingestion guards, Data Contract,
manifest verification, and the cloud publisher remain authoritative.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Mapping

BUCKET = 'CourseworkBigData/env-event-lake'
PREFIX = 'lake/v1'

@dataclass(frozen=True)
class Source:
    name: str
    region: str
    lag: int
    max_bytes: int
    max_cloud_bytes: int
    max_restore_bytes: int
    sensor_id: int | None = None
    product: str | None = None

# Explicit, reviewed pilot scope. Changing scope requires code review.
SOURCES = {
    'openaq': Source('openaq', 'HANOI', 2, 2097152, 4194304, 4194304,
                     sensor_id=13502154),
    'firms': Source('firms', 'VN_BBOX', 2, 2097152, 4194304, 16777216,
                    product='VIIRS_NOAA20_NRT'),
    'era5': Source('era5', 'HANOI', 7, 8388608, 16777216, 4194304),
}


def source_names(selected: str) -> tuple[str, ...]:
    if selected == 'all':
        return tuple(SOURCES)
    if selected not in SOURCES:
        raise ValueError('unsupported source')
    return (selected,)


def require_credentials(mode: str, selected: str, env: Mapping[str, str]) -> None:
    if mode == 'offline':
        return
    if not env.get('HF_READ_TOKEN'):
        raise ValueError('read token required')
    if mode != 'live':
        return
    if not env.get('HF_WRITE_TOKEN'):
        raise ValueError('write token required')
    needed = {
        'openaq': 'OPENAQ_API_KEY',
        'firms': 'FIRMS_MAP_KEY',
        'era5': 'CDSAPI_KEY',
    }
    for name in source_names(selected):
        if not env.get(needed[name]):
            raise ValueError('source credential required')


def commands_for(name: str, as_of: date, mode: str) -> tuple[tuple[str, ...], ...]:
    source = SOURCES[name]
    day = as_of.toordinal() - source.lag
    target = date.fromordinal(day).isoformat()
    common = (
        sys.executable, '-m', 'src.ingestion.daily_cloud_workflow',
        '--source', source.name, '--region', source.region,
        '--as-of', as_of.isoformat(), '--lag-days', str(source.lag),
        '--lookback-days', '1', '--max-candidates', '1',
        '--max-http-requests', '3', '--max-bytes', str(source.max_bytes),
        '--max-cloud-bytes', str(source.max_cloud_bytes),
        '--bucket', BUCKET, '--prefix', PREFIX,
    )
    if source.sensor_id is not None:
        common += ('--sensor-id', str(source.sensor_id))
    if source.product is not None:
        common += ('--product', source.product)
    if mode == 'offline':
        return (common,)
    restore = (
        sys.executable, '-m', 'src.ingestion.cloud_restore',
        '--bucket', BUCKET, '--prefix', PREFIX,
        '--scope-source', source.name, '--scope-day', target,
        '--max-bytes', str(source.max_restore_bytes), '--approve-download',
    )
    if mode == 'preview':
        return restore, common
    if mode == 'live':
        return restore, common, common + ('--approve-live', '--approve-upload')
    raise ValueError('invalid mode')


def run_tick(
    *, selected: str, mode: str, approve_api: bool, approve_cloud: bool,
    now: date | None = None, env: Mapping[str, str] | None = None,
    runner: Callable[..., object] = subprocess.run,
) -> None:
    if mode not in ('offline', 'preview', 'live'):
        raise ValueError('invalid mode')
    if type(approve_api) is not bool or type(approve_cloud) is not bool:
        raise ValueError('invalid approvals')
    if mode == 'live' and not (approve_api and approve_cloud):
        raise ValueError('both explicit approvals required')
    if mode != 'live' and (approve_api or approve_cloud):
        raise ValueError('approvals are only valid for live mode')
    if env is None:
        env = os.environ
    names = source_names(selected)
    require_credentials(mode, selected, env)
    today = now or datetime.now(timezone.utc).date()
    if type(today) is not date:
        raise ValueError('UTC date required')
    # The runner is fresh every time; never read local time or hardcode a day.
    for name in names:
        target = date.fromordinal(today.toordinal() - SOURCES[name].lag)
        print(f'SOURCE={name} UTC_AS_OF={today} TARGET={target} MODE={mode}', flush=True)
        for command in commands_for(name, today, mode):
            stage = ('restore' if 'src.ingestion.cloud_restore' in command
                     else 'live' if '--approve-live' in command else 'preview')
            print(f'RUN {name}: {stage}', flush=True)
            child_env = dict(env)
            # Least privilege per subprocess: do not pass write/provider
            # credentials to restores or offline previews.
            sensitive = ('HF_TOKEN', 'OPENAQ_API_KEY', 'FIRMS_MAP_KEY',
                         'CDSAPI_KEY', 'CDSAPI_URL', 'HF_READ_TOKEN', 'HF_WRITE_TOKEN')
            for key in sensitive:
                child_env.pop(key, None)
            if stage == 'restore':
                child_env['HF_TOKEN'] = env['HF_READ_TOKEN']
            elif stage == 'live':
                child_env['HF_TOKEN'] = env['HF_WRITE_TOKEN']
                provider = {'openaq': 'OPENAQ_API_KEY',
                            'firms': 'FIRMS_MAP_KEY', 'era5': 'CDSAPI_KEY'}[name]
                child_env[provider] = env[provider]
                if name == 'era5':
                    child_env['CDSAPI_URL'] = env.get(
                        'CDSAPI_URL', 'https://cds.climate.copernicus.eu/api')
            runner(command, check=True, env=child_env)
    print('DAILY MULTISOURCE TICK: PASS', flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=('all', *SOURCES), required=True)
    parser.add_argument('--mode', choices=('offline', 'preview', 'live'), default='offline')
    parser.add_argument('--approve-api', action='store_true')
    parser.add_argument('--approve-cloud', action='store_true')
    args = parser.parse_args(argv)
    try:
        run_tick(selected=args.source, mode=args.mode,
                 approve_api=args.approve_api, approve_cloud=args.approve_cloud)
    except (ValueError, OSError, subprocess.CalledProcessError):
        # Don't surface provider exceptions, URLs, credentials or headers.
        print('DAILY MULTISOURCE TICK: STOP (guard or subprocess)', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
