#!/usr/bin/env python3
"""Compute one deterministic, non-overlapping historical FIRMS shard (offline)."""
import datetime as dt
import json
import os
from pathlib import Path

BEGIN = dt.date(2023, 3, 1)
END = dt.date(2024, 11, 12)  # day before existing 2024-11-13 local dataset
SHARD_DAYS = 50  # a multiple of the FIRMS 5-day request cap
FIRST_SCHEDULE_UTC = dt.date(2026, 10, 29)
COUNT = ((END - BEGIN).days + 1 + SHARD_DAYS - 1) // SHARD_DAYS


def select_shard(event, manual_index, today_utc):
    if event == 'schedule':
        return (today_utc - FIRST_SCHEDULE_UTC).days
    return int(manual_index)


def bounds(index):
    if not 0 <= index < COUNT:
        raise ValueError(f'shard_index must be in 0..{COUNT - 1}')
    start = BEGIN + dt.timedelta(days=index * SHARD_DAYS)
    stop = min(END, start + dt.timedelta(days=SHARD_DAYS - 1))
    return start, stop


def main():
    event = os.environ.get('GITHUB_EVENT_NAME', 'workflow_dispatch')
    index = select_shard(event, os.environ.get('SHARD_INDEX', '0'), dt.datetime.now(dt.timezone.utc).date())
    active = 0 <= index < COUNT
    values = {'active': 'true' if active else 'false', 'index': str(index),
              'total_shards': str(COUNT)}
    if active:
        start, stop = bounds(index)
        values.update(start=start.isoformat(), end=stop.isoformat())
    else:
        values.update(start='', end='')
    print(json.dumps(values, ensure_ascii=False, indent=2))
    if event != 'schedule' and not active:
        raise SystemExit('Invalid manual shard_index; no API requests made')
    out = os.environ.get('GITHUB_OUTPUT')
    if out:
        with open(out, 'a', encoding='utf-8') as f:
            for k, v in values.items():
                f.write(f'{k}={v}\n')


if __name__ == '__main__':
    main()
