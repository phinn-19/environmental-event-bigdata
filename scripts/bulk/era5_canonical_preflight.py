#!/usr/bin/env python3
"""Read-only ERA5 monthly staging => lake/v1 compatibility / overlap preflight.

Not a publisher. No cloud writes, no CDS API, no data modifications.
This checks metadata of the canonical ERA5 namespace, and reports monthly
window overlaps. It does NOT re-verify GRIB SHA or approve schema migration.
"""
from __future__ import annotations

import argparse
import calendar
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Callable

BUCKET = 'CourseworkBigData/env-event-lake'
PREFIX = 'lake/v1/data/raw/era5/'
MANIFEST_PREFIX = PREFIX + 'manifests/'
STAGING_PREFIX = 'experiments/era5_monthly_vn_bbox_v1/'
MONTHS = ('2023-08', '2023-09', '2023-10', '2023-11')
MAX_OBJECTS = 1500
MAX_MANIFESTS = 400
MAX_MANIFEST_SIZE = 65536


def month_window(month: str) -> tuple[datetime, datetime]:
    y, m = map(int, month.split('-'))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    end = datetime(ny, nm, 1, tzinfo=timezone.utc)
    return start, end


def parse_utc(text: str) -> datetime:
    if not isinstance(text, str):
        raise ValueError('invalid UTC date-time')
    obj = datetime.fromisoformat(text.replace('Z', '+00:00'))
    if obj.tzinfo is None or obj.utcoffset().total_seconds() != 0:
        raise ValueError('non-UTC interval')
    return obj


def intervals_overlap(a: tuple[datetime, datetime], b: tuple[datetime, datetime]) -> bool:
    return a[0] < b[1] and b[0] < a[1]


def classify_manifests(manifests: list[tuple[str, dict]], months: tuple[str, ...]) -> dict:
    conflicts = []
    checked = 0
    for key, m in manifests:
        if m.get('source') != 'era5':
            raise ValueError('unexpected source in ERA5 manifest: ' + key)
        if m.get('source_product') not in (None, 'reanalysis-era5-single-levels'):
            raise ValueError('unexpected ERA5 product in manifest: ' + key)
        start = parse_utc(m.get('start_utc'))
        end = parse_utc(m.get('end_utc'))
        if start >= end:
            raise ValueError('invalid manifest interval: ' + key)
        checked += 1
        # Any conflict with same region is a blocker; monthly and daily files
        # cannot be published side by side without an explicit dedupe policy.
        if m.get('region_id') == 'VN_BBOX':
            for month in months:
                if intervals_overlap((start, end), month_window(month)):
                    conflicts.append({'month': month, 'existing_manifest': key,
                                      'existing_start': start.isoformat(),
                                      'existing_end': end.isoformat()})
    return {'canonical_era5_manifests_checked': checked, 'overlaps': conflicts}


def _safe_relative(path: str) -> bool:
    return (isinstance(path, str) and path.startswith('data/raw/era5/')
            and '\\' not in path and ':' not in path
            and all(x not in ('', '.', '..') for x in path.split('/')))


def run_remote(*, bucket: str, token: str, get_info: Callable,
               list_tree: Callable, download: Callable,
               months: tuple[str, ...]) -> dict:
    if bucket != BUCKET or not token.strip():
        raise ValueError('bucket/token guard')
    info = get_info(bucket, token=token)
    if info.private is not True:
        raise ValueError('HF Storage Bucket is not private')
    entries = list(list_tree(bucket, prefix=PREFIX, recursive=True, token=token))
    file_nodes = [x for x in entries if getattr(x, 'type', None) == 'file']
    if len(file_nodes) > MAX_OBJECTS:
        raise ValueError('too many ERA5 canonical objects to review')
    keys = [x.path for x in file_nodes]
    if len(set(keys)) != len(keys):
        raise ValueError('duplicate canonical names')
    if any(not key.startswith(PREFIX) for key in keys):
        raise ValueError('out of scope canonical object')
    manifests = [x for x in file_nodes if x.path.startswith(MANIFEST_PREFIX) and x.path.endswith('.manifest.json')]
    if len(manifests) > MAX_MANIFESTS:
        raise ValueError('too many canonical manifests')
    if any(type(x.size) is not int or x.size <= 0 or x.size > MAX_MANIFEST_SIZE for x in manifests):
        raise ValueError('bad/oversize canonical manifest')
    docs = []
    with tempfile.TemporaryDirectory(prefix='era5-canonical-preflight-') as t:
        for i in range(0, len(manifests), 20):
            batch = manifests[i:i + 20]
            targets = [(x.path, str(Path(t) / f'manifest-{i+j}.json')) for j, x in enumerate(batch)]
            if targets:
                download(bucket, files=targets, token=token, raise_on_missing_files=True)
            for (remote, local), node in zip(targets, batch):
                p = Path(local)
                if not p.is_file() or p.stat().st_size != node.size:
                    raise ValueError('remote canonical manifest size mismatch')
                doc = json.loads(p.read_text(encoding='utf-8'))
                if not isinstance(doc, dict) or not _safe_relative(doc.get('file')):
                    raise ValueError('unsafe canonical manifest')
                raw_key = 'lake/v1/' + doc['file']
                if raw_key not in keys:
                    raise ValueError('canonical manifest references missing Raw')
                docs.append((remote, doc))
    assessment = classify_manifests(docs, months)
    # Catch Raw objects in selected month folders even if their manifests are
    # absent or broken. A missing manifest must never be treated as no overlap.
    raw_hits = sorted(k for k in keys
                      if not k.endswith('.manifest.json')
                      and any(f'/VN_BBOX/year={m[:4]}/month={m[5:7]}/' in k
                              for m in months))
    assessment['raw_objects_in_target_month_dirs'] = raw_hits
    assessment.update({'mode': 'CANONICAL_READ_ONLY_PREFLIGHT', 'bucket': bucket,
                       'months': list(months), 'canonical_era5_objects': len(file_nodes),
                       'scope': 'ERA5 VN_BBOX overlap detection only',
                       'hf_writes': 0, 'cds_calls': 0, 'local_raw_writes': 0,
                       'schema_policy': 'BLOCKED_PENDING_MANIFEST_V1_CONTRACT_REVIEW',
                       'staging_sha': 'VERIFIED_IN_SEPARATE_GITHUB_RUN_NOT_RECHECKED_HERE'})
    return assessment


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('plan', 'check'))
    p.add_argument('--approve-hf-read', action='store_true')
    p.add_argument('--report', default='')
    args = p.parse_args()
    try:
        if args.mode == 'plan':
            print(json.dumps({'mode': 'OFFLINE_PLAN', 'months': list(MONTHS),
                              'windows_utc': {m: [v.isoformat() for v in month_window(m)] for m in MONTHS},
                              'hf_reads': 0, 'hf_writes': 0, 'cds_calls': 0}, indent=2))
            return 0
        if not args.approve_hf_read:
            raise ValueError('explicit HF read approval required')
        from huggingface_hub import bucket_info, list_bucket_tree, download_bucket_files
        result = run_remote(bucket=BUCKET, token=os.environ.get('HF_READ_TOKEN', ''),
                            get_info=bucket_info, list_tree=list_bucket_tree,
                            download=download_bucket_files, months=MONTHS)
        if args.report:
            path = Path(args.report)
            if path.exists():
                raise ValueError('refuse overwrite report')
            path.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, indent=2))
        if result['overlaps'] or result['raw_objects_in_target_month_dirs']:
            print(f"CANONICAL_OVERLAP_HOLD manifest_conflicts={len(result['overlaps'])} raw_path_conflicts={len(result['raw_objects_in_target_month_dirs'])}")
            return 3
        print(f"CANONICAL_OVERLAP_CLEAR months={len(MONTHS)}; NO WRITES; MANIFEST CONTRACT REVIEW STILL REQUIRED")
        return 0
    except Exception as exc:
        # Provider exceptions could include private data/secrets. Never echo.
        print(f'STOP: {type(exc).__name__}; no cloud writes', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
