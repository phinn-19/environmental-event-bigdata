#!/usr/bin/env python3
"""Publish NASA FIRMS locally audited CSV+audit pairs to PRIVATE HF bulk/v1.

Not the V1 daily Raw/Manifest format. No provider requests; consumes existing
export/ after the exact historical shard audit. Immutable keys; SHA readback.
"""
from __future__ import annotations
import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import tempfile

from firms_vn_guarded import SOURCE_CHOICES, audit_one, parse_date, paths, plan, read_json

BUCKET = 'CourseworkBigData/env-event-lake'
PREFIX = 'bulk/v1/data/raw/nasa_firms/viirs_sp_historical'
MAX_BATCH_BYTES = 512 * 1024 * 1024
MAX_BUCKET_BYTES = 20 * 1024 ** 3


@dataclass(frozen=True)
class Entry:
    local: Path
    remote: str
    size: int
    sha256: str
    is_audit: bool


def digest(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    n = 0
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            n += len(block)
            h.update(block)
    return n, h.hexdigest()


def build_entries(*, export: Path) -> tuple[Entry, ...]:
    summary = json.loads((export / 'summary.json').read_text(encoding='utf-8'))
    if summary.get('verified') is not True or summary.get('source_system') != 'NASA FIRMS Area API':
        raise ValueError('summary not a verified NASA historical shard')
    first, last = parse_date(summary['start']), parse_date(summary['end'])
    root = export / 'firms_vn_viirs_sp'
    tasks = plan(first, last, list(SOURCE_CHOICES))
    objects = summary.get('objects')
    if not isinstance(objects, list) or len(objects) != len(tasks):
        raise ValueError('summary item count mismatch')
    entries = []
    total = rows = 0
    for task, obj in zip(tasks, objects):
        if (obj.get('source'), obj.get('start'), obj.get('days')) != (
                task['source'], task['start'], task['days']):
            raise ValueError('summary order/scope mismatch')
        raw, audit = paths(root, task)
        ok, reason = audit_one(raw, audit, task)
        if not ok:
            raise ValueError('FIRMS offline audit failed: ' + str(reason))
        info = read_json(audit)
        raw_size, raw_hash = digest(raw)
        if (raw_size != obj.get('size_bytes') or raw_size != info.get('size_bytes') or
                raw_hash != obj.get('sha256') or raw_hash != info.get('sha256') or
                obj.get('rows') != info.get('row_count')):
            raise ValueError('source audit/summary SHA or rows mismatch')
        audit_size, audit_hash = digest(audit)
        total += raw_size + audit_size
        rows += obj['rows']
        if total > MAX_BATCH_BYTES:
            raise ValueError('batch bytes safety limit exceeded')
        remote_dir = f'{PREFIX}/{task["source"]}/{parse_date(task["start"]).year}'
        entries.extend((
            Entry(raw, f'{remote_dir}/{raw.name}', raw_size, raw_hash, False),
            Entry(audit, f'{remote_dir}/{audit.name}', audit_size, audit_hash, True),
        ))
    if total < summary.get('bytes', -1) or rows != summary.get('rows') or len(tasks) != summary.get('files'):
        raise ValueError('summary aggregate mismatch')
    remotes = [e.remote for e in entries]
    if len(set(remotes)) != len(remotes):
        raise ValueError('duplicate cloud object names')
    return tuple(entries)


def publish(*, entries: tuple[Entry, ...], bucket: str = BUCKET,
            bucket_info_fn=None, list_tree_fn=None, batch_fn=None, download_fn=None) -> tuple[int, int]:
    if not entries:
        raise ValueError('no audited entries')
    if any(not e.remote.startswith(PREFIX + '/') for e in entries):
        raise ValueError('unreviewed HF destination')
    if None in (bucket_info_fn, list_tree_fn, batch_fn, download_fn):
        from huggingface_hub import bucket_info, list_bucket_tree, batch_bucket_files, download_bucket_files
        bucket_info_fn = bucket_info
        list_tree_fn = list_bucket_tree
        batch_fn = batch_bucket_files
        download_fn = download_bucket_files
    info = bucket_info_fn(bucket)
    if info.private is not True:
        raise RuntimeError('bucket must be private')
    existing = {o.path: o for o in list_tree_fn(bucket, prefix='bulk/v1/', recursive=True)
                if getattr(o, 'type', 'file') == 'file'}
    additions = sum(e.size for e in entries if e.remote not in existing)
    if int(info.size) + additions > MAX_BUCKET_BYTES:
        raise RuntimeError('private bucket storage limit exceeded')
    for i in range(0, len(entries), 2):
        raw, audit = entries[i:i+2]
        if raw.is_audit or not audit.is_audit:
            raise ValueError('invalid raw/audit order')
        if audit.remote in existing and raw.remote not in existing:
            raise RuntimeError('remote audit without raw - review required')
    with tempfile.TemporaryDirectory(prefix='firms-hf-audit-') as tmp:
        def verify(items: list[Entry]):
            for start in range(0, len(items), 12):
                chunk = items[start:start + 12]
                pairs = [(e.remote, str(Path(tmp) / f'obj-{start+i:06d}'))
                         for i, e in enumerate(chunk)]
                download_fn(bucket, files=pairs)
                for e, (_, target) in zip(chunk, pairs):
                    if digest(Path(target)) != (e.size, e.sha256):
                        raise RuntimeError('HF cloud SHA readback mismatch; STOP')
        reused = [e for e in entries if e.remote in existing]
        verify(reused)
        news = [e for e in entries if e.remote not in existing]
        for phase in ([e for e in news if not e.is_audit],
                      [e for e in news if e.is_audit]):
            for i in range(0, len(phase), 8):
                chunk = phase[i:i+8]
                batch_fn(bucket, add=[(str(e.local), e.remote) for e in chunk])
                verify(chunk)
    return len(news), len(reused)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', type=Path, default=Path('export'))
    parser.add_argument('--approve-upload', action='store_true')
    args = parser.parse_args()
    try:
        entries = build_entries(export=args.export)
        print(f'NASA FIRMS HF preflight: audited_objects={len(entries)} bytes={sum(e.size for e in entries)}', flush=True)
        if not args.approve_upload:
            print('PREVIEW ONLY; no HF calls')
            return 0
        new, reused = publish(entries=entries)
        print(f'NASA FIRMS PRIVATE HF BULK VERIFIED: new={new} reused={reused}', flush=True)
        return 0
    except (ValueError, OSError, RuntimeError, KeyError, TypeError):
        print('NASA FIRMS HF PUBLISH STOP: invalid audit/private bucket/cloud verification', flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
