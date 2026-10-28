#!/usr/bin/env python3
"""Guarded local import of a downloaded FIRMS GH Action artifact.

Default is READ ONLY. --apply copies only new validated files using exclusive writes.
Never overwrites existing data; detects partial target pairs and hash conflicts.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from firms_vn_guarded import SOURCE_CHOICES, audit_one, parse_date, plan, paths


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--artifact', type=Path, required=True, help='Extracted artifact directory with summary.json')
    p.add_argument('--target', type=Path, default=Path('data/bulk_staging/firms_vn_viirs_sp'))
    p.add_argument('--apply', action='store_true', help='Copy new verified pairs; default dry run')
    a = p.parse_args()
    root = a.artifact.resolve()
    summary = json.loads((root / 'summary.json').read_text(encoding='utf-8'))
    if summary.get('verified') is not True:
        raise RuntimeError('Unverified artifact summary')
    src = root / 'firms_vn_viirs_sp'
    if not src.is_dir():
        raise RuntimeError('Missing firms_vn_viirs_sp directory in artifact')
    tasks = plan(parse_date(summary['start']), parse_date(summary['end']), list(SOURCE_CHOICES))
    if len(tasks) != summary['files']:
        raise RuntimeError('Summary file count mismatch')
    if set(src.rglob('*.csv')) != {paths(src, t)[0] for t in tasks}:
        raise RuntimeError('Unexpected or missing CSVs in artifact')
    if set(src.rglob('*.audit.json')) != {paths(src, t)[1] for t in tasks}:
        raise RuntimeError('Unexpected or missing audit JSONs in artifact')
    count_bytes = count_rows = 0
    new = []
    for t in tasks:
        data, meta = paths(src, t)
        if data.is_symlink() or meta.is_symlink():
            raise RuntimeError('Symlink not allowed in artifact')
        ok, reason = audit_one(data, meta, t)
        if not ok:
            raise RuntimeError(f'Artifact failed audit: {data}: {reason}')
        m = json.loads(meta.read_text(encoding='utf-8'))
        count_bytes += m['size_bytes']
        count_rows += m['row_count']
        dst_data, dst_meta = paths(a.target, t)
        if dst_data.exists() != dst_meta.exists():
            raise RuntimeError(f'Incomplete target pair; stop: {dst_data}')
        if dst_data.exists():
            ok, reason = audit_one(dst_data, dst_meta, t)
            if not ok or dst_data.read_bytes() != data.read_bytes():
                raise RuntimeError(f'Existing target differs; NO OVERWRITE: {dst_data}: {reason}')
        else:
            new.append((data, meta, dst_data, dst_meta, t))
    if (count_bytes, count_rows) != (summary['bytes'], summary['rows']):
        raise RuntimeError('Summary totals mismatch')
    print(f'ARTIFACT VERIFIED: files={len(tasks)} rows={count_rows} bytes={count_bytes}')
    print(f'NEW FILE PAIRS={len(new)}; EXISTING VERIFIED={len(tasks)-len(new)}')
    if not a.apply:
        print('DRY RUN ONLY; re-run with --apply to copy new pairs')
        return
    for data, meta, dst_data, dst_meta, task in new:
        dst_data.parent.mkdir(parents=True, exist_ok=True)
        with dst_data.open('xb') as f:
            f.write(data.read_bytes())
        with dst_meta.open('xb') as f:
            f.write(meta.read_bytes())
        ok, reason = audit_one(dst_data, dst_meta, task)
        if not ok:
            raise RuntimeError(f'Post-copy verification FAILED: {dst_data}: {reason}')
    print(f'IMPORTED {len(new)} VERIFIED PAIRS; 0 files overwritten')


if __name__ == '__main__':
    main()
