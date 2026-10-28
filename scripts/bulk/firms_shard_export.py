#!/usr/bin/env python3
"""Audit every planned file and export a self-contained verified GitHub artifact."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from firms_vn_guarded import SOURCE_CHOICES, audit_one, parse_date, paths, plan, read_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--shard', required=True, type=int)
    parser.add_argument('--root', type=Path, default=Path('data/bulk_staging/firms_vn_viirs_sp'))
    parser.add_argument('--export', type=Path, default=Path('export'))
    args = parser.parse_args()
    if args.export.exists():
        raise SystemExit('Export directory already exists; refusing overwrite')
    tasks = plan(parse_date(args.start), parse_date(args.end), list(SOURCE_CHOICES))
    total_bytes = total_rows = 0
    seen = []
    for task in tasks:
        csv_file, meta_file = paths(args.root, task)
        ok, message = audit_one(csv_file, meta_file, task)
        if not ok:
            raise RuntimeError(f'AUDIT FAILED {task["source"]} {task["start"]}: {message}')
        meta = read_json(meta_file)
        total_bytes += meta['size_bytes']
        total_rows += meta['row_count']
        seen.append({'source': task['source'], 'start': task['start'], 'days': task['days'],
                     'size_bytes': meta['size_bytes'], 'rows': meta['row_count'],
                     'sha256': meta['sha256']})
    args.export.mkdir(parents=True)
    shutil.copytree(args.root, args.export / 'firms_vn_viirs_sp')
    summary = {'verified': True, 'source_system': 'NASA FIRMS Area API',
               'scope': '102,8,110,24 (BBOX, NOT Vietnam polygon)',
               'start': args.start, 'end': args.end, 'shard': args.shard,
               'files': len(seen), 'rows': total_rows, 'bytes': total_bytes,
               'retrieved_at_utc': datetime.now(timezone.utc).isoformat(),
               'notes': 'GH Action temporary artifact; NOT Hugging Face and not permanent data lake',
               'objects': seen}
    (args.export / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
    print(f'FINAL SHARD AUDIT PASS: files={len(seen)} rows={total_rows} bytes={total_bytes}')


if __name__ == '__main__':
    main()
