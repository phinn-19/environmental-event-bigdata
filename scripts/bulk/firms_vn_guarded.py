#!/usr/bin/env python3
"""Guarded NASA FIRMS VIIRS SP CSV acquisition, local only. No HF/publish.

Python 3.12 standard library only. Designed for the FIRMS area CSV API.
BBOX intentionally includes neighboring countries; not a Vietnam boundary clip.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SOURCE_CHOICES = ('VIIRS_SNPP_SP', 'VIIRS_NOAA20_SP', 'VIIRS_NOAA21_SP')
BBOX = '102,8,110,24'  # API: west,south,east,north
BASE = 'https://firms.modaps.eosdis.nasa.gov'
REQUIRED = {'latitude', 'longitude', 'acq_date', 'acq_time', 'frp', 'satellite'}
USER_AGENT = 'environmental-event-bigdata-coursework/1.0 (data ingestion; local staging)'
MAX_RESPONSE_BYTES = 48 * 1024 * 1024


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def read_json(p: Path):
    return json.loads(p.read_text(encoding='utf-8'))


def write_new(p: Path, b: bytes):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('xb') as f:
        f.write(b)
        f.flush()
        os.fsync(f.fileno())


def parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def plan(start: dt.date, end: dt.date, sources: list[str]):
    if start > end or (end - start).days > 366 * 3:
        raise ValueError('Invalid dates or >3 years; split into smaller acquisition plans')
    out = []
    for source in sources:
        cur = start
        while cur <= end:
            count = min(5, (end - cur).days + 1)
            out.append({'source': source, 'start': cur.isoformat(), 'days': count,
                        'end': (cur + dt.timedelta(days=count - 1)).isoformat()})
            cur += dt.timedelta(days=count)
    return out


def req(url: str, *, secret: str) -> bytes:
    # URL contains MAP_KEY. Never print raw URL or raw exception text.
    request = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'text/csv'})
    try:
        with urlopen(request, timeout=75) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise RuntimeError('FIRMS reply exceeds 48 MiB safety cap')
            return body
    except HTTPError as e:
        raise RuntimeError(f'FIRMS HTTP {e.code}; stop and check API availability/quota') from None
    except URLError as e:
        raise RuntimeError(f'FIRMS network failure: {type(e.reason).__name__}; stop, no retry') from None


def validate_csv(raw: bytes, source: str, start: dt.date, end: dt.date):
    if not raw or b'\x00' in raw or raw[:50].lstrip().lower().startswith((b'<html', b'<!doctype')):
        raise ValueError('Not a valid CSV payload (empty/HTML/NUL)')
    try:
        decoded = raw.decode('utf-8-sig')
    except UnicodeDecodeError:
        raise ValueError('CSV is not UTF-8') from None
    reader = csv.DictReader(io.StringIO(decoded))
    names = set(reader.fieldnames or [])
    if not REQUIRED.issubset(names):
        raise ValueError(f'Unexpected CSV schema; missing {sorted(REQUIRED - names)}')
    records = 0
    dates = set()
    for r in reader:
        try:
            day = parse_date(r['acq_date'])
            lat, lon = float(r['latitude']), float(r['longitude'])
            float(r['frp'])
        except (ValueError, TypeError, KeyError):
            raise ValueError(f'Bad date/coordinate/FRP at CSV data row {records+1}') from None
        if not (start <= day <= end):
            raise ValueError('API payload contains out-of-window detection')
        if not (8 <= lat <= 24 and 102 <= lon <= 110):
            raise ValueError('API payload contains detection outside requested BBOX')
        if not r['satellite']:
            raise ValueError('Missing satellite attribute')
        records += 1
        dates.add(day.isoformat())
    return {'row_count': records, 'observation_dates': len(dates), 'columns': reader.fieldnames}


def paths(root: Path, task):
    rel = Path(task['source']) / task['start'][:4] / f"{task['start']}_{task['days']}d"
    return root / (str(rel) + '.csv'), root / (str(rel) + '.audit.json')


def audit_one(csv_path: Path, meta_path: Path, task):
    if not csv_path.is_file() or not meta_path.is_file():
        return False, 'missing CSV or audit JSON'
    try:
        meta = read_json(meta_path)
        raw = csv_path.read_bytes()
        if meta['source'] != task['source'] or meta['start'] != task['start'] or meta['days'] != task['days']:
            return False, 'metadata/plan mismatch'
        if meta['sha256'] != sha256_bytes(raw) or meta['size_bytes'] != len(raw):
            return False, 'SHA/size mismatch'
        info = validate_csv(raw, task['source'], parse_date(task['start']), parse_date(task['end']))
        if meta['row_count'] != info['row_count']:
            return False, 'row count mismatch'
        return True, f"PASS bytes={len(raw)} rows={info['row_count']}"
    except (ValueError, KeyError, OSError, json.JSONDecodeError) as exc:
        return False, f'{type(exc).__name__}: {exc}'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name in ('plan', 'availability', 'fetch', 'audit'):
        p = sub.add_parser(name)
        p.add_argument('--start', default='2024-11-13')
        p.add_argument('--end', default='2025-06-30')
        p.add_argument('--sources', nargs='+', choices=SOURCE_CHOICES, default=list(SOURCE_CHOICES))
        p.add_argument('--root', type=Path, default=Path('data/bulk_staging/firms_vn_viirs_sp'))
        if name == 'fetch':
            p.add_argument('--max-new-requests', type=int, default=6)
            p.add_argument('--delay-seconds', type=float, default=2.0)
    a = ap.parse_args()
    tasks = plan(parse_date(a.start), parse_date(a.end), a.sources)
    if a.cmd == 'plan':
        print(json.dumps({'mode': 'OFFLINE READ-ONLY', 'bbox_wsen': BBOX,
                          'note': 'BBOX includes some neighboring countries; not Vietnam clipped',
                          'products': a.sources, 'requested_windows': len(tasks),
                          'first': tasks[:3], 'last': tasks[-3:]}, indent=2))
        return
    if a.cmd == 'audit':
        passed = missing = failed = total_bytes = rows = 0
        for t in tasks:
            csvp, metap = paths(a.root, t)
            if not csvp.exists() and not metap.exists():
                missing += 1
                continue
            ok, message = audit_one(csvp, metap, t)
            print(f"{t['source']} {t['start']}: {message}")
            if ok:
                passed += 1
                meta = read_json(metap)
                total_bytes += meta['size_bytes']
                rows += meta['row_count']
            else:
                failed += 1
        print(json.dumps({'mode': 'OFFLINE AUDIT', 'PASS': passed, 'MISSING': missing,
                          'FAIL': failed, 'verified_bytes': total_bytes, 'verified_rows': rows}, indent=2))
        if failed:
            sys.exit(2)
        return
    key = os.environ.get('FIRMS_MAP_KEY', '').strip()
    if not (8 <= len(key) <= 128) or any(c in key for c in '/?&# '):
        raise SystemExit('FIRMS_MAP_KEY absent/invalid. Set environment variable; never paste key into chat.')
    if a.cmd == 'availability':
        url = f'{BASE}/api/data_availability/csv/{key}/ALL'
        raw = req(url, secret=key)
        s = raw.decode('utf-8-sig', errors='replace')
        # Only output relevant sensor names and min/max dates; do not expose key
        reader = csv.DictReader(io.StringIO(s))
        if not {'data_id', 'min_date', 'max_date'}.issubset(reader.fieldnames or []):
            raise RuntimeError('Unexpected FIRMS availability schema')
        print(json.dumps({'mode': 'REMOTE READ-ONLY; 1 API request, no file writes',
                          'availability': [r for r in reader if r['data_id'] in a.sources]}, indent=2))
        return
    if a.max_new_requests < 1 or a.max_new_requests > 12:
        raise SystemExit('--max-new-requests must be 1..12')
    if a.delay_seconds < 1:
        raise SystemExit('--delay-seconds must be >= 1')
    done = 0
    for t in tasks:
        csvp, metap = paths(a.root, t)
        if csvp.exists() or metap.exists():
            ok, msg = audit_one(csvp, metap, t)
            if not ok:
                raise RuntimeError(f'Existing incomplete/corrupt data at {csvp}: {msg}; NO OVERWRITE')
            print(f"SKIP VERIFIED {t['source']} {t['start']}")
            continue
        if done >= a.max_new_requests:
            break
        url = f"{BASE}/api/area/csv/{key}/{t['source']}/{BBOX}/{t['days']}/{t['start']}"
        print(f"API FETCH {t['source']} {t['start']} {t['days']} days (key hidden)", flush=True)
        raw = req(url, secret=key)
        info = validate_csv(raw, t['source'], parse_date(t['start']), parse_date(t['end']))
        metadata = {'source_system': 'NASA FIRMS area API', 'dataset': t['source'],
                    'source': t['source'], 'start': t['start'], 'days': t['days'],
                    'end': t['end'], 'bbox_wsen': BBOX, 'area_scope': 'BBOX, not Vietnam polygon',
                    'source_url_template': f"{BASE}/api/area/csv/<REDACTED>/{t['source']}/{BBOX}/{t['days']}/{t['start']}",
                    'retrieved_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                    'sha256': sha256_bytes(raw), 'size_bytes': len(raw), **info,
                    'license_review': 'REQUIRED_BEFORE_CLOUD_PUBLISH'}
        write_new(csvp, raw)
        write_new(metap, (json.dumps(metadata, indent=2) + '\n').encode('utf-8'))
        done += 1
        print(f"STAGED {t['source']} {t['start']} bytes={len(raw)} rows={info['row_count']} SHA={metadata['sha256']}")
        if done < a.max_new_requests:
            time.sleep(a.delay_seconds)
    print(f'FETCH COMPLETE: new_files={done}; no HF uploads; run audit before reporting')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'STOP ({type(e).__name__}): {e}', file=sys.stderr)
        sys.exit(2)
