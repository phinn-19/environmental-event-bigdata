"""Strict, OFFLINE manifest audit for Sprint 06 backfill plans.

This module NEVER downloads data or writes Raw. 'complete' means only that the
existing artifact matches the currently planned acquisition identity and
manifest; it does not certify scientific data quality or provider availability.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

from src.common.validation import validate_config
from .backfill import FIRMS_PRODUCTS, ROOT, SOURCES, PlanItem, plan_backfill
from .era5 import DATASET, _paths as era5_paths, validate_bbox, variables_for_request
from .firms import _paths as firms_paths, valid_bbox
from .openaq import _paths as openaq_paths
from .manifest import request_id


@dataclass(frozen=True)
class AuditResult:
    item: PlanItem
    status: str  # complete | not_found | review
    reason: str


def _utc(day: str) -> str:
    return f"{day}T00:00:00Z"


def _digest(path: Path) -> tuple[int, str]:
    size = 0
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _verified(root: Path, manifest_path: Path, expected_raw: Path, *,
              source: str, product: str, region: str, start: str,
              end: str, req_id: str, mode: str = 'live') -> dict | None:
    """Return validated manifest, or None. Exact file/identity/bytes must match."""
    try:
        data = json.loads(manifest_path.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            return None
        required = {
            'schema_version': 'manifest-v1', 'source': source,
            'source_product': product, 'acquisition_mode': mode,
            'region_id': region, 'start_utc': _utc(start), 'end_utc': _utc(end),
            'request_id': req_id,
        }
        if any(data.get(key) != value for key, value in required.items()):
            return None
        if data.get('status') not in ('success', 'empty'):
            return None
        n = data.get('row_count')
        if type(n) is not int or n < 0:
            return None
        if (data['status'] == 'empty') != (n == 0):
            return None
        expected_rel = expected_raw.relative_to(root).as_posix()
        if data.get('file') != expected_rel:
            return None
        if not expected_raw.is_file():
            return None
        length, digest = _digest(expected_raw)
        if data.get('bytes') != length or data.get('sha256') != digest:
            return None
        return data
    except (OSError, UnicodeError, ValueError, TypeError, KeyError):
        return None


def _related_manifest(root: Path, source: str, item: PlanItem) -> bool:
    """Conservative alert for existing overlapping requests (any acquisition mode)."""
    path = root / 'data/raw' / ('nasa_firms' if source == 'firms' else 'era5') / 'manifests'
    if not path.exists():
        return False
    for mf in path.rglob('*.manifest.json'):
        try:
            m = json.loads(mf.read_text(encoding='utf-8'))
            if (m.get('source') != ('nasa_firms' if source == 'firms' else 'era5')
                    or m.get('region_id') != item.region_id):
                continue
            if source == 'firms' and m.get('source_product') != item.product:
                continue
            if source == 'era5' and m.get('source_product') != DATASET:
                continue
            a, b = m.get('start_utc'), m.get('end_utc')
            if isinstance(a, str) and isinstance(b, str) and a < _utc(item.end) and b > _utc(item.start):
                return True
        except (OSError, UnicodeError, ValueError, TypeError):
            return True  # conservatively signal review for unreadable manifest
    return False


def audit_item(*, root: Path, item: PlanItem, config: dict,
               mode: str = 'live') -> AuditResult:
    """Audit one planned item. Never returns COMPLETE on checksum/identity uncertainty."""
    if mode != 'live':
        raise ValueError('v1 auditor supports live plans only')
    if item.region_id not in config['regions']:
        raise ValueError(f'unknown region ID: {item.region_id}')
    first, after = date.fromisoformat(item.start), date.fromisoformat(item.end)
    if not first < after:
        raise ValueError('bad plan window')

    if item.source == 'openaq':
        if item.sensor_id is None or after != first + timedelta(days=1):
            raise ValueError('OpenAQ plans require a sensor and one UTC day')
        sample_raw, sample_manifest = openaq_paths(root, item.sensor_id, first, 1)
        raw_dir, manifest_dir = sample_raw.parent, sample_manifest.parent
        manifests = sorted(manifest_dir.glob('page=*.manifest.json')) if manifest_dir.exists() else []
        raws = sorted(raw_dir.glob('page=*.json')) if raw_dir.exists() else []
        if not manifests and not raws:
            return AuditResult(item, 'not_found', 'no_raw_or_manifest')
        if not manifests or len(raws) != len(manifests):
            return AuditResult(item, 'review', 'raw_manifest_count_mismatch')
        pages: list[tuple[int, int, Path]] = []
        for mf in manifests:
            matched = re.fullmatch(r'page=(\d{4}).manifest.json', mf.name)
            if not matched:
                return AuditResult(item, 'review', 'unknown_page_manifest')
            number = int(matched.group(1))
            raw, expected_manifest = openaq_paths(root, item.sensor_id, first, number)
            if number < 1 or mf != expected_manifest:
                return AuditResult(item, 'review', 'unexpected_page')
            m = _verified(root, mf, raw, source='openaq', product='openaq_v3_hours',
                          region=item.region_id, start=item.start, end=item.end,
                          req_id=request_id('openaq', item.sensor_id, item.start, number))
            if m is None:
                return AuditResult(item, 'review', 'manifest_identity_status_or_sha_mismatch')
            pages.append((number, m['row_count'], raw))
        pages.sort()
        if [num for num, _, _ in pages] != list(range(1, len(pages) + 1)):
            return AuditResult(item, 'review', 'pagination_gap')
        limit = config['ingestion']['openaq'].get('page_limit', 100)
        if type(limit) is not int or limit < 1:
            return AuditResult(item, 'review', 'invalid_config_page_limit')
        if any(n > limit for _, n, _ in pages):
            return AuditResult(item, 'review', 'page_limit_changed_or_invalid')
        if any(n < limit for _, n, _ in pages[:-1]):
            return AuditResult(item, 'review', 'unexpected_early_terminal_page')
        last_num, last_rows, last_path = pages[-1]
        # An exact API 'found' count must agree with all stored pages, even when
        # the final page is short. Otherwise we would incorrectly mark a
        # truncated response COMPLETE and skip its missing observations forever.
        found = None
        try:
            obj = json.loads(last_path.read_text(encoding='utf-8'))
            if isinstance(obj, dict) and isinstance(obj.get('meta'), dict):
                found = obj['meta'].get('found')
        except (OSError, UnicodeError, TypeError, ValueError, AttributeError):
            pass
        total_rows = sum(n for _, n, _ in pages)
        if type(found) is int and found != total_rows:
            return AuditResult(item, 'review', 'pagination_found_count_mismatch')
        if last_rows < limit:
            return AuditResult(item, 'complete', 'verified_sha_and_terminal_page')
        # A full final page requires proof that more pages are not missing.
        if type(found) is int and found == total_rows:
            return AuditResult(item, 'complete', 'verified_sha_and_total_count')
        return AuditResult(item, 'review', 'pagination_terminal_unproven')

    if item.source == 'firms':
        if item.product not in FIRMS_PRODUCTS or item.sensor_id is not None:
            raise ValueError('bad FIRMS plan')
        days = (after - first).days
        if not 1 <= days <= 5:
            raise ValueError('FIRMS plan must be 1..5 days')
        bbox = valid_bbox(config['regions'][item.region_id])
        raw, mf, key = firms_paths(root, item.product, item.region_id, first, days, bbox, mode)
        product, source = item.product, 'nasa_firms'
        expected_n = None
    elif item.source == 'era5':
        if after != first + timedelta(days=1) or item.product is not None or item.sensor_id is not None:
            raise ValueError('ERA5 plan must be one day without sensor/product')
        area = validate_bbox(config['regions'][item.region_id])
        variables = variables_for_request(config['ingestion']['era5']['variables'], None)
        raw, mf, key = era5_paths(root, item.region_id, first, area, variables, mode)
        product, source = DATASET, 'era5'
        expected_n = len(variables) * 24
    else:
        raise ValueError('unsupported source')

    if not mf.exists() and not raw.exists():
        if _related_manifest(root, item.source, item):
            return AuditResult(item, 'review', 'overlapping_or_alternate_acquisition_found')
        return AuditResult(item, 'not_found', 'no_exact_matching_acquisition')
    m = _verified(root, mf, raw, source=source, product=product,
                  region=item.region_id, start=item.start, end=item.end,
                  req_id=key, mode=mode)
    if m is None:
        return AuditResult(item, 'review', 'manifest_identity_status_or_sha_mismatch')
    if expected_n is not None and m['row_count'] != expected_n:
        return AuditResult(item, 'review', 'era5_expected_message_count_mismatch')
    return AuditResult(item, 'complete', 'verified_manifest_identity_and_sha')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='OFFLINE read-only backfill audit; NO API or Raw writes.')
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids')
    parser.add_argument('--product', choices=FIRMS_PRODUCTS)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--max-plan-items', type=int, default=31)
    parser.add_argument('--format', choices=('text', 'json'), default='text')
    parser.add_argument('--config', default='configs/project.yaml')
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
        items = plan_backfill(source=args.source, region_id=args.region,
                              start=args.start, end=args.end, sensor_ids=sensor_ids,
                              product=args.product, max_plan_items=args.max_plan_items)
        results = [audit_item(root=ROOT, item=item, config=config) for item in items]
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f'BACKFILL AUDIT ERROR: {exc}', file=sys.stderr)
        return 2
    if args.format == 'json':
        print(json.dumps({'mode': 'offline_manifest_audit', 'no_download': True,
                          'results': [{'item': asdict(r.item), 'status': r.status,
                                       'reason': r.reason} for r in results]}, indent=2))
    else:
        print('OFFLINE MANIFEST AUDIT — NO NETWORK OR RAW WRITES')
        for i, result in enumerate(results, 1):
            print(f'{i:03d}  {result.item.start} -> {result.item.end}  '
                  f'{result.status.upper()}  {result.reason}')
        for status in ('complete', 'review', 'not_found'):
            print(f'{status.upper()}: {sum(r.status == status for r in results)}')
        print('REVIEW/NOT_FOUND do NOT authorize automatic downloads. No executor is included.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
