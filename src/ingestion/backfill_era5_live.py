"""Guarded one-day ERA5 CDS acquisition using the existing ingestion adapter.

Default is offline, read-only. An explicit approval enables one CDS retrieval
into staging, semantic GRIB validation and guarded publication of Raw+manifest.
A CDS retrieval can run for a long time, and `--max-bytes` is only a *post-
download file-size ceiling*, NOT a pre-download/network quota. See docs.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from src.common.validation import validate_config
from .backfill import ROOT, PlanItem, plan_backfill
from .backfill_audit import audit_item
from .backfill_gate import evaluate_gate
from .era5 import (DATASET, MAX_GRIB_BYTES, _paths, cds_download, ingest_era5,
                   validate_bbox, validate_grib, variables_for_request)
from .openaq import IngestionError

DEFAULT_MAX_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class ERA5LiveResult:
    state: str  # skipped | approval_required | blocked_review | failed | published
    requests: int
    grib_bytes: int  # size observed on disk; NOT guaranteed network bytes
    messages: int
    reason: str


def execute_one_era5_day(
    *, root: Path, item: PlanItem, config: dict, approve_live: bool,
    max_bytes: int = DEFAULT_MAX_BYTES,
    downloader: Callable[[str, dict, Path], None] = cds_download,
    validator: Callable = validate_grib,
) -> ERA5LiveResult:
    """One explicit synchronous CDS request, zero application-level retries.

    Test callers can inject a fake downloader and validator; ordinary live use
    invokes the ERA5 adapter's ecCodes-based semantic validator unchanged.
    """
    if type(approve_live) is not bool:
        raise ValueError('approve_live must be boolean')
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_GRIB_BYTES:
        raise ValueError(f'max_bytes must be 1..{MAX_GRIB_BYTES}')
    if item.source != 'era5' or item.sensor_id is not None or item.product is not None:
        raise ValueError('ERA5 requires no sensor ID or product option')
    planned = plan_backfill(source='era5', region_id=item.region_id,
                            start=item.start, end=item.end, max_plan_items=1)
    if planned != (item,):
        raise ValueError('ERA5 guard supports exactly one UTC day')
    if item.region_id not in config['regions']:
        raise ValueError('unknown region ID')
    area = validate_bbox(config['regions'][item.region_id])
    era5_settings = config['ingestion']['era5']
    variables = variables_for_request(era5_settings['variables'], None)
    if era5_settings['dataset'] != DATASET:
        raise ValueError('unsupported ERA5 dataset')

    gate = evaluate_gate(root=root, items=planned, config=config, max_candidates=1)
    if gate.state == 'nothing_to_do':
        return ERA5LiveResult('skipped', 0, 0, 0, 'already_complete')
    if gate.state != 'ready_for_manual_review':
        return ERA5LiveResult('blocked_review', 0, 0, 0, gate.decisions[0].reason)
    if not approve_live:
        return ERA5LiveResult('approval_required', 0, 0, 0, 'dry_run_only')

    source_root = root / 'data/raw/era5'
    first = date.fromisoformat(item.start)
    _, _, req_key = _paths(root, item.region_id, first, area, variables, 'live')
    lock_dir = source_root / '_locks'
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / f'{req_key}.lock'
    try:
        with lock_file.open('x', encoding='ascii') as lock:
            lock.write('local_exclusive_lock\n')
    except FileExistsError:
        return ERA5LiveResult('blocked_review', 0, 0, 0, 'active_or_stale_lock')

    requests = 0
    bytes_on_disk = 0
    try:
        if audit_item(root=root, item=item, config=config).status != 'not_found':
            return ERA5LiveResult('blocked_review', 0, 0, 0,
                                  'candidate_changed_after_lock')

        def bounded_download(dataset: str, request: dict, target: Path) -> None:
            nonlocal requests, bytes_on_disk
            if requests >= 1:
                raise IngestionError('request_budget_exceeded')
            requests += 1
            try:
                downloader(dataset, request, target)
            finally:
                # CDS downloads to disk; this detects oversize *after* transfer,
                # before ingest_era5 reads it into memory or validates it.
                if target.is_file():
                    bytes_on_disk = target.stat().st_size
            if bytes_on_disk > max_bytes:
                raise IngestionError('grib_budget_exceeded')

        stage_parent = source_root / '_staging'
        stage_parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='era5-', dir=stage_parent) as tmp:
            stage_root = Path(tmp)
            try:
                summary = ingest_era5(
                    root=stage_root, region_id=item.region_id,
                    bbox=config['regions'][item.region_id],
                    start=item.start, end=item.end,
                    config_variables=variables, selected_variables=None,
                    dataset=DATASET, overwrite=False,
                    downloader=bounded_download, validator=validator,
                )
            except (OSError, IngestionError, TypeError, ValueError):
                return ERA5LiveResult('failed', requests, bytes_on_disk, 0,
                                      'staged_ingestion_error')
            if not summary.ok or summary.days != 1 or summary.messages != 24 * len(variables):
                return ERA5LiveResult('failed', requests, bytes_on_disk, summary.messages,
                                      'staged_ingestion_failed')
            if audit_item(root=stage_root, item=item, config=config).status != 'complete':
                return ERA5LiveResult('failed', requests, bytes_on_disk, summary.messages,
                                      'staged_audit_not_complete')
            if audit_item(root=root, item=item, config=config).status != 'not_found':
                return ERA5LiveResult('blocked_review', requests, bytes_on_disk,
                                      summary.messages, 'candidate_changed_before_publish')

            src_raw, src_manifest, _ = _paths(stage_root, item.region_id, first,
                                               area, variables, 'live')
            dst_raw, dst_manifest, _ = _paths(root, item.region_id, first,
                                               area, variables, 'live')
            if not src_raw.is_file() or not src_manifest.is_file():
                return ERA5LiveResult('failed', requests, bytes_on_disk, summary.messages,
                                      'incomplete_stage')
            if dst_raw.exists() or dst_manifest.exists():
                return ERA5LiveResult('blocked_review', requests, bytes_on_disk,
                                      summary.messages, 'destination_not_empty')
            created: list[Path] = []
            try:
                for src, dst in ((src_raw, dst_raw), (src_manifest, dst_manifest)):
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    with src.open('rb') as reader, dst.open('xb') as writer:
                        created.append(dst)
                        shutil.copyfileobj(reader, writer, length=1024 * 1024)
                if audit_item(root=root, item=item, config=config).status != 'complete':
                    raise OSError('published_artifact_failed_audit')
            except (OSError, ValueError):
                for dst in reversed(created):
                    dst.unlink(missing_ok=True)
                return ERA5LiveResult('failed', requests, bytes_on_disk, summary.messages,
                                      'publish_failed')
            return ERA5LiveResult('published', requests, bytes_on_disk, summary.messages,
                                  'staged_and_verified')
    finally:
        lock_file.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Guarded one-day ERA5 acquisition.')
    parser.add_argument('--region', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--max-bytes', type=int, default=DEFAULT_MAX_BYTES,
                        help='POST-download GRIB size cap; NOT a network quota')
    parser.add_argument('--approve-live', action='store_true',
                        help='Authorize ONE synchronous CDS retrieval, which can take hours')
    args = parser.parse_args(argv)
    try:
        path = Path(args.config)
        if not path.is_absolute():
            path = ROOT / path
        config = validate_config(path)
        item, = plan_backfill(source='era5', region_id=args.region,
                              start=args.start, end=args.end, max_plan_items=1)
        result = execute_one_era5_day(root=ROOT, item=item, config=config,
                                      approve_live=args.approve_live,
                                      max_bytes=args.max_bytes)
    except (OSError, KeyError, TypeError, ValueError):
        print('ERA5 GUARDED BACKFILL ERROR: invalid plan, config or filesystem state',
              file=sys.stderr)
        return 2
    print(f'ERA5 GUARDED BACKFILL: {result.state.upper()}')
    print(f'CDS_REQUESTS: {result.requests}/1; GRIB_BYTES: {result.grib_bytes}/{args.max_bytes}')
    print(f'GRIB_MESSAGES: {result.messages}; REASON: {result.reason}')
    if not args.approve_live:
        print('DRY RUN ONLY — no CDS requests or Raw writes.')
    elif result.state == 'published':
        print('One ERA5 day published after GRIB validation; verify with offline Audit.')
    return 0 if result.state in ('skipped', 'approval_required', 'published') else 2


if __name__ == '__main__':
    raise SystemExit(main())
