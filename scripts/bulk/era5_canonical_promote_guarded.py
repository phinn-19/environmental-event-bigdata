#!/usr/bin/env python3
"""ERA5 monthly HF staging -> private canonical lake/v1, guarded.

Modes:
  plan    zero network; list deterministic target structure
  check   read-only: independent staged SHA/provenance, manifest-v1 and overlap checks
  promote explicit approval; re-run all read checks; upload Raw then manifest,
          round-trip verify, refuse nonidentical objects, and support safe retries.

Do not run concurrently with other lake/v1 ERA5 writers. No CDS API, deletes,
source changes, local Raw writes, or Spark.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
import tempfile
from typing import Callable, Any

from era5_hf_staging_verify import (
    BUCKET, MONTHS, MAX_RAW_BYTES, MAX_JSON_BYTES,
    key_for, reconcile_month, expected_request_sha, expected_fields, digest,
)
from era5_canonical_preflight import (
    PREFIX as CANONICAL_PREFIX, MANIFEST_PREFIX,
    month_window, parse_utc, intervals_overlap, _safe_relative,
    MAX_OBJECTS, MAX_MANIFESTS,
)

CANONICAL_PRODUCT = 'reanalysis-era5-single-levels'
MAX_TOTAL_RAW = 100 * 1024 * 1024
MAX_BUCKET = 20 * 1024 * 1024 * 1024  # safety cap, not provider quota
MONTH_RE = re.compile(r'^(?:2021|2022|2023)-(?:0[1-9]|1[0-2])$')
SHA_RE = re.compile(r'^[a-f0-9]{64}$')


def require(ok: bool, reason: str) -> None:
    if not ok:
        raise ValueError(reason)


def approved_month(month: str) -> bool:
    """Months are limited to the historical queue range, not audited local ERA5."""
    return (bool(MONTH_RE.fullmatch(month)) and
            '2021-01' <= month <= '2023-11')


def parse_months(raw: str) -> tuple[str, ...]:
    months = tuple(x.strip() for x in raw.split(','))
    require(bool(months) and all(months) and len(set(months)) == len(months),
            'empty or duplicate months')
    require(all(approved_month(m) for m in months),
            'month outside approved historical cloud range')
    # Preserve the four-month verified pilot for existing workflows. For other
    # months only one month per invocation is allowed, to cap blast radius.
    require(len(months) == 1 or months == MONTHS,
            'only a single new month (or the original 4-month pilot) is allowed')
    return months


def canonical_paths(month: str) -> tuple[str, str]:
    require(approved_month(month), 'month out of scope')
    request_hash = expected_request_sha(month)[:20]
    y, mo = month.split('-')
    suffix = f'{CANONICAL_PRODUCT}/VN_BBOX/year={y}/month={mo}/live-monthly-{request_hash}'
    raw_rel = f'data/raw/era5/{suffix}.grib'
    mf_rel = f'data/raw/era5/manifests/{suffix}.manifest.json'
    require(_safe_relative(raw_rel) and _safe_relative(mf_rel), 'unsafe planned path')
    return raw_rel, mf_rel


def utc_z(date: datetime) -> str:
    return date.astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def build_manifest(month: str, state: dict, stage_manifest: dict, raw_path: Path,
                   schema: dict) -> dict:
    bytes_, sha = digest(raw_path)
    verified = reconcile_month(month, state, stage_manifest, bytes_, sha)
    # CDS download time is not tracked by the queue. The upstream audit time is
    # the earliest reliable recorded time by which the file was retrieved.
    # This is a documented conservative proxy, not a claimed CDS download time.
    audit_at = state.get('audit_time_utc')
    require(isinstance(audit_at, str) and audit_at, 'missing recorded audit time')
    dt = parse_utc(audit_at)
    start, end = month_window(month)
    raw_rel, _ = canonical_paths(month)
    manifest = {
        'schema_version': 'manifest-v1',
        'source': 'era5',
        'source_product': CANONICAL_PRODUCT,
        'acquisition_mode': 'live',
        'region_id': 'VN_BBOX',
        'start_utc': utc_z(start),
        'end_utc': utc_z(end),
        'retrieved_at_utc': utc_z(dt),
        'format': 'grib',
        'file': raw_rel,
        'status': 'success',
        # Number of GRIB messages, NOT number of decoded grid cells.
        'row_count': expected_fields(month),
        'bytes': verified['bytes'],
        'sha256': verified['sha256'],
        'request_id': verified['request_id'],
        'error_type': None,
    }
    validate_manifest(manifest, schema)
    return manifest


def validate_manifest(doc: dict, schema: dict, *, target_region: bool = True) -> None:
    """Validate manifest-v1; only proposed targets must use VN_BBOX.

    Existing ERA5 canonical manifests may legitimately be for HANOI or other
    regions. They must stay valid and retain their existing paths/provenance.
    Overlap checks still compare only the same VN_BBOX region as before.
    """
    from jsonschema import Draft202012Validator, FormatChecker
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(doc)
    require(doc.get('source') == 'era5' and doc.get('source_product') == CANONICAL_PRODUCT,
            'unexpected canonical source')
    require(isinstance(doc.get('region_id'), str) and bool(doc['region_id'].strip()),
            'invalid region')
    if target_region:
        require(doc['region_id'] == 'VN_BBOX', 'unexpected target region')
    require(doc.get('status') == 'success', 'canonical manifest not successful')
    require(_safe_relative(doc.get('file')), 'unsafe raw path')
    require(type(doc.get('bytes')) is int and doc['bytes'] > 0, 'bad raw size')
    require(type(doc.get('row_count')) is int and doc['row_count'] > 0, 'bad message count')
    require(bool(SHA_RE.fullmatch(str(doc.get('sha256', '')))), 'invalid SHA')
    require(parse_utc(doc['start_utc']) < parse_utc(doc['end_utc']), 'invalid interval')


def comparable(doc: dict, planned: dict) -> bool:
    # retrieved_at_utc is a signed manifest event field and may differ if an
    # earlier run already published the same immutable content.
    fields = ('schema_version', 'source', 'source_product', 'acquisition_mode',
              'region_id', 'start_utc', 'end_utc', 'format', 'file', 'status',
              'row_count', 'bytes', 'sha256', 'request_id', 'error_type')
    return all(doc.get(k) == planned.get(k) for k in fields)


def evaluate_canonical(docs: dict[str, dict], all_keys: set[str],
                       manifests: dict[str, dict]) -> list[dict]:
    """Return idempotent actions for each target; fail on OTHER overlap/orphans."""
    wanted_mf_keys = {f'lake/v1/{canonical_paths(m)[1]}': m for m in manifests}
    wanted_raw_keys = {f'lake/v1/{canonical_paths(m)[0]}': m for m in manifests}
    planned_records = []

    for remote_mf, existing in docs.items():
        for month, proposed in manifests.items():
            if existing.get('region_id') != 'VN_BBOX':
                continue
            if not intervals_overlap((parse_utc(existing['start_utc']),
                                      parse_utc(existing['end_utc'])), month_window(month)):
                continue
            if remote_mf != f'lake/v1/{canonical_paths(month)[1]}' or not comparable(existing, proposed):
                raise ValueError('overlap with another/differing ERA5 canonical manifest')

    for key in all_keys:
        if key in wanted_raw_keys or key in wanted_mf_keys:
            continue
        if key.startswith(CANONICAL_PREFIX) and any(
            f'/VN_BBOX/year={m[:4]}/month={m[5:]}/' in key
            for m in manifests
        ):
            raise ValueError('unreviewed raw/manifest object in destination month folder')

    for month, proposed in manifests.items():
        raw_rel, mf_rel = canonical_paths(month)
        raw_key, mf_key = 'lake/v1/' + raw_rel, 'lake/v1/' + mf_rel
        raw_exists, mf_exists = raw_key in all_keys, mf_key in all_keys
        require(not mf_exists or raw_exists, 'canonical manifest exists without raw')
        if mf_exists:
            require(mf_key in docs and comparable(docs[mf_key], proposed),
                    'canonical manifest does not match planned provenance')
        planned_records.append({
            'month': month, 'raw_key': raw_key, 'manifest_key': mf_key,
            'bytes': proposed['bytes'], 'sha256': proposed['sha256'],
            'status': 'ALREADY_PRESENT' if mf_exists else (
                'RESUME_MANIFEST' if raw_exists else 'NEW_RAW_AND_MANIFEST'),
        })
    return planned_records


def _download(download: Callable, bucket: str, token: str, remote: str, local: Path) -> None:
    download(bucket, files=[(remote, str(local))], token=token,
             raise_on_missing_files=True)
    require(local.is_file(), 'HF object missing after download')


def _round_trip(bucket: str, read_token: str, download: Callable,
                remote: str, expected_bytes: int, expected_sha: str,
                temp_root: Path, index: int) -> None:
    local = temp_root / f'verify-{index:05d}'
    _download(download, bucket, read_token, remote, local)
    require(digest(local) == (expected_bytes, expected_sha),
            'remote object bytes/SHA differs; no overwrite')


def _canonical_list(bucket: str, token: str, list_tree: Callable) -> dict[str, Any]:
    nodes = list(list_tree(bucket, prefix=CANONICAL_PREFIX, recursive=True, token=token))
    files = [x for x in nodes if getattr(x, 'type', None) == 'file']
    require(len(files) <= MAX_OBJECTS, 'too many canonical objects')
    result = {x.path: x for x in files}
    require(len(result) == len(files), 'duplicate canonical key')
    require(all(x.startswith(CANONICAL_PREFIX) for x in result), 'out-of-scope key')
    return result


def run_remote(*, mode: str, months: tuple[str, ...], bucket: str,
               read_token: str, write_token: str, schema: dict,
               get_info: Callable, list_tree: Callable,
               download: Callable, upload: Callable | None = None) -> dict:
    """Provider API is injected for offline unit tests (no tool-side secrets)."""
    require(mode in ('check', 'promote'), 'invalid mode')
    require(bucket == BUCKET and bool(read_token.strip()), 'invalid bucket/read token')
    require(get_info(bucket, token=read_token).private is True, 'bucket is not PRIVATE')
    if mode == 'promote':
        require(bool(write_token.strip()) and upload is not None, 'write token/action missing')
        require(get_info(bucket, token=write_token).private is True,
                'write token cannot access private bucket')

    # Read namespace; no remote writes up to and including all SHA/manifest checks.
    stage_prefix = 'experiments/era5_monthly_vn_bbox_v1/'
    stage_keys = {x.path for x in list_tree(bucket, prefix=stage_prefix,
                                           recursive=True, token=read_token)
                  if getattr(x, 'type', None) == 'file'}
    need = {key_for(m, kind) for m in months for kind in ('raw', 'manifest', 'state')}
    require(need <= stage_keys, 'missing staged source object')

    from jsonschema import Draft202012Validator
    Draft202012Validator.check_schema(schema)
    # Contract must be the reviewed manifest-v1 schema; no inline mutations.
    require(schema.get('additionalProperties') is False and
            schema.get('properties', {}).get('schema_version', {}).get('const') == 'manifest-v1',
            'unreviewed canonical schema')

    with tempfile.TemporaryDirectory(prefix='era5-canonical-guarded-') as td:
        t = Path(td)
        planned = {}
        local_raw = {}
        local_mf = {}
        sum_bytes = 0
        for m in months:
            files = {kind: t / f'{m}-{kind}' for kind in ('raw', 'manifest', 'state')}
            for kind, local in files.items():
                _download(download, bucket, read_token, key_for(m, kind), local)
                limit = MAX_RAW_BYTES if kind == 'raw' else MAX_JSON_BYTES
                require(0 < local.stat().st_size <= limit, 'unsafe staged file size')
            state = json.loads(files['state'].read_text(encoding='utf-8'))
            src_mf = json.loads(files['manifest'].read_text(encoding='utf-8'))
            manifest = build_manifest(m, state, src_mf, files['raw'], schema)
            sum_bytes += manifest['bytes']
            require(sum_bytes <= MAX_TOTAL_RAW, 'too much raw data for reviewed pilot')
            planned[m] = manifest
            local_raw[m] = files['raw']
            local_mf[m] = t / f'{m}-canonical-manifest.json'
            local_mf[m].write_text(json.dumps(manifest, indent=2, ensure_ascii=False,
                                               sort_keys=True) + '\n', encoding='utf-8')
            print(f"STAGING_REVERIFIED month={m} fields={manifest['row_count']} "
                  f"bytes={manifest['bytes']} sha256={manifest['sha256']}", flush=True)

        # Recheck current canonical, not previous run's preflight results.
        nodes = _canonical_list(bucket, read_token, list_tree)
        canonical_keys = set(nodes)
        manifest_nodes = [x for x in nodes.values()
                          if x.path.startswith(MANIFEST_PREFIX)
                          and x.path.endswith('.manifest.json')]
        require(len(manifest_nodes) <= MAX_MANIFESTS, 'too many canonical manifests')
        docs = {}
        for i, node in enumerate(manifest_nodes):
            require(type(node.size) is int and 0 < node.size <= MAX_JSON_BYTES,
                    'canonical manifest size outside safe range')
            p = t / f'canonical-{i:04d}.json'
            _download(download, bucket, read_token, node.path, p)
            require(p.stat().st_size == node.size, 'canonical manifest size mismatch')
            doc = json.loads(p.read_text(encoding='utf-8'))
            validate_manifest(doc, schema, target_region=False)
            require('lake/v1/' + doc['file'] in canonical_keys,
                    'canonical manifest missing referenced raw')
            docs[node.path] = doc

        actions = evaluate_canonical(docs, canonical_keys, planned)
        # In all modes, verify any already-existing target Raw exact size+SHA.
        for i, act in enumerate(actions):
            if act['raw_key'] in canonical_keys:
                _round_trip(bucket, read_token, download, act['raw_key'],
                            act['bytes'], act['sha256'], t, i)

        result = {'mode': 'CANONICAL_READ_ONLY_CHECK' if mode == 'check'
                  else 'CANONICAL_GUARDED_PROMOTE',
                  'months': list(months), 'canonical_checked_manifests': len(docs),
                  'source_bytes_verified': sum_bytes,
                  'actions': actions, 'hf_writes': 0, 'cds_calls': 0,
                  'schema': 'manifest-v1',
                  'retrieved_at_note': 'uses recorded source audit time as conservative retrieval timestamp proxy',
                  'no_deletes': True}
        if mode == 'check':
            print(f"CANONICAL_PROMOTION_DRY_RUN_PASS months={len(months)} "
                  f"raw_bytes={sum_bytes} new={sum(a['status'] == 'NEW_RAW_AND_MANIFEST' for a in actions)} "
                  f"resumable={sum(a['status'] == 'RESUME_MANIFEST' for a in actions)}", flush=True)
            return result

        # Upload is non-transactional, so publish Raw *then* manifest, verify
        # each object by downloading it. On interruption, rerun safely.
        # Never overwrite an existing key in an already-taken snapshot.
        require(upload is not None, 'no upload function')
        info = get_info(bucket, token=read_token)
        added_estimate = sum(
            (a['bytes'] if a['status'] == 'NEW_RAW_AND_MANIFEST' else 0)
            + (local_mf[a['month']].stat().st_size if a['status'] != 'ALREADY_PRESENT' else 0)
            for a in actions)
        require(type(info.size) is int and info.size + added_estimate <= MAX_BUCKET,
                'bucket size guard')
        new_uploads = 0
        for i, act in enumerate(actions):
            m = act['month']
            # A parallel writer can invalidate the snapshot, so re-list and
            # stop if an unexpected new object appears before each month.
            latest = set(_canonical_list(bucket, read_token, list_tree))
            new_keys = latest - canonical_keys
            require(not new_keys, 'canonical namespace changed concurrently; stop')
            if act['status'] == 'NEW_RAW_AND_MANIFEST':
                upload(bucket, add=[(str(local_raw[m]), act['raw_key'])], token=write_token)
                _round_trip(bucket, read_token, download, act['raw_key'],
                            act['bytes'], act['sha256'], t, 100 + i)
                canonical_keys.add(act['raw_key'])
                new_uploads += 1
                print(f'PROMOTED_RAW_VERIFIED month={m}', flush=True)
            if act['status'] in ('NEW_RAW_AND_MANIFEST', 'RESUME_MANIFEST'):
                mfbytes, mfsha = digest(local_mf[m])
                upload(bucket, add=[(str(local_mf[m]), act['manifest_key'])], token=write_token)
                _round_trip(bucket, read_token, download, act['manifest_key'],
                            mfbytes, mfsha, t, 200 + i)
                canonical_keys.add(act['manifest_key'])
                new_uploads += 1
                print(f'PROMOTED_MANIFEST_VERIFIED month={m}', flush=True)
            if act['status'] == 'ALREADY_PRESENT':
                print(f'SKIP_ALREADY_VERIFIED month={m}', flush=True)

        result['hf_writes'] = new_uploads
        print(f'CANONICAL_PROMOTED_PASS months={len(months)} objects_written={new_uploads}', flush=True)
        return result


def find_next_unpromoted_pass(*, bucket: str, read_token: str,
                              get_info: Callable, list_tree: Callable,
                              download: Callable) -> str | None:
    """Read-only discovery. Never trust a queue PASS without rechecking in run_remote.

    Select the most recent verified staging month that is not fully canonical.
    An existing manifest with missing Raw is an integrity failure, not a skip.
    A Raw-only upload is eligible for safe RESUME_MANIFEST in run_remote.
    """
    require(bucket == BUCKET and bool(read_token.strip()), 'invalid read-only bucket/token')
    require(get_info(bucket, token=read_token).private is True, 'bucket not PRIVATE')
    stage_prefix = 'experiments/era5_monthly_vn_bbox_v1/'
    stage_nodes = list(list_tree(bucket, prefix=stage_prefix, recursive=True,
                                 token=read_token))
    require(len(stage_nodes) <= 250, 'too many staging nodes for guarded scheduler')
    stage_keys = {node.path for node in stage_nodes if getattr(node, 'type', None) == 'file'}
    canonical_keys = set(_canonical_list(bucket, read_token, list_tree))

    # At most 35 queue months, ordered newest-first, no local-audited overlap.
    for year in (2023, 2022, 2021):
        for n in range(12, 0, -1):
            month = f'{year}-{n:02d}'
            if not approved_month(month):
                continue
            raw_rel, mf_rel = canonical_paths(month)
            raw_done = 'lake/v1/' + raw_rel in canonical_keys
            mf_done = 'lake/v1/' + mf_rel in canonical_keys
            require(not mf_done or raw_done,
                    'canonical manifest without Raw for ' + month)
            if raw_done and mf_done:
                continue

            state_key = key_for(month, 'state')
            if state_key not in stage_keys:
                # If canonical Raw is present but state absent, cannot resume safely.
                require(not raw_done, 'orphaned canonical Raw without staging state ' + month)
                continue

            with tempfile.TemporaryDirectory(prefix='era5-auto-state-') as t:
                local = Path(t) / 'state.json'
                _download(download, bucket, read_token, state_key, local)
                require(0 < local.stat().st_size <= MAX_JSON_BYTES,
                        'unsafe staging state size for ' + month)
                state = json.loads(local.read_text(encoding='utf-8'))
            require(isinstance(state, dict) and state.get('month') == month,
                    'staging state month mismatch for ' + month)
            if state.get('status') != 'PASS':
                # An in-flight/request-intent state is not eligible for promotion.
                require(not raw_done, 'canonical Raw exists for non-PASS state ' + month)
                continue
            require(state.get('audit_status') == 'PASS',
                    'staging marked PASS without audit for ' + month)
            require(all(key_for(month, k) in stage_keys for k in ('raw', 'manifest')),
                    'staging PASS lacks complete verified objects for ' + month)
            return month
    return None


def auto_tick(*, mode: str, bucket: str, read_token: str, write_token: str,
              schema: dict, get_info: Callable, list_tree: Callable,
              download: Callable, upload: Callable | None = None) -> dict:
    """Single-month guarded scheduler: check only unless explicit auto-promote gate."""
    require(mode in ('auto-check', 'auto-promote'), 'invalid auto mode')
    require(mode != 'auto-promote' or (bool(write_token.strip()) and upload is not None),
            'automatic write not explicitly authorized')
    chosen = find_next_unpromoted_pass(bucket=bucket, read_token=read_token,
                                      get_info=get_info, list_tree=list_tree,
                                      download=download)
    if chosen is None:
        print('AUTO_CANONICAL_IDLE no new staged PASS requiring canonical promotion', flush=True)
        return {'mode': mode, 'status': 'IDLE', 'months': [], 'hf_writes': 0,
                'cds_calls': 0, 'no_deletes': True}
    print(f'AUTO_CANONICAL_SELECTED month={chosen} mode={mode}', flush=True)
    result = run_remote(mode='check' if mode == 'auto-check' else 'promote',
                        months=(chosen,), bucket=bucket, read_token=read_token,
                        write_token=write_token if mode == 'auto-promote' else '',
                        schema=schema, get_info=get_info, list_tree=list_tree,
                        download=download,
                        upload=upload if mode == 'auto-promote' else None)
    result['auto_selected_month'] = chosen
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('plan', 'check', 'promote', 'auto-check', 'auto-promote'))
    parser.add_argument('--months', default=','.join(MONTHS))
    parser.add_argument('--approve-hf-read', action='store_true')
    parser.add_argument('--approve-hf-write', action='store_true')
    parser.add_argument('--reviewed-manifest-contract', action='store_true')
    parser.add_argument('--schema', default='contracts/schemas/manifest.schema.json')
    parser.add_argument('--report', default='')
    args = parser.parse_args()
    try:
        months = () if args.mode.startswith('auto-') else parse_months(args.months)
        if args.mode == 'plan':
            report = {'mode': 'OFFLINE_PLAN', 'months': list(months),
                      'raw_manifest_targets': [canonical_paths(m) for m in months],
                      'hf_reads': 0, 'hf_writes': 0, 'cds_calls': 0}
            print(json.dumps(report, indent=2))
            return 0
        require(args.approve_hf_read, 'explicit HF read approval missing')
        if args.mode in ('promote', 'auto-promote'):
            require(args.approve_hf_write and args.reviewed_manifest_contract,
                    'explicit write and contract review approvals required')
        schema = json.loads(Path(args.schema).read_text(encoding='utf-8'))
        from huggingface_hub import bucket_info, list_bucket_tree, download_bucket_files, batch_bucket_files
        hf_read = os.environ.get('HF_READ_TOKEN', '')
        hf_write = (os.environ.get('HF_WRITE_TOKEN', '')
                    if args.mode in ('promote', 'auto-promote') else '')
        kwargs = dict(bucket=BUCKET, read_token=hf_read, write_token=hf_write,
                      schema=schema, get_info=bucket_info,
                      list_tree=list_bucket_tree, download=download_bucket_files,
                      upload=batch_bucket_files if hf_write else None)
        if args.mode.startswith('auto-'):
            result = auto_tick(mode=args.mode, **kwargs)
        else:
            result = run_remote(mode=args.mode, months=months, **kwargs)
        if args.report:
            p = Path(args.report)
            require(not p.exists(), 'report path already exists')
            p.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
            print('LOCAL_REPORT_CREATED ' + str(p))
        return 0
    except Exception as exc:
        # HTTP/provider errors can include private URL/token. Never echo exc.
        print(f'CANONICAL_PROMOTION_STOP: {type(exc).__name__}; do not retry blind, check state',
              file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
