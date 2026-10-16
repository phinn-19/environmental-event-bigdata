"""Manifest v1 writer for raw ingestion. No credentials or request URLs are serialized."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


VALID_STATUSES = frozenset({'success', 'empty', 'partial', 'failed'})


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def request_id(source: str, sensor_id: int, day: str, page: int) -> str:
    """Stable logical request ID, independent of credentials and mode."""
    key = f'{source}|{sensor_id}|{day}|{page}'
    return hashlib.sha256(key.encode('utf-8')).hexdigest()[:20]


def atomic_write_bytes(path: Path, content: bytes, *, overwrite: bool = False) -> None:
    """Persist raw *exactly* as received; refuse unexpected overwrites by default."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if sha256_bytes(path.read_bytes()) == sha256_bytes(content):
            return  # idempotent replay/re-download
        if not overwrite:
            raise FileExistsError('raw file already exists with different bytes; use --overwrite explicitly')
    temp = path.with_name(path.name + f'.tmp-{os.getpid()}')
    try:
        temp.write_bytes(content)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def make_manifest(*, region_id: str, start_utc: str | None, end_utc: str | None,
                  request_id_value: str, mode: str, status: str,
                  raw_file: str | None, raw_bytes: bytes | None,
                  row_count: int | None, error_type: str | None = None,
                  source_product: str = 'openaq_v3_hours', source: str = 'openaq',
                  data_format: str = 'json') -> dict:
    if status not in VALID_STATUSES:
        raise ValueError('bad manifest status')
    if mode not in ('live', 'replay'):
        raise ValueError('bad acquisition mode')
    if source not in ('openaq', 'nasa_firms', 'era5') or data_format not in ('json', 'csv', 'grib'):
        raise ValueError('bad manifest source/format')
    return {
        'schema_version': 'manifest-v1',
        'source': source,
        'source_product': source_product,
        'acquisition_mode': mode,
        'region_id': region_id,
        'start_utc': start_utc,
        'end_utc': end_utc,
        'retrieved_at_utc': utc_now(),
        'format': data_format,
        'file': raw_file,
        'status': status,
        'row_count': row_count,
        'bytes': len(raw_bytes) if raw_bytes is not None else None,
        'sha256': sha256_bytes(raw_bytes) if raw_bytes is not None else None,
        'request_id': request_id_value,
        'error_type': error_type,
    }


def write_manifest(path: Path, manifest: dict) -> None:
    """Atomic JSON write; preserve JSON nulls and UTF-8 without leakage."""
    payload = (json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f'.tmp-{os.getpid()}')
    try:
        temp.write_bytes(payload)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)
