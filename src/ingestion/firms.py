"""NASA FIRMS VIIRS NOAA-20 raw CSV ingestion (no hotspot clustering).

API area date is INCLUSIVE, project CLI end date is EXCLUSIVE.
Chunk requests into <=5 UTC days; preserve original response bytes.
Credentials are used only to construct an HTTPS request and never serialized.
"""
from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from .manifest import atomic_write_bytes, make_manifest, write_manifest
from .openaq import IngestionError, parse_days

BASE_URL = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
PRODUCTS = frozenset({'VIIRS_NOAA20_NRT', 'VIIRS_NOAA20_SP'})
VIIRS_COLUMNS = frozenset({
    'latitude', 'longitude', 'bright_ti4', 'scan', 'track', 'acq_date',
    'acq_time', 'satellite', 'instrument', 'confidence', 'version',
    'bright_ti5', 'frp', 'daynight',
})
RETRYABLE_HTTP = frozenset({408, 429, 500, 502, 503, 504})
MAX_CSV_BYTES = 64 * 1024 * 1024


@dataclass
class FIRMSIngestionSummary:
    chunks: int = 0
    rows: int = 0
    empty_chunks: int = 0
    failures: int = 0

    @property
    def ok(self) -> bool:
        return self.failures == 0


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect must not forward credentials embedded in the FIRMS URL."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_get(url: str, timeout: float) -> bytes:
    request = urllib.request.Request(url, headers={'Accept': 'text/csv',
                                                   'User-Agent': 'EnvironmentalEventBigData/0.1'},
                                     method='GET')
    opener = urllib.request.build_opener(_NoRedirect)
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise IngestionError(f'http_{response.status}')
        data = response.read(MAX_CSV_BYTES + 1)
        if len(data) > MAX_CSV_BYTES:
            raise IngestionError('csv_too_large')
        return data


def get_with_retry(*, url: str, timeout: float, max_retries: int,
                   transport: Callable, sleep: Callable) -> bytes:
    # Any error reported to caller must be a token-free code; HTTPError.url
    # contains the secret and may NEVER be printed, logged or re-raised.
    for attempt in range(max_retries + 1):
        try:
            return transport(url, timeout)
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP and attempt < max_retries:
                sleep(min(2 ** attempt, 8))
                continue
            raise IngestionError(f'http_{exc.code}') from None
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt < max_retries:
                sleep(min(2 ** attempt, 8))
                continue
            raise IngestionError('transport_error') from None
    raise AssertionError('unreachable')


def valid_bbox(bbox: dict) -> tuple[float, float, float, float]:
    try:
        west, south, east, north = (float(bbox[k]) for k in ('west', 'south', 'east', 'north'))
    except (KeyError, TypeError, ValueError):
        raise ValueError('region must specify numeric west/south/east/north') from None
    if not all(map(math.isfinite, (west, south, east, north))) or not (
        -180 <= west < east <= 180 and -90 <= south < north <= 90
    ):
        raise ValueError('invalid region bbox')
    return west, south, east, north


def windows(start: str, end: str, *, chunk_days: int = 5) -> list[tuple[date, int]]:
    days = parse_days(start, end)  # 1..31 days, end exclusive
    if type(chunk_days) is not int or not 1 <= chunk_days <= 5:
        raise ValueError('FIRMS chunk_days must be 1..5')
    return [(days[i], len(days[i:i + chunk_days])) for i in range(0, len(days), chunk_days)]


def _coords(bbox_tuple: tuple[float, float, float, float]) -> str:
    return ','.join(f'{v:g}' for v in bbox_tuple)


def _paths(root: Path, product: str, region_id: str, begin: date,
           days: int, bbox_tuple: tuple[float, float, float, float], mode: str) -> tuple[Path, Path, str]:
    # Mode in the request identity: replay and live must not overwrite
    # each other's manifests, even if the raw response happens to match.
    logical = f'nasa_firms|{product}|{region_id}|{_coords(bbox_tuple)}|{begin.isoformat()}|{days}|{mode}'
    key = hashlib.sha256(logical.encode('utf-8')).hexdigest()[:20]
    rel = Path(product) / region_id / f'year={begin.year:04d}' / f'month={begin.month:02d}'
    raw = root / 'data/raw/nasa_firms' / rel / f'{key}.csv'
    manifest = root / 'data/raw/nasa_firms/manifests' / rel / f'{key}.manifest.json'
    return raw, manifest, key


def inspect_csv(data: bytes, *, begin: date, days: int,
                bbox: tuple[float, float, float, float], product: str) -> int:
    """Validate essentials, keep every row and EVERY original byte unchanged."""
    if len(data) > MAX_CSV_BYTES:
        raise IngestionError('csv_too_large')
    try:
        text = data.decode('utf-8-sig')
    except UnicodeDecodeError:
        raise IngestionError('invalid_csv_encoding') from None
    try:
        reader = csv.DictReader(io.StringIO(text, newline=''), strict=True)
        fields = reader.fieldnames or []
        if len(set(fields)) != len(fields) or not VIIRS_COLUMNS.issubset(fields):
            raise IngestionError('invalid_firms_csv_header')
        count = 0
        west, south, east, north = bbox
        for row in reader:
            if None in row or any(row.get(c) is None for c in VIIRS_COLUMNS):
                raise IngestionError('invalid_firms_csv_row')
            try:
                lat, lon = float(row['latitude']), float(row['longitude'])
                acquired = date.fromisoformat(row['acq_date'])
                raw_time = row['acq_time'].strip()
                if not re.fullmatch(r'\d{1,4}', raw_time):
                    raise ValueError()
                hhmm = raw_time.zfill(4)
                if not (0 <= int(hhmm[:2]) < 24 and 0 <= int(hhmm[2:]) < 60):
                    raise ValueError()
            except (ValueError, TypeError):
                raise IngestionError('invalid_firms_coordinates_or_time') from None
            if not (math.isfinite(lat) and math.isfinite(lon) and
                    -90 <= lat <= 90 and -180 <= lon <= 180):
                raise IngestionError('invalid_firms_coordinates_or_time')
            if not begin <= acquired < begin + timedelta(days=days):
                raise IngestionError('out_of_window_hotspot')
            if not west <= lon <= east or not south <= lat <= north:
                raise IngestionError('out_of_region_hotspot')
            if product.endswith('_SP') and 'NRT' in row['version'].upper():
                raise IngestionError('source_product_mismatch')
            if row['satellite'].strip() == '' or row['instrument'].strip() == '':
                raise IngestionError('invalid_firms_csv_row')
            count += 1
        return count
    except (csv.Error, UnicodeError):
        raise IngestionError('invalid_firms_csv_row') from None


def ingest_firms(*, root: Path, region_id: str, bbox: dict, product: str,
                 start: str, end: str, api_key: str | None,
                 chunk_days: int = 5, timeout: float = 30.0,
                 max_retries: int = 2, replay_file: Path | None = None,
                 overwrite: bool = False, transport: Callable = http_get,
                 sleep: Callable = time.sleep) -> FIRMSIngestionSummary:
    if product not in PRODUCTS:
        raise ValueError('unsupported FIRMS product; choose VIIRS_NOAA20_NRT or VIIRS_NOAA20_SP')
    bbox_tuple = valid_bbox(bbox)
    chunks = windows(start, end, chunk_days=chunk_days)
    if timeout <= 0 or type(max_retries) is not int or not 0 <= max_retries <= 5:
        raise ValueError('timeout/retry settings invalid')
    if replay_file is not None:
        if len(chunks) != 1:
            raise ValueError('FIRMS replay requires a single request window (<=5 days)')
        if not replay_file.is_file():
            raise ValueError('replay file does not exist')
    elif not api_key:
        raise ValueError('FIRMS_MAP_KEY is missing (do not commit or print the key)')

    summary = FIRMSIngestionSummary()
    mode = 'replay' if replay_file is not None else 'live'
    for begin, days in chunks:
        raw_path, manifest_path, req_id = _paths(root, product, region_id, begin, days, bbox_tuple, mode)
        start_utc = f'{begin.isoformat()}T00:00:00Z'
        end_utc = f'{(begin + timedelta(days=days)).isoformat()}T00:00:00Z'
        raw_bytes = None
        raw_rel = None
        row_count = None
        error_type = None
        try:
            if replay_file is not None:
                raw_bytes = replay_file.read_bytes()
            else:
                # NASA: date is INCLUSIVE, day_range 1..5. Never use 'recent'
                # endpoint for historical requests because it changes over time.
                url = f'{BASE_URL}/{api_key}/{product}/{_coords(bbox_tuple)}/{days}/{begin.isoformat()}'
                raw_bytes = get_with_retry(url=url, timeout=timeout, max_retries=max_retries,
                                           transport=transport, sleep=sleep)
            row_count = inspect_csv(raw_bytes, begin=begin, days=days, bbox=bbox_tuple, product=product)
            atomic_write_bytes(raw_path, raw_bytes, overwrite=overwrite)
            raw_rel = raw_path.relative_to(root).as_posix()
            status = 'success' if row_count else 'empty'
            summary.chunks += 1
            summary.rows += row_count
            if not row_count:
                summary.empty_chunks += 1
        except IngestionError as exc:
            error_type = exc.code
        except FileExistsError:
            error_type = 'raw_file_conflict'
        except OSError:
            error_type = 'file_io_error'

        if error_type is not None:
            summary.failures += 1
            manifest = make_manifest(region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                                     request_id_value=req_id, mode=mode, status='failed',
                                     raw_file=None, raw_bytes=None, row_count=None,
                                     error_type=error_type, source_product=product,
                                     source='nasa_firms', data_format='csv')
            # Existing success manifest must not be destroyed on conflict.
            if error_type != 'raw_file_conflict' or not manifest_path.exists():
                write_manifest(manifest_path, manifest)
            continue

        manifest = make_manifest(region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                                 request_id_value=req_id, mode=mode, status=status,
                                 raw_file=raw_rel, raw_bytes=raw_bytes, row_count=row_count,
                                 source_product=product, source='nasa_firms', data_format='csv')
        write_manifest(manifest_path, manifest)
    return summary
