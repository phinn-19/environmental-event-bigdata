"""OpenAQ v3 hourly RAW acquisition (not Silver processing).

All date windows are half-open [start, end), UTC midnight boundaries.
A single UTC day/sensor/page produces a raw JSON file + manifest.
Raw HTTP payload is written without changing flags/nulls/period fields.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .manifest import atomic_write_bytes, make_manifest, request_id, write_manifest

BASE_URL = 'https://api.openaq.org/v3'
RETRYABLE_HTTP = frozenset({408, 429, 500, 502, 503, 504})


class IngestionError(Exception):
    """A sanitized diagnostic that never includes API keys or raw URL."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass
class IngestionSummary:
    pages: int = 0
    rows: int = 0
    empty_pages: int = 0
    failures: int = 0

    @property
    def ok(self) -> bool:
        return self.failures == 0


def parse_days(start: str, end: str) -> list[date]:
    try:
        start_date, end_date = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError as exc:
        raise ValueError('start/end must be valid YYYY-MM-DD dates') from exc
    if start_date.isoformat() != start or end_date.isoformat() != end:
        raise ValueError('start/end must use YYYY-MM-DD')
    delta = (end_date - start_date).days
    if not 1 <= delta <= 31:
        raise ValueError('date window must be 1..31 UTC days, end exclusive')
    return [start_date + timedelta(days=n) for n in range(delta)]


def _iso_utc(day: date) -> str:
    return f'{day.isoformat()}T00:00:00Z'


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward our API key to a redirected host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_get(url: str, headers: dict[str, str], timeout: float) -> bytes:
    request = urllib.request.Request(url, headers=headers, method='GET')
    opener = urllib.request.build_opener(_NoRedirect)
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise IngestionError(f'http_{response.status}')
        return response.read()


def get_with_retry(*, url: str, key: str, timeout: float,
                   max_retries: int, transport: Callable,
                   sleep: Callable[[float], None]) -> bytes:
    headers = {'X-API-Key': key, 'Accept': 'application/json', 'User-Agent': 'EnvironmentalEventBigData/0.1'}
    for attempt in range(max_retries + 1):
        try:
            return transport(url, headers, timeout)
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


def parse_payload(data: bytes, day: date) -> tuple[dict, list]:
    try:
        obj = json.loads(data.decode('utf-8'), parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, json.JSONDecodeError, ValueError):
        raise IngestionError('invalid_json') from None
    if not isinstance(obj, dict) or not isinstance(obj.get('results'), list):
        raise IngestionError('invalid_openaq_payload')
    for row in obj['results']:
        if not isinstance(row, dict):
            raise IngestionError('invalid_openaq_payload')
        try:
            ts = row['period']['datetimeFrom']['utc']
            parsed = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (KeyError, TypeError, ValueError, AttributeError):
            raise IngestionError('invalid_measurement_period') from None
        if not (parsed.tzinfo == timezone.utc and parsed.date() == day):
            raise IngestionError('out_of_window_measurement')
    return obj, obj['results']


def _paths(root: Path, sensor_id: int, day: date, page: int) -> tuple[Path, Path]:
    parts = (f'sensor={sensor_id}', f'year={day.year:04d}',
             f'month={day.month:02d}', f'day={day.day:02d}')
    raw = root / 'data/raw/openaq/hours' / Path(*parts) / f'page={page:04d}.json'
    manifest = root / 'data/raw/openaq/manifests' / Path(*parts) / f'page={page:04d}.manifest.json'
    return raw, manifest


def ingest_openaq(*, root: Path, region_id: str, sensor_ids: list[int],
                  start: str, end: str, api_key: str | None,
                  page_limit: int = 100, max_pages: int = 100,
                  timeout: float = 25.0, max_retries: int = 2,
                  replay_file: Path | None = None, overwrite: bool = False,
                  transport: Callable = http_get,
                  sleep: Callable[[float], None] = time.sleep) -> IngestionSummary:
    days = parse_days(start, end)
    if not sensor_ids or any(type(x) is not int or x <= 0 for x in sensor_ids):
        raise ValueError('sensor IDs must be positive integers')
    if not 1 <= page_limit <= 1000 or not 1 <= max_pages <= 1000:
        raise ValueError('page_limit and max_pages must be 1..1000')
    if timeout <= 0 or not 0 <= max_retries <= 5:
        raise ValueError('timeout/retry settings invalid')
    if replay_file is not None:
        if len(days) != 1 or len(sensor_ids) != 1:
            raise ValueError('replay mode requires exactly one day and one sensor')
        if not replay_file.is_file():
            raise ValueError('replay file does not exist')
    elif not api_key:
        raise ValueError('OPENAQ_API_KEY is missing (do not add the key to YAML or Git)')

    result = IngestionSummary()
    for sensor_id in sensor_ids:
        for day in days:
            start_utc, end_utc = _iso_utc(day), _iso_utc(day + timedelta(days=1))
            mode = 'replay' if replay_file is not None else 'live'
            for page in range(1, max_pages + 1):
                raw_path, manifest_path = _paths(root, sensor_id, day, page)
                request_code = request_id('openaq', sensor_id, day.isoformat(), page)
                raw_bytes = None
                raw_rel = None
                row_count = None
                err = None
                try:
                    if replay_file is not None:
                        raw_bytes = replay_file.read_bytes()
                    else:
                        query = urllib.parse.urlencode({
                            'datetime_from': start_utc,
                            'datetime_to': end_utc,
                            'page': page,
                            'limit': page_limit,
                        })
                        url = f'{BASE_URL}/sensors/{sensor_id}/hours?{query}'
                        raw_bytes = get_with_retry(url=url, key=api_key or '', timeout=timeout,
                                                   max_retries=max_retries, transport=transport, sleep=sleep)
                    obj, rows = parse_payload(raw_bytes, day)
                    row_count = len(rows)
                    # Do not overwrite different old data without explicit opt-in.
                    atomic_write_bytes(raw_path, raw_bytes, overwrite=overwrite)
                    raw_rel = raw_path.relative_to(root).as_posix()
                    status = 'empty' if not rows else 'success'
                    result.pages += 1
                    result.rows += row_count
                    if not rows:
                        result.empty_pages += 1
                except IngestionError as exc:
                    err = exc.code
                except FileExistsError:
                    err = 'raw_file_conflict'
                except OSError:
                    err = 'file_io_error'
                if err is not None:
                    result.failures += 1
                    # In failure manifests a file may be null: never reference a non-existent raw.
                    manifest = make_manifest(
                        region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                        request_id_value=request_code, mode=mode, status='failed',
                        raw_file=None, raw_bytes=None, row_count=None, error_type=err)
                    # Do not overwrite a successful prior manifest in a file-conflict case.
                    if err != 'raw_file_conflict' or not manifest_path.exists():
                        write_manifest(manifest_path, manifest)
                    break
                manifest = make_manifest(
                    region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                    request_id_value=request_code, mode=mode, status=status,
                    raw_file=raw_rel, raw_bytes=raw_bytes, row_count=row_count)
                write_manifest(manifest_path, manifest)
                if replay_file is not None or len(rows) < page_limit:
                    break
                # An exact numeric total is useful, but meta.found may be '>24'.
                found = obj.get('meta', {}).get('found') if isinstance(obj.get('meta'), dict) else None
                if type(found) is int and page * page_limit >= found:
                    break
                if page == max_pages:
                    # Preserve complete pages, signal that pagination may be incomplete.
                    result.failures += 1
                    failure_manifest = make_manifest(
                        region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                        request_id_value=request_id('openaq', sensor_id, day.isoformat(), page + 1),
                        mode=mode, status='failed', raw_file=None, raw_bytes=None,
                        row_count=None, error_type='pagination_limit_reached')
                    _, next_manifest = _paths(root, sensor_id, day, page + 1)
                    write_manifest(next_manifest, failure_manifest)
                    break
    return result
