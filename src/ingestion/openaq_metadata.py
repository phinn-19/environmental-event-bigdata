"""OpenAQ v3 location metadata acquisition.

Fetches GET /v3/locations/{id} or replays an existing locations response.
Raw response remains byte-for-byte intact; any location/sensor checks are in-memory.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Callable

from .manifest import atomic_write_bytes, make_manifest, request_id, write_manifest
from .openaq import BASE_URL, IngestionError, IngestionSummary, get_with_retry, http_get


def inspect_location_payload(data: bytes, *, location_id: int, sensor_id: int,
                             bbox: dict | None = None) -> int:
    """Validate the selected station exists; return raw results count, not sensor count.

    For offline replay, `results` may be a *list of stations*, whereas the live
    /locations/{id} endpoint returns a list containing the selected station.
    """
    try:
        obj = json.loads(data.decode('utf-8'), parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, json.JSONDecodeError, ValueError):
        raise IngestionError('invalid_json') from None
    if not isinstance(obj, dict) or not isinstance(obj.get('results'), list):
        raise IngestionError('invalid_openaq_location_payload')
    rows = obj['results']
    if any(not isinstance(r, dict) for r in rows):
        raise IngestionError('invalid_openaq_location_payload')
    selected = [r for r in rows if type(r.get('id')) is int and r['id'] == location_id]
    if len(selected) != 1:
        raise IngestionError('selected_location_missing_or_duplicate')
    row = selected[0]
    coords = row.get('coordinates')
    if not isinstance(coords, dict):
        raise IngestionError('location_missing_coordinates')
    lat, lon = coords.get('latitude'), coords.get('longitude')
    if not (type(lat) in (float, int) and type(lon) in (float, int)
            and math.isfinite(lat) and math.isfinite(lon)
            and -90 <= lat <= 90 and -180 <= lon <= 180):
        raise IngestionError('location_invalid_coordinates')
    if bbox is not None and not (bbox['south'] <= lat <= bbox['north'] and
                                 bbox['west'] <= lon <= bbox['east']):
        raise IngestionError('location_outside_region')
    sensors = row.get('sensors')
    if not isinstance(sensors, list):
        raise IngestionError('location_missing_sensors')
    selected_sensors = [s for s in sensors if isinstance(s, dict) and s.get('id') == sensor_id]
    if len(selected_sensors) != 1:
        raise IngestionError('selected_sensor_missing_or_duplicate')
    param = selected_sensors[0].get('parameter')
    if not isinstance(param, dict) or param.get('name') != 'pm25':
        raise IngestionError('selected_sensor_not_pm25')
    # Preserve units and licenses as returned; downstream QC will decide compatibility.
    return len(rows)


def _location_paths(root: Path, location_id: int, mode: str) -> tuple[Path, Path, str]:
    # Different names for live and replay avoid treating a list-page replay as a
    # live detail response; repeated acquisitions require --overwrite if changed.
    logical_id = request_id(f'openaq_location_{mode}', location_id, 'v3', 1)
    raw = root / 'data/raw/openaq/locations' / f'{logical_id}.json'
    manifest = root / 'data/raw/openaq/manifests/locations' / f'{logical_id}.manifest.json'
    return raw, manifest, logical_id


def ingest_openaq_location(*, root: Path, region_id: str, location_id: int,
                           sensor_id: int, bbox: dict, api_key: str | None,
                           replay_file: Path | None = None, overwrite: bool = False,
                           timeout: float = 25.0, max_retries: int = 2,
                           transport: Callable = http_get,
                           sleep: Callable[[float], None] | None = None) -> IngestionSummary:
    """Fetch exactly one station's metadata and record a lossless raw + manifest.

    No time bounds are sent to the API. In the metadata manifest start/end are null,
    not invented days that could be misread as proof of historical sensor coverage.
    """
    if type(location_id) is not int or location_id <= 0:
        raise ValueError('location_id must be a positive integer')
    if type(sensor_id) is not int or sensor_id <= 0:
        raise ValueError('sensor_id must be a positive integer')
    if timeout <= 0 or not 0 <= max_retries <= 5:
        raise ValueError('timeout/retry settings invalid')
    if replay_file is not None:
        if not replay_file.is_file():
            raise ValueError('replay file does not exist')
    elif not api_key:
        raise ValueError('OPENAQ_API_KEY is missing (do not commit or print the key)')
    if sleep is None:
        import time
        sleep = time.sleep

    mode = 'replay' if replay_file is not None else 'live'
    raw_path, manifest_path, req_id = _location_paths(root, location_id, mode)
    summary = IngestionSummary()
    error_type: str | None = None
    raw_bytes: bytes | None = None
    row_count: int | None = None
    raw_rel: str | None = None
    try:
        if replay_file is not None:
            raw_bytes = replay_file.read_bytes()
        else:
            url = f'{BASE_URL}/locations/{location_id}'
            raw_bytes = get_with_retry(url=url, key=api_key or '', timeout=timeout,
                                       max_retries=max_retries, transport=transport,
                                       sleep=sleep)
        row_count = inspect_location_payload(raw_bytes, location_id=location_id,
                                             sensor_id=sensor_id, bbox=bbox)
        atomic_write_bytes(raw_path, raw_bytes, overwrite=overwrite)
        raw_rel = raw_path.relative_to(root).as_posix()
        summary.pages = 1
        summary.rows = row_count
    except IngestionError as exc:
        error_type = exc.code
    except FileExistsError:
        error_type = 'raw_file_conflict'
    except OSError:
        error_type = 'file_io_error'

    if error_type is not None:
        summary.failures = 1
        manifest = make_manifest(region_id=region_id, start_utc=None, end_utc=None,
                                 request_id_value=req_id, mode=mode, status='failed',
                                 raw_file=None, raw_bytes=None, row_count=None,
                                 error_type=error_type, source_product='openaq_v3_locations')
        if error_type != 'raw_file_conflict' or not manifest_path.exists():
            write_manifest(manifest_path, manifest)
    else:
        manifest = make_manifest(region_id=region_id, start_utc=None, end_utc=None,
                                 request_id_value=req_id, mode=mode, status='success',
                                 raw_file=raw_rel, raw_bytes=raw_bytes, row_count=row_count,
                                 source_product='openaq_v3_locations')
        write_manifest(manifest_path, manifest)
    return summary
