"""ERA5 single-levels CDS raw GRIB ingestion, batch one UTC day per request.

No GRIB -> Silver transforms.  All CDS calls are synchronous via cdsapi;
remote queue duration is controlled by CDS, not this CLI.
"""
from __future__ import annotations

import hashlib
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from .manifest import atomic_write_bytes, make_manifest, write_manifest
from .openaq import IngestionError, parse_days

DATASET = 'reanalysis-era5-single-levels'
VARIABLE_TO_SHORT = {
    '2m_temperature': '2t',
    '2m_dewpoint_temperature': '2d',
    '10m_u_component_of_wind': '10u',
    '10m_v_component_of_wind': '10v',
    'total_precipitation': 'tp',
}
MAX_GRIB_BYTES = 128 * 1024 * 1024  # one-day chunk; fail closed above this size
HOURS = tuple(f'{hour:02d}:00' for hour in range(24))


@dataclass
class ERA5Summary:
    days: int = 0
    messages: int = 0
    failures: int = 0

    @property
    def ok(self) -> bool:
        return self.failures == 0


def validate_bbox(bbox: dict) -> list[float]:
    try:
        north, west, south, east = (float(bbox[k]) for k in ('north', 'west', 'south', 'east'))
    except (KeyError, ValueError, TypeError):
        raise ValueError('ERA5 region bbox requires north/west/south/east') from None
    if not all(math.isfinite(n) for n in (north, west, south, east)) or not (
        -90 <= south < north <= 90 and -180 <= west < east <= 180
    ):
        raise ValueError('invalid ERA5 region bbox')
    return [north, west, south, east]  # CDS convention, NOT FIRMS convention


def variables_for_request(config_variables: list[str], selected: list[str] | None) -> list[str]:
    if not isinstance(config_variables, list) or not config_variables:
        raise ValueError('ERA5 variables must be a nonempty list')
    if len(set(config_variables)) != len(config_variables) or any(v not in VARIABLE_TO_SHORT for v in config_variables):
        raise ValueError('unsupported or duplicate ERA5 config variable')
    variables = config_variables if selected is None else selected
    if not variables or len(set(variables)) != len(variables) or any(v not in config_variables for v in variables):
        raise ValueError('requested ERA5 variables must be unique and belong to project config')
    return list(variables)


def cds_request(day: date, *, area: list[float], variables: list[str]) -> dict:
    """Exactly 24 valid UTC hours for a single day, one or more variables."""
    return {
        'product_type': ['reanalysis'],
        'variable': variables,
        'year': [f'{day.year:04d}'],
        'month': [f'{day.month:02d}'],
        'day': [f'{day.day:02d}'],
        'time': list(HOURS),
        'area': area,
        'data_format': 'grib',
        'download_format': 'unarchived',
    }


def _paths(root: Path, region_id: str, day: date, area: list[float],
           variables: list[str], mode: str) -> tuple[Path, Path, str]:
    identity = f'{DATASET}|{region_id}|{day.isoformat()}|{area}|{sorted(variables)}|{mode}|24h'
    request_key = hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]
    rel = (Path(DATASET) / region_id / f'year={day.year:04d}' /
           f'month={day.month:02d}' / f'day={day.day:02d}')
    raw = root / 'data/raw/era5' / rel / f'{mode}-{request_key}.grib'
    manifest = root / 'data/raw/era5/manifests' / rel / f'{mode}-{request_key}.manifest.json'
    return raw, manifest, request_key


def inspect_grib_framing(path: Path) -> int:
    """Inspect complete GRIB1/GRIB2 message envelopes; no GRIB decoding/normalization.

    Reject ZIP, truncated payloads, extra bytes and invalid terminators.
    EcCodes must additionally validate parameter/time completeness before success.
    """
    total_size = path.stat().st_size
    if not 20 <= total_size <= MAX_GRIB_BYTES:
        raise IngestionError('invalid_grib_size')
    count = 0
    with path.open('rb') as stream:
        offset = 0
        while offset < total_size:
            stream.seek(offset)
            header = stream.read(16)
            if len(header) < 8 or header[:4] != b'GRIB':
                raise IngestionError('invalid_grib_header')
            edition = header[7]
            if edition == 1:
                length = int.from_bytes(header[4:7], 'big')
                # Large GRIB1 multi-octet size extensions are intentionally unsupported.
            elif edition == 2 and len(header) == 16:
                length = int.from_bytes(header[8:16], 'big')
            else:
                raise IngestionError('unsupported_grib_edition')
            if length < 20 or offset + length > total_size:
                raise IngestionError('truncated_grib')
            stream.seek(offset + length - 4)
            if stream.read(4) != b'7777':
                raise IngestionError('invalid_grib_terminator')
            offset += length
            count += 1
        if offset != total_size or count == 0:
            raise IngestionError('invalid_grib_size')
    return count


def inspect_grib_semantics(path: Path, *, day: date, variables: list[str]) -> int:
    """Use ecCodes keys to prove all requested variables and 24 UTC hours exist once.

    Ingestion only inspects GRIB metadata; it does not decode grid arrays to Parquet.
    """
    try:
        import eccodes
    except ImportError:
        raise IngestionError('missing_eccodes') from None
    expected = {(VARIABLE_TO_SHORT[v], h) for v in variables for h in range(24)}
    observed = set()
    count = 0
    try:
        with path.open('rb') as stream:
            while True:
                gid = eccodes.codes_grib_new_from_file(stream)
                if gid is None:
                    break
                try:
                    short_name = str(eccodes.codes_get(gid, 'shortName'))
                    validity_date = int(eccodes.codes_get(gid, 'validityDate'))
                    validity_time = int(eccodes.codes_get(gid, 'validityTime'))
                    number_of_points = int(eccodes.codes_get(gid, 'numberOfPoints'))
                finally:
                    eccodes.codes_release(gid)
                hr, minute = divmod(validity_time, 100)
                pair = (short_name, hr)
                if (validity_date != int(day.strftime('%Y%m%d')) or minute != 0 or
                    not 0 <= hr <= 23 or number_of_points <= 0 or
                    pair not in expected or pair in observed):
                    raise IngestionError('grib_unexpected_or_duplicate_field')
                observed.add(pair)
                count += 1
    except IngestionError:
        raise
    except Exception:
        raise IngestionError('invalid_grib_metadata') from None
    if observed != expected:
        raise IngestionError('grib_missing_requested_fields')
    return count


def validate_grib(path: Path, *, day: date, variables: list[str]) -> int:
    envelopes = inspect_grib_framing(path)
    metadata = inspect_grib_semantics(path, day=day, variables=variables)
    if envelopes != metadata:
        raise IngestionError('grib_message_count_mismatch')
    return envelopes


def cds_download(dataset: str, request: dict, target: Path) -> None:
    """Blocking CDS retrieval. Never log credentials or unsanitized exceptions."""
    try:
        import cdsapi
    except ImportError:
        raise IngestionError('missing_cdsapi') from None
    try:
        client = cdsapi.Client(quiet=True, progress=False)
        client.retrieve(dataset, request, str(target))
    except Exception:
        # Errors may carry authenticated URL/token: never serialize/log repr(exc).
        raise IngestionError('cds_retrieval_failed') from None


def ingest_era5(*, root: Path, region_id: str, bbox: dict, start: str, end: str,
                config_variables: list[str], selected_variables: list[str] | None = None,
                dataset: str = DATASET, replay_file: Path | None = None,
                overwrite: bool = False,
                downloader: Callable = cds_download,
                validator: Callable = validate_grib) -> ERA5Summary:
    if dataset != DATASET:
        raise ValueError('ERA5 ingestion supports only reanalysis-era5-single-levels')
    area = validate_bbox(bbox)
    variables = variables_for_request(config_variables, selected_variables)
    days = parse_days(start, end)  # 1..31 days; each CDS job handles ONE day
    if replay_file is not None:
        if len(days) != 1:
            raise ValueError('ERA5 replay requires exactly one UTC day')
        if not replay_file.is_file():
            raise ValueError('ERA5 replay GRIB file not found')
    summary = ERA5Summary()
    mode = 'replay' if replay_file is not None else 'live'
    for day in days:
        raw_path, manifest_path, req_id = _paths(root, region_id, day, area, variables, mode)
        start_utc = f'{day.isoformat()}T00:00:00Z'
        end_utc = f'{(day + timedelta(days=1)).isoformat()}T00:00:00Z'
        temp_dir = root / 'data/raw/era5/_downloads'
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp = temp_dir / f'{uuid.uuid4().hex}.download'
        error_type = None
        n_messages = None
        raw_bytes = None
        raw_relative = None
        try:
            if replay_file is not None:
                # copy rather than rename user's irreplaceable source file
                # chunked copy avoids loading it before size guard
                from shutil import copyfile
                copyfile(replay_file, temp)
            else:
                downloader(dataset, cds_request(day, area=area, variables=variables), temp)
            n_messages = validator(temp, day=day, variables=variables)
            raw_bytes = temp.read_bytes()
            if len(raw_bytes) > MAX_GRIB_BYTES:
                raise IngestionError('grib_too_large')
            atomic_write_bytes(raw_path, raw_bytes, overwrite=overwrite)
            raw_relative = raw_path.relative_to(root).as_posix()
            summary.days += 1
            summary.messages += n_messages
        except IngestionError as exc:
            error_type = exc.code
        except FileExistsError:
            error_type = 'raw_file_conflict'
        except (OSError, ValueError):
            error_type = 'era5_file_io_error'
        finally:
            temp.unlink(missing_ok=True)
        if error_type:
            summary.failures += 1
            failed = make_manifest(region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                                   request_id_value=req_id, mode=mode, status='failed',
                                   raw_file=None, raw_bytes=None, row_count=None,
                                   error_type=error_type, source='era5', source_product=dataset,
                                   data_format='grib')
            if error_type != 'raw_file_conflict' or not manifest_path.exists():
                write_manifest(manifest_path, failed)
            continue
        manifest = make_manifest(region_id=region_id, start_utc=start_utc, end_utc=end_utc,
                                 request_id_value=req_id, mode=mode, status='success',
                                 raw_file=raw_relative, raw_bytes=raw_bytes, row_count=n_messages,
                                 source='era5', source_product=dataset, data_format='grib')
        write_manifest(manifest_path, manifest)
    return summary
