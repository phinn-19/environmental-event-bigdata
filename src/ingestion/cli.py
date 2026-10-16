"""Batch raw acquisition CLI. OpenAQ, FIRMS, ERA5 raw ingestion implemented."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from src.common.validation import validate_config
from src.ingestion.openaq import ingest_openaq
from src.ingestion.openaq_metadata import ingest_openaq_location
from src.ingestion.firms import ingest_firms
from src.ingestion.era5 import ingest_era5
from src.ingestion.dotenv import load_project_env

ROOT = Path(__file__).resolve().parents[2]


def main(argv=None):
    parser = argparse.ArgumentParser(description='Environmental Event Big Data — raw ingestion')
    parser.add_argument('--source', required=True, choices=['era5', 'openaq', 'firms'])
    parser.add_argument('--resource', choices=['hours', 'locations'], default='hours',
                        help='OpenAQ resource; default hours preserves Sprint 01A CLI')
    parser.add_argument('--region', required=True)
    parser.add_argument('--start', help='YYYY-MM-DD (UTC inclusive), required for hours')
    parser.add_argument('--end', help='YYYY-MM-DD (UTC exclusive), required for hours')
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids', help='OpenAQ sensor ID; repeatable for hours')
    parser.add_argument('--location-id', type=int, help='OpenAQ location ID; required for locations')
    parser.add_argument('--replay-file', type=Path, help='OFFLINE: saved provider JSON, CSV or GRIB')
    parser.add_argument('--product', choices=['VIIRS_NOAA20_NRT', 'VIIRS_NOAA20_SP'], help='NASA FIRMS source product')
    parser.add_argument('--variable', action='append', dest='variables', help='ERA5 variable subset for pilot/replay; repeatable, default all 5')
    parser.add_argument('--overwrite', action='store_true', help='Allow replacing existing raw file with different bytes')
    args = parser.parse_args(argv)
    try:
        config_path = ROOT / args.config
        config = validate_config(config_path)
        if args.region not in config['regions']:
            raise ValueError(f'unknown region ID: {args.region}')
        load_project_env(ROOT)  # env vars win; .env is never printed or committed
        if args.source != 'era5' and args.variables is not None:
            raise ValueError('--variable applies only to ERA5')
        if args.source == 'era5':
            if args.resource != 'hours' or args.sensor_ids or args.location_id is not None or args.product is not None:
                raise ValueError('OpenAQ/FIRMS options do not apply to ERA5')
            if not args.start or not args.end:
                raise ValueError('--start and --end are required for ERA5')
            era5_opts = config['ingestion']['era5']
            summary = ingest_era5(
                root=ROOT, region_id=args.region, bbox=config['regions'][args.region],
                start=args.start, end=args.end,
                config_variables=era5_opts['variables'], selected_variables=args.variables,
                dataset=era5_opts['dataset'], replay_file=args.replay_file, overwrite=args.overwrite)
            mode = 'REPLAY (NO NETWORK)' if args.replay_file else 'LIVE'
            print(f'ERA5 {mode}: days={summary.days}, grib_messages={summary.messages}, failures={summary.failures}')
            print('Raw GRIB + manifest: data/raw/era5/ (Git-ignored). No Silver/Parquet output.')
            return 0 if summary.ok else 2
        if args.source == 'firms':
            if args.resource != 'hours' or args.sensor_ids or args.location_id is not None:
                raise ValueError('OpenAQ --resource/--sensor-id/--location-id do not apply to FIRMS')
            if not args.start or not args.end:
                raise ValueError('--start and --end are required for FIRMS')
            if args.product is None:
                raise ValueError('--product is required for FIRMS; choose NRT or SP explicitly')
            opts_firms = config['ingestion']['firms']
            summary = ingest_firms(
                root=ROOT, region_id=args.region, bbox=config['regions'][args.region],
                product=args.product, start=args.start, end=args.end,
                api_key=os.getenv('FIRMS_MAP_KEY'),
                chunk_days=opts_firms.get('max_days_per_request', 5),
                replay_file=args.replay_file, overwrite=args.overwrite)
            mode = 'REPLAY (NO NETWORK)' if args.replay_file else 'LIVE'
            print(f'FIRMS {mode}: chunks={summary.chunks}, raw_rows={summary.rows}, '
                  f'empty_chunks={summary.empty_chunks}, failures={summary.failures}')
            print('Raw CSV + manifest: data/raw/nasa_firms/ (Git-ignored). No Silver/Parquet output.')
            return 0 if summary.ok else 2
        if args.product is not None:
            raise ValueError('--product applies only to FIRMS')
        opts = config['ingestion']['openaq']
        if args.resource == 'hours':
            if not args.start or not args.end:
                raise ValueError('--start and --end are required for --resource hours')
            if args.location_id is not None:
                raise ValueError('--location-id is only for --resource locations')
            sensor_ids = args.sensor_ids or opts['pilot_sensor_ids']
            summary = ingest_openaq(
                root=ROOT, region_id=args.region, sensor_ids=sensor_ids,
                start=args.start, end=args.end, api_key=os.getenv('OPENAQ_API_KEY'),
                page_limit=opts.get('page_limit', 100),
                replay_file=args.replay_file, overwrite=args.overwrite)
            print(f'OpenAQ {"REPLAY (NO NETWORK)" if args.replay_file else "LIVE"}: '
                  f'pages={summary.pages}, raw_rows={summary.rows}, '
                  f'empty_pages={summary.empty_pages}, failures={summary.failures}')
        else:
            if args.start or args.end:
                raise ValueError('OpenAQ location metadata is not time-scoped; omit --start/--end')
            if args.location_id is None:
                raise ValueError('--location-id is required for --resource locations')
            if not args.sensor_ids or len(args.sensor_ids) != 1:
                raise ValueError('exactly one --sensor-id required to verify metadata for a selected sensor')
            summary = ingest_openaq_location(
                root=ROOT, region_id=args.region, location_id=args.location_id,
                sensor_id=args.sensor_ids[0], bbox=config['regions'][args.region],
                api_key=os.getenv('OPENAQ_API_KEY'), replay_file=args.replay_file,
                overwrite=args.overwrite)
            print(f'OpenAQ LOCATIONS {"REPLAY (NO NETWORK)" if args.replay_file else "LIVE"}: '
                  f'raw_locations={summary.rows}, failures={summary.failures}')
        print('Raw JSON + manifest: data/raw/openaq/ (Git-ignored). No Silver/Parquet output.')
        return 0 if summary.ok else 2
    except (ValueError, KeyError, OSError) as exc:
        # Only sanitized validation errors; never show API tokens or raw URLs.
        print(f'INGESTION ERROR: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
