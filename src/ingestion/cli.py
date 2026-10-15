"""Owner: member 1; callable signature agreed in acquisition contract."""
import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(description='Raw ingestion - NOT IMPLEMENTED')
    parser.add_argument('--source', required=True, choices=['era5','openaq','firms'])
    parser.add_argument('--region', required=True)
    parser.add_argument('--start', required=True, help='UTC start date YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='UTC end-exclusive YYYY-MM-DD')
    args = parser.parse_args(argv)
    print(f'NOT IMPLEMENTED: ingestion for {args.source}; contract in contracts/DATA_ACQUISITION_V1.md')
    return 3


if __name__ == '__main__':
    raise SystemExit(main())
