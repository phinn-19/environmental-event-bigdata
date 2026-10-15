"""Foundation entrypoint. Deliberately NO real pipeline until modules are merged."""
import argparse
import shutil
import sys
from pathlib import Path

from src.common.validation import ContractError, read_json, validate_config, validate_fixture_set, validate_events

ROOT = Path(__file__).resolve().parent


def doctor():
    config = validate_config(ROOT / 'configs/project.yaml')
    counts = validate_fixture_set(ROOT)
    print('FOUNDATION PASS — config, table fixtures and event contract are consistent')
    print('Project:', config['project']['name'])
    print('Fixture rows:', ', '.join(f'{key}={num}' for key,num in sorted(counts.items())))
    print('No API/Spark call was made; production pipeline is NOT implemented.')
    return 0


def mock_run():
    doctor()
    source = ROOT / 'fixtures/events/events_mock.json'
    events = read_json(source)
    validate_events(events)
    config = validate_config(ROOT / 'configs/project.yaml')
    output = ROOT / config['storage']['demo'] / 'events_mock.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, output)
    print(f'MOCK ONLY / SYNTHETIC events ({len(events)}) saved to {output}')
    print('NOT actual anomaly detection. Do not use for real data claims or benchmark.')
    return 0


def main():
    parser = argparse.ArgumentParser(description='Environmental Event Big Data: foundation scaffold')
    parser.add_argument('command', choices=['doctor','mock-run','run'])
    args = parser.parse_args()
    try:
        if args.command == 'doctor': return doctor()
        if args.command == 'mock-run': return mock_run()
        print('NOT IMPLEMENTED: real run needs src.ingestion + src.processing + src.events merged and E2E tests.')
        return 3
    except (ContractError, OSError, ValueError, KeyError) as exc:
        print(f'FOUNDATION FAIL: {exc}',file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
