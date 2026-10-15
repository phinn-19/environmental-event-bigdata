"""Owner: member 2; scaffold only."""
import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(description='processing - NOT IMPLEMENTED')
    parser.add_argument('--config', default='configs/project.yaml')
    args = parser.parse_args(argv)
    print('NOT IMPLEMENTED: processing; expected config: ' + args.config)
    return 3


if __name__ == '__main__':
    raise SystemExit(main())
