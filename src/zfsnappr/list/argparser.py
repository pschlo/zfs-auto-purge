from argparse import ArgumentParser


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('--tag', type=str, action='append', default=[])