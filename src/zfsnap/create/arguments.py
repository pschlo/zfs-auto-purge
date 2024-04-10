from __future__ import annotations
from argparse import ArgumentParser


def setup_parser(parser: ArgumentParser) -> None:
  parser.add_argument('snapname', nargs='?', metavar='SNAPNAME')
  parser.add_argument('-t', '--tag', action='append')
