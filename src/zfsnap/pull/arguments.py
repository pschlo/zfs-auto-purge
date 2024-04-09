from __future__ import annotations
from argparse import ArgumentParser


def setup_parser(parser: ArgumentParser) -> None:
  parser.add_argument('source', metavar='USER@HOST:DATASET')
  parser.add_argument('-p', '--port', type=int)
