from __future__ import annotations
from argparse import ArgumentParser


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('remote', metavar='USER@HOST:DATASET')
  parser.add_argument('-p', '--port', type=int)
