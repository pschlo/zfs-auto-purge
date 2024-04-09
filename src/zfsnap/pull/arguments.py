from __future__ import annotations
from argparse import ArgumentParser


def setup_parser(parser: ArgumentParser) -> None:
  parser.add_argument('endpoint', metavar='USER@HOST[:PORT]')
  parser.add_argument('remote_dataset', metavar='REMOTE-DATASET')