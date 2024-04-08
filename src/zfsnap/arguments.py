from __future__ import annotations
import argparse
from dateutil.relativedelta import relativedelta
import re
from . import prune as _prune
from . import create as _create
from . import fetch as _fetch


def get_args() -> argparse.Namespace:
  # create top-level parser
  parser = argparse.ArgumentParser('zfsnap')
  subparsers = parser.add_subparsers(dest="subcommand", required=True)
  parser.add_argument('-n', '--dry-run', action='store_true')
  parser.add_argument('-d', '--dataset', type=str, metavar="DATASET", default=None)
  parser.add_argument('-r', '--recursive', action='store_true')

  # create subcommand parsers
  _prune.setup_parser(subparsers.add_parser('prune'))
  _create.setup_parser(subparsers.add_parser('create'))
  _fetch.setup_parser(subparsers.add_parser('fetch'))

  return parser.parse_args()
