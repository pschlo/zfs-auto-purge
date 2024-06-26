from __future__ import annotations
import argparse

from . import (
  prune as _prune,
  create as _create,
  push as _push,
  pull as _pull,
  list as _list,
  tag as _tag,
  version as _version
)


def get_args() -> argparse.Namespace:
  # create top-level parser
  parser = argparse.ArgumentParser('zfsnappr')
  subparsers = parser.add_subparsers(dest="subcommand", required=True)
  parser.add_argument('-d', '--dataset', type=str, metavar="DATASET")
  parser.add_argument('-r', '--recursive', action='store_true')
  parser.add_argument('-n', '--dry-run', action='store_true')

  # create subcommand parsers
  _list.argparser.setup(subparsers.add_parser('list'))
  _create.argparser.setup(subparsers.add_parser('create'))
  _prune.argparser.setup(subparsers.add_parser('prune'))
  _push.argparser.setup(subparsers.add_parser('push'))
  _pull.argparser.setup(subparsers.add_parser('pull'))
  _tag.argparser.setup(subparsers.add_parser('tag'))
  _version.argparser.setup(subparsers.add_parser('version'))

  return parser.parse_args()
