from __future__ import annotations

from .arguments import get_args
from . import (
  prune as _prune,
  create as _create,
  fetch as _fetch
)


def entrypoint() -> None:
  args = get_args()
  subcommand = args.subcommand
  args.__delattr__('subcommand')

  if subcommand == 'prune':
    _prune.entrypoint(args)
  elif subcommand == 'create':
    _create.entrypoint(args)
  elif subcommand == 'fetch':
    _fetch.entrypoint(args)
  else:
    assert False
