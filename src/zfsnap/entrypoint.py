from __future__ import annotations

from .arguments import get_args
from . import (
  prune as _prune,
  create as _create,
  pull as _pull
)


def entrypoint() -> None:
  args = get_args()
  subcommand = args.subcommand
  args.__delattr__('subcommand')

  if subcommand == 'prune':
    _prune.entrypoint(args)
  elif subcommand == 'create':
    _create.entrypoint(args)
  elif subcommand == 'pull':
    _pull.entrypoint(args)
  else:
    assert False
