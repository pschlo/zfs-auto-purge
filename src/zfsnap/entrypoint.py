from __future__ import annotations

from .arguments import get_args
from . import (
  prune as _prune,
  create as _create,
  push as _push,
  pull as _pull
)


def entrypoint() -> None:
  args = get_args()
  subcommand = args.subcommand
  args.__delattr__('subcommand')

  s = subcommand
  if s == 'prune':
    _prune.entrypoint(args)
  elif s == 'create':
    _create.entrypoint(args)
  elif s == 'push':
    _push.entrypoint(args)
  elif s == 'pull':
    _pull.entrypoint(args)
  else:
    assert False
