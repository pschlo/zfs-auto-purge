from __future__ import annotations
from argparse import Namespace
from typing import Optional, Callable, cast
from dataclasses import dataclass

from ..zfs import LocalZfsCli, Snapshot, Hold, ZfsProperty
from .arguments import Args


COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
  name: str
  get: Callable[[Snapshot], str]

# TODO: Use this list output for other subcommands as well

def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  if not args.dataset:
    raise ValueError(f"No dataset provided")

  cli = LocalZfsCli()
  snaps = cli.get_all_snapshots(dataset=args.dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)

  # get hold tags for all snapshots with holds
  holdtags: dict[str, set[str]] = {s.longname: set() for s in snaps}
  for hold in cli.get_holds([s.longname for s in snaps]):
    holdtags[hold.snap_longname].add(hold.tag)
  
  fields: list[Field] = [
    Field('DATASET',    lambda s: s.dataset),
    Field('SHORT NAME', lambda s: s.shortname),
    Field('TAGS',       lambda s: ','.join(s.tags) if s.tags is not None else 'NONE'),
    Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    Field('HOLDS',      lambda s: ','.join(holdtags[s.longname]))
  ]
  widths: list[int] = [max(len(f.name), *(len(f.get(s)) for s in snaps), 0) for f in fields]
  total_width = (len(COLUMN_SEPARATOR) * ((len(fields) or 1) - 1)) + sum(widths)

  print(COLUMN_SEPARATOR.join(f.name.ljust(w) for f, w in zip(fields, widths)))
  print((HEADER_SEPARATOR * (total_width//len(HEADER_SEPARATOR) + 1))[:total_width])
  for snap in snaps:
    print(COLUMN_SEPARATOR.join(f.get(snap).ljust(w) for f, w in zip(fields, widths)))
