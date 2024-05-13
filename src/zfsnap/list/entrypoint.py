from __future__ import annotations
from argparse import Namespace
from typing import Optional, Callable
from dataclasses import dataclass

from ..zfs import LocalZfsCli, Snapshot


COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
  name: str
  get: Callable[[Snapshot], str]


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  dataset: str = args.dataset
  recursive: bool = args.recursive

  cli = LocalZfsCli()
  snaps = sorted(cli.get_snapshots(dataset=dataset, recursive=recursive), key=lambda s: s.timestamp)
  fields: list[Field] = [
    Field('DATASET',    lambda s: s.dataset),
    Field('SHORT NAME', lambda s: s.shortname),
    Field('TAGS',       lambda s: ','.join(s.tags)),
    Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    Field('HOLDS',      lambda s: str(s.holds) if s.holds > 0 else '')
  ]
  widths: list[int] = [max(len(f.name), *(len(f.get(s)) for s in snaps)) for f in fields]
  total_width = (len(COLUMN_SEPARATOR) * (len(fields)-1)) + sum(widths)

  print(COLUMN_SEPARATOR.join(f.name.ljust(w) for f, w in zip(fields, widths)))
  print((HEADER_SEPARATOR * (total_width//len(HEADER_SEPARATOR) + 1))[:total_width])
  for snap in snaps:
    print(COLUMN_SEPARATOR.join(f.get(snap).ljust(w) for f, w in zip(fields, widths)))
