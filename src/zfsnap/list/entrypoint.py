from __future__ import annotations
from argparse import Namespace
from typing import Optional
import random

from ..zfs import LocalZfsCli


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  dataset: str = args.dataset
  recursive: bool = args.recursive

  snaps = LocalZfsCli().get_snapshots(dataset=dataset, recursive=recursive)
  longname_width = max(len(s.longname) for s in snaps)
  for snap in sorted(snaps, key=lambda s: s.timestamp):
    print(f'{snap.longname.ljust(longname_width)}  {snap.timestamp}')