from __future__ import annotations
from argparse import Namespace

from ..zfs import create_snapshot


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  dataset: str = args.dataset
  snapname: str = args.snapname

  print(f'Creating snapshot "{dataset}@{snapname}"')
  snap = create_snapshot(dataset=dataset, short_name=snapname)
