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
  snapname: str = args.snapname or to_hex(random.getrandbits(64), 16)

  print(f'Creating snapshot of "{dataset}"')
  snap = LocalZfsCli().create_snapshot(dataset=dataset, short_name=snapname, recursive=recursive)


def to_hex(num: int, digits: int) -> str:
  hex_str = hex(num)[2:]
  hex_str_padded = f'{hex_str:0>{digits}}'
  return hex_str_padded
