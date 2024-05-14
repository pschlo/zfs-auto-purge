from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast
import random

from ..zfs import LocalZfsCli, ZfsProperty
from .arguments import Args


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  if not args.dataset:
    raise ValueError(f"No dataset provided")

  cli = LocalZfsCli()
  
  if args.snapname is not None:
    fullname = f'{args.dataset}@{args.snapname}'
  else:
    # use temporary name
    shortname: str = 'TEMP-' + hex(random.getrandbits(64))[2:].rjust(16, '0')
    fullname = f'{args.dataset}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=args.recursive,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(args.tag)
    }
  )

  if args.snapname is None:
    # rename to GUID
    # the GUID is 64 bit, which means we need at most 20 digits to display it in decimal
    snap = cli.get_snapshot(fullname)
    shortname = str(snap.guid).rjust(20, '0')
    cli.rename_snapshot(fullname, shortname)
    fullname = f'{args.dataset}@{shortname}'

  print(f'Created snapshot {fullname}')
