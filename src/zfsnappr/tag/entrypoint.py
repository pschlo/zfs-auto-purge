from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast
import random
import string

from ..zfs import LocalZfsCli, ZfsProperty
from .arguments import Args


TAG_SEPARATOR = "_"


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  cli = LocalZfsCli()
  
  # get all snapshots
  if args.snapshot:
    snapshots = cli.get_snapshots([f"{args.dataset}@{s}" for s in args.snapshot])
  else:
    snapshots = cli.get_all_snapshots(args.dataset, recursive=args.recursive)

  # extract tags from another tags property
  if args.set_from_prop is not None:
    p = args.set_from_prop
    props = cli.get_snapshot_properties([s.longname for s in snapshots], [p])
    for snap in snapshots:
      if props[snap.longname][p] != '-':
        tags = props[snap.longname][p].split(',')
        cli.set_tags(snap.longname, tags)
      else:
        print(f"Could not extract tags from property {p} for snapshot {snap.longname}")

  # extract tags from name
  if args.set_from_name:
    for snap in snapshots:
      s = [a for a in snap.shortname.split(TAG_SEPARATOR) if a]  # ignore empty tags
      shortname_notags, tags = s[0], frozenset(s[1:])
      cli.set_tags(snap.longname, tags)
      cli.rename_snapshot(snap.longname, shortname_notags)
