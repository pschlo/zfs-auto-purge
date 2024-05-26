from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast, Literal

from ..zfs import LocalZfsCli, ZfsProperty, ZfsCli, Snapshot
from .arguments import Args


TAG_SEPARATOR = "_"


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

#   cli = LocalZfsCli()
  
#   # get all snapshots
#   if args.snapshot:
#     snapshots = cli.get_snapshots([f"{args.dataset}@{s}" for s in args.snapshot])
#   else:
#     snapshots = cli.get_all_snapshots(args.dataset, recursive=args.recursive)

#   snaps_to_tags: dict[Snapshot, Optional[set[str]]] = {s: set(s.tags) if s.tags is not None else None for s in snapshots}

#   # extract tags from another tags property
#   if args.set_from_prop is not None:
#     from_prop(cli, args.set_from_prop, snaps_to_tags, 'SET')
#   if args.add_from_prop is not None:
#     from_prop(cli, args.add_from_prop, snaps_to_tags, 'ADD')

#   # extract tags from name
#   if args.set_from_name:
#     from_name(cli, snaps_to_tags, 'SET')
#   if args.add_from_name:
#     from_name(cli, snaps_to_tags, 'ADD')

#   # set tags
#   for snap, tags in snaps_to_tags.items():
#     cli.set_tags(snap.longname, tags)


# def from_prop(cli: ZfsCli, property: str, snaps_to_tags: dict[Snapshot, Optional[set[str]]], mode: Literal['SET', 'ADD', 'REMOVE']):
#   props = cli.get_snapshot_properties([s.longname for s in snaps_to_tags], [property])
#   for snap in snaps_to_tags:
#     value = props[snap.longname][property]
#     if value == '-':
#       # property not set
#       continue
#     new_tags = set(value.split(','))
#     old_tags = snaps_to_tags[snap] or set()
#     if mode == 'ADD':
#       snaps_to_tags[snap] = old_tags | new_tags
#     elif mode == 'REMOVE':
#       snaps_to_tags[snap] = old_tags - new_tags
#     elif mode == 'SET':
#       snaps_to_tags[snap] = new_tags
#     else:
#       assert False

# def from_name(cli: ZfsCli, snaps_to_tags: dict[Snapshot, Optional[set[str]]], mode: Literal['SET', 'ADD', 'REMOVE']):
#     for snap in snaps_to_tags:
#       s = [a for a in snap.shortname.split(TAG_SEPARATOR) if a]  # ignore empty tags
#       shortname_notags, new_tags = s[0], set(s[1:])
#       if not new_tags:
#         # no tags in name
#         continue
#       old_tags = snaps_to_tags[snap] or set()
#       if mode == 'ADD':
#         snaps_to_tags[snap] = old_tags | new_tags
#       elif mode == 'REMOVE':
#         snaps_to_tags[snap] = old_tags - new_tags
#       elif mode == 'SET':
#         snaps_to_tags[snap] = new_tags
#       else:
#         assert False
