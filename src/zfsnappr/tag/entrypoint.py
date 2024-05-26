from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast, Literal, Callable

from ..zfs import LocalZfsCli, ZfsProperty, ZfsCli, Snapshot
from .. import filter
from .arguments import Args


TAG_SEPARATOR = "_"


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  cli = LocalZfsCli()

  # --- determine operations ---
  operations: list[
    tuple[
      Callable[[Snapshot], Optional[set[str]]],
      Literal['ADD', 'SET', 'REMOVE']
    ]
  ] = []

  # TODO: remove
  ...

  # set
  if args.set_from_name:
    operations.append((get_from_name, 'SET'))
  if args.set_from_prop is not None:
    p = args.set_from_prop
    operations.append((lambda s: get_from_prop(s, p), 'SET'))

  # add
  if args.add_from_name:
    operations.append((get_from_name, 'ADD'))
  if args.add_from_prop is not None:
    p = args.add_from_prop
    operations.append((lambda s: get_from_prop(s, p), 'ADD'))

  if not operations:
    print(f"No tag operations specified, nothing to do")
    return
  

  # --- get snapshots ---
  props = [p for p in [args.add_from_prop, args.set_from_prop] if p is not None]
  snapshots = cli.get_all_snapshots(args.dataset, recursive=args.recursive, properties=props)
  snapshots = filter.filter_snaps(snapshots, tag=filter.parse_tags(args.tag), shortname=filter.parse_shortnames(args.snapshot))
  if not snapshots:
    print(f"No snapshots, nothing to do")
    return

  # --- apply tag operations ---
  # SET sets the tags even if no new tags were found, while ADD and REMOVE leave the tags potentially unset, i.e. as None
  for snap in snapshots:
    for get_tags, action in operations:
      tags = snap.tags
      new_tags = get_tags(snap)

      if action == 'SET':
        tags = new_tags or set()
      elif action == 'ADD' and new_tags is not None:
        tags = (tags or set()) | new_tags
      elif action == 'REMOVE' and new_tags is not None:
        tags = (tags or set()) - new_tags

      # apply tag changes
      if tags != snap.tags and tags is not None:
        cli.set_tags(snap.longname, tags)



def get_from_prop(snap: Snapshot, property: str) -> Optional[set[str]]:
  value = snap.properties[property]
  if value == '-':
    # property not set
    return None
  return set(t for t in value.split(',') if t)  # ignore empty tags

def get_from_name(snap: Snapshot) -> Optional[set[str]]:
  s = [a for a in snap.shortname.split(TAG_SEPARATOR) if a]  # ignore empty tags
  shortname_notags, tags = s[0], set(s[1:])
  if not tags:
    # no tags in name
    return None
  return tags
