from __future__ import annotations
from argparse import Namespace
from typing import cast, Optional

from ..zfs import LocalZfsCli, Snapshot, ZfsProperty
from .policy import KeepPolicy
from .prune_snaps import prune_snapshots
from .arguments import Args
from .grouping import GroupType


def entrypoint(raw_args: Namespace):
  args = cast(Args, raw_args)

  filter_tags: set[frozenset[str]] = {frozenset(b.split(',')) for b in args.tag}

  policy = KeepPolicy(
    last = args.keep_last,
    hourly = args.keep_hourly,
    daily = args.keep_daily,
    weekly = args.keep_weekly,
    monthly = args.keep_monthly,
    yearly = args.keep_yearly,

    within = args.keep_within,
    within_hourly = args.keep_within_hourly,
    within_daily = args.keep_within_daily,
    within_weekly = args.keep_within_weekly,
    within_monthly = args.keep_within_monthly,
    within_yearly = args.keep_within_yearly,

    name = args.keep_name,
    tags = frozenset(args.keep_tag)
  )

  cli = LocalZfsCli()
  snapshots = cli.get_snapshots(dataset=args.dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)

  # filter for snapshots with tags
  # snapshots are included iff all of their tags are included in one of the groups in filter_tags
  # if no filter_tags are given, snaps are not filtered
  filtered_snaps: list[Snapshot]
  if filter_tags:
    filtered_snaps = []
    for snap in snapshots:
      if any(snap.tags >= group for group in filter_tags):
        filtered_snaps.append(snap)
  else:
    filtered_snaps = snapshots

  get_grouptype: dict[str, Optional[GroupType]] = {
    'dataset': GroupType.DATASET,
    '': None
  }

  prune_snapshots(cli, filtered_snaps, policy, dry_run=args.dry_run, group_by=get_grouptype[args.group_by])
