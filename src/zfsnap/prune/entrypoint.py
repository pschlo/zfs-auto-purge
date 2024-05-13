#!/usr/bin/env python3

from __future__ import annotations
from argparse import Namespace

from .policy import ExpirePolicy
from .prune_snaps import prune_snapshots
from ..zfs import LocalZfsCli


def entrypoint(args: Namespace):

  a: list[str] = args.tag or []
  tag: set[frozenset[str]] = {frozenset(b.split(',')) for b in a}

  policy = ExpirePolicy(
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

    name_matches = args.keep_name_matches,
    keep_tag = frozenset(args.keep_tag or [])
  )

  cli = LocalZfsCli()

  snapshots = cli.get_snapshots(dataset=args.dataset, recursive=args.recursive)

  # filter for snapshots with tags
  # snapshots are included iff all of their tags are included one of the groups in "tag"
  filtered_snaps = set()
  for snap in snapshots:
    if any(snap.tags >= group for group in tag):
      filtered_snaps.add(snap)

  prune_snapshots(cli, filtered_snaps, policy, dry_run=args.dry_run, group_by=args.group_by)
