#!/usr/bin/env python3

from __future__ import annotations
from argparse import Namespace

from .policy import ExpirePolicy
from .prune import prune_snapshots


def entrypoint(args: Namespace):

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

    name = args.keep_name
  )

  prune_snapshots(policy, dry_run=args.dry_run, dataset=args.dataset, recursive=args.recursive, group=args.group_by)
