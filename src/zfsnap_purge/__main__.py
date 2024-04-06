#!/usr/bin/env python3

from __future__ import annotations

from .arguments import get_args
from .policy import ExpirePolicy
from .purge import purge_snaps


args = get_args()

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
  within_yearly = args.keep_within_yearly
)

purge_snaps(policy, dry_run=args.dry_run, dataset=args.dataset, match_name=args.match_name, recursive=args.recursive)
