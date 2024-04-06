#!/usr/bin/env python3
from __future__ import annotations
from policy import ExpirePolicy, apply_policy
from arguments import get_args
from zfs import Snapshot, get_filesystems, get_snapshots


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


def print_snap(snap: Snapshot):
  print(f'{snap.timestamp}  {snap.name}')

print(get_filesystems())
print()
snaps = get_snapshots()

print()
keep, remove = apply_policy(snaps, policy)
assert keep | remove == snaps
keep = sorted(keep, key=lambda x: x.timestamp, reverse=True)
remove = sorted(remove, key=lambda x: x.timestamp, reverse=True)

print("KEEP")
for snap in keep:
  print_snap(snap)
print()
print("REMOVE")
for snap in remove:
  print_snap(snap)
