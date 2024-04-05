#!/usr/bin/env python3
from __future__ import annotations
import argparse
from dateutil.relativedelta import relativedelta
from zfs import Snapshot, get_filesystems, get_snapshots
from policy import ExpirePolicy, apply_policy


parser = argparse.ArgumentParser("simple_example")
parser.add_argument("counter", help="An integer will be increased by 1 and printed.", type=int)
args = parser.parse_args()
print(args.counter + 1)


def print_snap(snap: Snapshot):
  print(f'{snap.timestamp}  {snap.name}')

print(get_filesystems())
print()
snaps = get_snapshots()

print()
policy = ExpirePolicy(
  hourly=5,
)
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
