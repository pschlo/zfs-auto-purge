#!/usr/bin/env python3
from __future__ import annotations
import argparse
from dateutil.relativedelta import relativedelta
from zfs import Snapshot, get_filesystems, get_snapshots
from policy import ExpirePolicy, apply_policy


class ParseError(Exception):
  def __init__(self, input: str, msg: str) -> None:
    super().__init__(f'Failed to parse duration "{input}": {msg}')


 # input has format like 2y5m7d3h
def to_relativedelta(input: str) -> relativedelta:
  res: dict[str, int] = dict()
  start = 0

  for i, c in enumerate(input):
    if c not in {'h', 'd', 'w', 'm', 'y'}:
      continue
    num = input[start:i]
    start = i+1
    if not num:
      raise ParseError(input, f'Unit "{c}" is without number')
    if c in res:
      raise ParseError(input, f'Duplicate unit "{c}"')
    try:
      res[c] = int(num)
    except ValueError:
      raise ParseError(input, f'Invalid number "{num}"')

  if not start == len(input):
    raise ParseError(input, f'Number "{input[start:]}" is without unit')

  return relativedelta(
    years=res.get('y', 0),
    months=res.get('m', 0),
    weeks=res.get('w', 0),
    days=res.get('d', 0),
    hours=res.get('h', 0)
  )



parser = argparse.ArgumentParser("simple_example")

x = [
  "--keep-last",
  "--keep-hourly",
  "--keep-daily",
  "--keep-weekly",
  "--keep-monthly",
  "--keep-yearly"
]
for i in x:
  parser.add_argument(i, type=int, metavar="N")

x = [
  "--keep-within",
  "--keep-within-hourly",
  "--keep-within-daily",
  "--keep-within-weekly",
  "--keep-within-monthly",
  "--keep-within-yearly"
]
for i in x:
  parser.add_argument(i, type=to_relativedelta, metavar="DURATION")


args = parser.parse_args()
print(print(args))
exit()

# def print_snap(snap: Snapshot):
#   print(f'{snap.timestamp}  {snap.name}')

# print(get_filesystems())
# print()
# snaps = get_snapshots()

# print()
# policy = ExpirePolicy(
#   hourly=5,
# )
# keep, remove = apply_policy(snaps, policy)
# assert keep | remove == snaps
# keep = sorted(keep, key=lambda x: x.timestamp, reverse=True)
# remove = sorted(remove, key=lambda x: x.timestamp, reverse=True)

# print("KEEP")
# for snap in keep:
#   print_snap(snap)
# print()
# print("REMOVE")
# for snap in remove:
#   print_snap(snap)
