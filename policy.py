from __future__ import annotations
from typing import Any, Callable
from collections.abc import Collection
from dataclasses import dataclass
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zfs import Snapshot


@dataclass
class Bucket:
  count: int
  func: Callable[[datetime], int]
  last: int

@dataclass
class BucketWithin:
  within: relativedelta
  func: Callable[[datetime], int]
  last: int


@dataclass
class ExpirePolicy:
  last: int = 0
  hourly: int = 0
  daily: int = 0
  weekly: int = 0
  monthly: int = 0
  yearly: int = 0

  within: relativedelta = relativedelta()
  within_hourly: relativedelta = relativedelta()
  within_daily: relativedelta = relativedelta()
  within_weekly: relativedelta = relativedelta()
  within_monthly: relativedelta = relativedelta()
  within_yearly: relativedelta = relativedelta()


def unique_bucket(_: datetime) -> int:
  return random.getrandbits(128)

def hour_bucket(date: datetime) -> int:
  return date.year*1_000_000 + date.month*10_000 + date.day*100 + date.hour

def day_bucket(date: datetime) -> int:
  return date.year*10_000 + date.month*100 + date.day

def week_bucket(date: datetime) -> int:
  year, week, _ = date.isocalendar()
  return year*100 + week

def month_bucket(date: datetime) -> int:
  return date.year*100 + date.month

def year_bucket(date: datetime) -> int:
  return date.year


"""
Returns tuple (keep, remove)
"""
def apply_policy(snapshots: Collection[Snapshot], policy: ExpirePolicy) -> tuple[set[Snapshot], set[Snapshot]]:
  # all snapshots, sorted from latest to oldest
  snaps = sorted(snapshots, key=lambda x: x.timestamp, reverse=True)
  keep: set[Snapshot] = set()
  remove: set[Snapshot] = set()
  
  buckets: list[Bucket] = [
    Bucket(policy.last, unique_bucket, -1),
    Bucket(policy.hourly, hour_bucket, -1),
    Bucket(policy.daily, day_bucket, -1),
    Bucket(policy.weekly, week_bucket, -1),
    Bucket(policy.monthly, month_bucket, -1),
    Bucket(policy.yearly, year_bucket, -1)
  ]

  bucketsWithin: list[BucketWithin] = [
    BucketWithin(policy.within, unique_bucket, -1),
    BucketWithin(policy.within_hourly, hour_bucket, -1),
    BucketWithin(policy.within_daily, day_bucket, -1),
    BucketWithin(policy.within_weekly, week_bucket, -1),
    BucketWithin(policy.within_monthly, month_bucket, -1),
    BucketWithin(policy.within_yearly, year_bucket, -1)
  ]


  for snap in snaps:
    keep_snap = False

    for bucket in buckets:
      if bucket.count == 0:
        continue
      value = bucket.func(snap.timestamp)
      if value != bucket.last:
        keep_snap = True
        bucket.last = value
        if bucket.count > 0:
          bucket.count -= 1

    now = datetime.now()
    for bucket in bucketsWithin:
      if snap.timestamp <= now - bucket.within:
        # snap too old
        continue
      value = bucket.func(snap.timestamp)
      if value != bucket.last:
        keep_snap = True
        bucket.last = value

    if keep_snap:
      keep.add(snap)
    else:
      remove.add(snap)

  return keep, remove
