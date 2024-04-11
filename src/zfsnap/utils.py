from typing import TypeVar, Callable
from collections.abc import Collection

from .zfs import Snapshot


T = TypeVar('T')

def group_by(snapshots: Collection[Snapshot], group: Callable[[Snapshot], T]) -> dict[T, set[Snapshot]]:
  a: dict[T, set[Snapshot]] = dict()
  for snap in snapshots:
    g = group(snap)
    if g not in a:
      a[g] = set()
    a[g].add(snap)
  return a
