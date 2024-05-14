from typing import TypeVar, Callable, Optional
from collections.abc import Collection, Hashable

from .zfs import Snapshot


T = TypeVar('T', bound=Hashable)

def group_snaps_by(snapshots: Collection[Snapshot], group: Callable[[Snapshot], T]) -> dict[T, set[Snapshot]]:
  groups: dict[T, set[Snapshot]] = {group(s): set() for s in snapshots}
  for snap in snapshots:
    groups[group(snap)].add(snap)
  return groups
