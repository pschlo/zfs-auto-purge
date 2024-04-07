from typing import Optional
from re import Pattern

from .zfs import Snapshot, get_snapshots, destroy_snapshots
from .policy import apply_policy, ExpirePolicy


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.full_name}')


def purge_snaps(
  policy: ExpirePolicy,
  *,
  dry_run: bool = True,
  dataset: Optional[str] = None,
  recursive: bool = False,
  match_name: Optional[Pattern] = None
) -> None:
  
  snaps = get_snapshots(dataset, match_name=match_name, recursive=recursive)
  if not snaps:
    print(f'Did not find any snapshots, nothing to do')
    return
  print(f'Found {len(snaps)} snapshots')
  
  print(f'Applying policy {policy}')
  keep, destroy = apply_policy(snaps, policy)

  print(f'Keeping {len(keep)} snapshots')
  for snap in sorted(keep, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  print(f'Destroying {len(destroy)} snapshots')
  for snap in sorted(destroy, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  if not keep:
    raise RuntimeError(f"Refusing to destroy all snapshots")

  if dry_run:
    return

  print(f'Purging snapshots')
  destroy_snapshots(destroy)
