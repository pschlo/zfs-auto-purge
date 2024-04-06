from policy import apply_policy, ExpirePolicy
from zfs import Snapshot, get_snapshots, destroy_snapshot
from typing import Optional
from re import Pattern


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.full_name}')


def purge_snaps(policy: ExpirePolicy, dry_run: bool, dataset: Optional[str] = None, match_name: Optional[Pattern] = None) -> None:
  snaps = get_snapshots(dataset, match_name=match_name)
  if not snaps:
    print(f'Could not find any snapshots, nothing to do')
    return
  
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
  for snap in destroy:
    destroy_snapshot(snap)
