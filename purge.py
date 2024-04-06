from policy import apply_policy, ExpirePolicy
from zfs import Snapshot, get_snapshots


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.name}')


def purge_snaps(policy: ExpirePolicy, dry_run: bool):
  snaps = get_snapshots()
  
  print(f'Applying policy')
  keep, remove = apply_policy(snaps, policy)

  print(f'Keeping {len(keep)} snapshots')
  for snap in sorted(keep, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  print()
  print(f'Destroying {len(remove)} snapshots')
  for snap in sorted(remove, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  if not keep:
    raise RuntimeError(f"Refusing to destroy all snapshots")

  if not dry_run:
    print(f'Purging snapshots')
    pass
    