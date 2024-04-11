from typing import Optional

from ..zfs import Snapshot, LocalZfsCli
from .policy import apply_policy, ExpirePolicy


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.fullname}')


def prune_snapshots(
  policy: ExpirePolicy,
  *,
  dry_run: bool = True,
  dataset: Optional[str] = None,
  recursive: bool = False,
) -> None:
  cli = LocalZfsCli()
  
  snaps = cli.get_snapshots(dataset, recursive=recursive)
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

  if not destroy:
    print("No snapshots to prune")
    return

  print(f'Pruning snapshots')

  # group snapshots by dataset
  _map: dict[str, set[str]] = dict()
  for snap in destroy:
    if snap.dataset not in _map:
      _map[snap.dataset] = set()
    _map[snap.dataset].add(snap.shortname)
  
  # call destroy for each dataset
  for _dataset, _shortnames in _map.items():
    cli.destroy_snapshots(_dataset, _shortnames)
