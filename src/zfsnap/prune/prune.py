from typing import Optional
from collections.abc import Collection
from subprocess import CalledProcessError

from ..zfs import Snapshot, LocalZfsCli
from .policy import apply_policy, ExpirePolicy


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.fullname}')


def group_by_dataset(snapshots: Collection[Snapshot]) -> dict[str, set[Snapshot]]:
  a: dict[str, set[Snapshot]] = dict()
  for snap in snapshots:
    if snap.dataset not in a:
      a[snap.dataset] = set()
    a[snap.dataset].add(snap)
  return a


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

  dataset_to_snaps = group_by_dataset(snaps)
  print(f'Found {len(snaps)} snapshots in {len(dataset_to_snaps)} datasets')
  
  print(f'Applying policy {policy} to each dataset')
  keep = set()
  destroy = set()
  for _dataset, _snaps in dataset_to_snaps.items():
    _keep, _destroy = apply_policy(_snaps, policy)
    keep.update(_keep)
    destroy.update(_destroy)

  print(f'Keeping {len(keep)} snapshots')
  for snap in sorted(keep, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  print(f'Destroying {len(destroy)} snapshots')
  for snap in sorted(destroy, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

  if not keep:
    raise RuntimeError(f"Refusing to destroy all snapshots")
  if not destroy:
    print("No snapshots to prune")
    return
  if dry_run:
    return

  print(f'Pruning snapshots')
  # call destroy for each dataset
  for _dataset, _snaps in group_by_dataset(destroy).items():
    try:
      cli.destroy_snapshots(_dataset, {s.shortname for s in _snaps})
    except CalledProcessError as e:
      # ignore if destroy failed with code 1, e.g. because it was held
      if e.returncode == 1: pass
      raise
