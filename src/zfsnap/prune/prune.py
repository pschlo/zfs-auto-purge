from typing import Optional, Any, Callable, TypeVar
from collections.abc import Collection
from subprocess import CalledProcessError
from enum import Enum

from ..zfs import Snapshot, LocalZfsCli
from .policy import apply_policy, ExpirePolicy


def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.fullname}')


T = TypeVar('T')

def group_by(snapshots: Collection[Snapshot], group: Callable[[Snapshot], T]) -> dict[T, set[Snapshot]]:
  a: dict[T, set[Snapshot]] = dict()
  for snap in snapshots:
    g = group(snap)
    if g not in a:
      a[g] = set()
    a[g].add(snap)
  return a


def prune_snapshots(
  policy: ExpirePolicy,
  *,
  dry_run: bool = True,
  dataset: Optional[str] = None,
  recursive: bool = False,
  group: str = 'dataset'
) -> None:
  cli = LocalZfsCli()
  
  snaps = cli.get_snapshots(dataset, recursive=recursive)
  if not snaps:
    print(f'Did not find any snapshots, nothing to do')
    return

  print(f'Found {len(snaps)} snapshots')
  
  print(f'Applying policy {policy}')
  groups: dict[Any, set[Snapshot]]
  if group == 'dataset':
    groups = group_by(snaps, lambda s: s.dataset)
  elif group == '':
    groups = {None: snaps}
  else:
    assert False

  # loop over groups and apply policy to each group
  keep = set()
  destroy = set()
  for _group, _snaps in groups.items():
    _keep, _destroy = apply_policy(_snaps, policy)
    keep.update(_keep)
    destroy.update(_destroy)

    if _group is not None:
      print(f'Group "{_group}"')
    print(f'Keeping {len(_keep)} snapshots')
    for snap in sorted(_keep, key=lambda x: x.timestamp, reverse=True):
      print_snap(snap)
    print(f'Destroying {len(_destroy)} snapshots')
    for snap in sorted(_destroy, key=lambda x: x.timestamp, reverse=True):
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
  for _dataset, _snaps in group_by(destroy, lambda s: s.dataset).items():
    try:
      cli.destroy_snapshots(_dataset, {s.shortname for s in _snaps})
    except CalledProcessError as e:
      # ignore if destroy failed with code 1, e.g. because it was held
      if e.returncode == 1: pass
      raise
