from typing import Optional, Any, Mapping
from collections.abc import Collection
from subprocess import CalledProcessError

from ..zfs import Snapshot, ZfsCli
from .policy import apply_policy, KeepPolicy
from ..utils import group_snaps_by


def prune_snapshots(
  cli: ZfsCli,
  snapshots: Collection[Snapshot],
  policy: KeepPolicy,
  *,
  group_by: str = 'dataset',
  dry_run: bool = True,
) -> None:
  """
  Prune given snapshots according to keep policy
  """
  if not snapshots:
    print(f'No snapshots, nothing to do')
    return

  groups = get_groups(group_by, snapshots)
  s = f' {group_by}' if group_by else ''
  print(f'Pruning {len(snapshots)} snapshots in {len(groups)}{s} groups')

  keep: set[Snapshot] = set()
  destroy: set[Snapshot] = set()
  # loop over groups and apply policy to each group
  for _group, _snaps in groups.items():
    _keep, _destroy = apply_policy(_snaps, policy)
    keep.update(_keep)
    destroy.update(_destroy)
    print_group(_keep, _destroy, _group)

  if not keep:
    raise RuntimeError(f"Refusing to destroy all snapshots")
  if not destroy:
    print("No snapshots to prune")
    return
  if dry_run:
    return

  print(f'Destroying snapshots')
  for snap in destroy:
    try:
      cli.destroy_snapshots(snap.dataset, [snap.shortname])
    except CalledProcessError as e:
      # ignore if destroy failed with code 1, e.g. because it was held
      if not e.returncode == 1:
        raise




def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.longname}')

def print_group(keep: Collection[Snapshot], destroy: Collection[Snapshot], group: Optional[str]=None):
  if group is not None:
    print(f'Group "{group}"')
  print(f'Keeping {len(keep)} snapshots')
  for snap in sorted(keep, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)
  print(f'Destroying {len(destroy)} snapshots')
  for snap in sorted(destroy, key=lambda x: x.timestamp, reverse=True):
    print_snap(snap)

def get_groups(group_type: str, snaps: Collection[Snapshot]) -> Mapping[Any, Collection[Snapshot]]:
  if group_type == 'dataset':
    return group_snaps_by(snaps, lambda s: s.dataset)
  elif group_type == '':
    return {None: snaps}
  else:
    assert False
