from typing import Optional, Any
from collections.abc import Collection
from subprocess import CalledProcessError

from ..zfs import Snapshot, ZfsCli
from .policy import apply_policy, KeepPolicy
from ..utils import group_snaps_by
from .grouping import GroupType, GET_GROUP


def prune_snapshots(
  cli: ZfsCli,
  snapshots: Collection[Snapshot],
  policy: KeepPolicy,
  *,
  group_by: Optional[GroupType] = GroupType.DATASET,
  dry_run: bool = True,
) -> None:
  """
  Prune given snapshots according to keep policy
  """
  if not snapshots:
    print(f'No snapshots, nothing to do')
    return

  if group_by is None:
    print(f'Pruning {len(snapshots)} snapshots without grouping')
    keep, destroy = apply_policy(snapshots, policy)
    print_policy_result(keep, destroy)
  else:
    print(f'Pruning {len(snapshots)} snapshots, grouped by {group_by.value}')
    # group the snapshots. Result is a dict with group name as key and set of snaps as value
    groups = group_snaps_by(snapshots, GET_GROUP[group_by])
    keep: list[Snapshot] = []
    destroy: list[Snapshot] = []
    for _group, _snaps in groups.items():
      _keep, _destroy = apply_policy(_snaps, policy)
      keep += _keep
      destroy += _destroy
      print(f'Group "{_group}"')
      print_policy_result(_keep, _destroy)

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
    except CalledProcessError:
      print(f'Failed to destroy snapshot "{snap.longname}"')




def print_policy_result(keep: Collection[Snapshot], destroy: Collection[Snapshot]):
  print(f'Keeping {len(keep)} snapshots')
  for snap in keep:
    print_snap(snap)
  print(f'Destroying {len(destroy)} snapshots')
  for snap in destroy:
    print_snap(snap)

def print_snap(snap: Snapshot):
  print(f'    {snap.timestamp}  {snap.longname}')
