from __future__ import annotations
from collections.abc import Collection

from ..zfs import Snapshot, ZfsCli
from ..utils import group_snaps_by
from .replicate_snaps import replicate_snaps


def replicate_hierarchy(
    source_cli: ZfsCli, source_dataset_root: str, source_snaps: Collection[Snapshot],
    dest_cli: ZfsCli, dest_dataset_root: str,
    initialize: bool
):
  """
  replicates given snaps under dest_dataset
  keeps the dataset hierarchy
  all source_snaps must be under source_dataset_root
  """
  for abs_source_dataset, source_snaps in group_snaps_by(source_snaps, lambda s: s.dataset).items():
    assert abs_source_dataset.startswith(source_dataset_root)
    rel_dataset = abs_source_dataset.removeprefix(source_dataset_root)
    abs_dest_dataset = dest_dataset_root + rel_dataset

    replicate_snaps(source_cli, source_snaps, dest_cli, abs_dest_dataset, initialize=initialize)
