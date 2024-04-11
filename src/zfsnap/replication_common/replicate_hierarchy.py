from __future__ import annotations
from collections.abc import Collection

from ..zfs import Snapshot, ZfsCli
from ..utils import group_snaps_by
from .replicate_snaps import replicate_snaps


"""
replicates given snaps under dest_dataset
keeps the dataset hierarchy
all source_snaps must be under source_dataset_root
"""
def replicate_hierarchy(
    source_cli: ZfsCli, source_dataset_root: str, source_snaps: Collection[Snapshot],
    dest_cli: ZfsCli, dest_dataset_root: str
):
  for dataset, snaps in group_snaps_by(source_snaps, lambda s: s.dataset).items():
    assert dataset.startswith(source_dataset_root)
    rel_dataset = dataset.removeprefix(source_dataset_root)
    replicate_snaps(source_cli, snaps, dest_cli, dest_dataset_root + rel_dataset)
