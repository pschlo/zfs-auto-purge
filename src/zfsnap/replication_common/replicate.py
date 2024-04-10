from __future__ import annotations

from ..zfs import ZfsCli
from .replicate_snaps import replicate_snaps
from .replicate_hierarchy import replicate_hierarchy


def replicate(source_cli: ZfsCli, source_dataset: str, dest_cli: ZfsCli, dest_dataset: str, recursive: bool=False):
  if recursive:
    source_snaps = source_cli.get_snapshots(source_dataset, recursive=True)
    replicate_hierarchy(source_cli, source_dataset, source_snaps, dest_cli, dest_dataset)
  else:
    source_snaps = source_cli.get_snapshots(source_dataset)
    replicate_snaps(source_cli, source_snaps, dest_cli, dest_dataset)
