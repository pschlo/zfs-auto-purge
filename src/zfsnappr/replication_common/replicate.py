from __future__ import annotations

from ..zfs import ZfsCli, ZfsProperty
from .replicate_snaps import replicate_snaps
from .replicate_hierarchy import replicate_hierarchy


def replicate(source_cli: ZfsCli, source_dataset: str, dest_cli: ZfsCli, dest_dataset: str, recursive: bool=False, initialize: bool=False):
  source_snaps = source_cli.get_all_snapshots(source_dataset, recursive=recursive, sort_by=ZfsProperty.CREATION, reverse=True)
  if recursive:
    replicate_hierarchy(source_cli, source_dataset, source_snaps, dest_cli, dest_dataset, initialize=initialize)
  else:
    replicate_snaps(source_cli, source_snaps, dest_cli, dest_dataset, initialize=initialize)
