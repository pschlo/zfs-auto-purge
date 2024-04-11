from __future__ import annotations

from ..zfs import Snapshot


# find source snap with same GUID as latest dest snap
def get_base_index(source_snaps: list[Snapshot], dest_snaps: list[Snapshot]) -> int:
  base = next((i for i, s in enumerate(source_snaps) if s.guid == dest_snaps[0].guid), None)
  if base is None:
    raise RuntimeError(f'Latest dest snapshot "{dest_snaps[0].shortname}" does not exist on source dataset')
  return base
