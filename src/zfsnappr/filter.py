from typing import Callable, Optional, Literal
from collections.abc import Collection

from .zfs import Snapshot


def parse_tags(tags: Collection[str]) -> Optional[set[frozenset[str]]]:
  if not tags:
    return None
  return {frozenset(b.split(',')) for b in tags}

def parse_shortnames(shortnames: Collection[str]) -> Optional[set[str]]:
  if not shortnames:
    return None
  return set(shortnames)

def filter_snaps(
  snapshots: Collection[Snapshot],
  tag: Optional[Collection[Collection[str]]] = None,
  dataset: Optional[Collection[str]] = None,
  shortname: Optional[Collection[str]] = None
) -> list[Snapshot]:
  filtered_snaps = []
  for snap in snapshots:
    keep = True

    # snap is included iff all of its tags are included in one of the groups in "tag"
    if tag is not None:
      for tag_group in tag:
        tag_group = set(tag_group)
        if snap.tags is not None and snap.tags >= tag_group:
          break
        if snap.tags is None and len(tag_group) == 1 and next(iter(tag_group)) == 'UNSET':
          break
      else:
        keep = False

    if dataset is not None:
      if not any(snap.dataset == d for d in dataset):
        keep = False

    if shortname is not None:
      if not any(snap.shortname == s for s in shortname):
        keep = False
    
    if keep:
      filtered_snaps.append(snap)

  return filtered_snaps
