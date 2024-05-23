from __future__ import annotations
from typing import Optional, cast
from collections.abc import Collection

from ..zfs import Snapshot, ZfsCli, ZfsProperty, Dataset
from .get_base_index import get_base_index
from .send_receive_snap import send_receive_incremental, send_receive_initial


def holdtag_src(dest_dataset: Dataset):
  return f'zfsnap-sendbase-{dest_dataset.guid}'

def holdtag_dest(src_dataset: Dataset):
  return f'zfsnap-recvbase-{src_dataset.guid}'


# TODO: raw send for encrypted datasets?
def replicate_snaps(source_cli: ZfsCli, source_snaps: Collection[Snapshot], dest_cli: ZfsCli, dest_dataset: str, initialize: bool):
  """
  replicates source_snaps to dest_dataset
  all source_snaps must be of same dataset

  Let S and D be the snapshots on source and dest, newest first.
  Then D[0] = S[b] for some index b.
  We call b the base index. It is used as an incremental basis for sending snapshots S[:b]
  """
  if not source_snaps:
    print(f'No source snapshots given, nothing to do')
    return

  # sorting is required
  source_snaps = sorted(source_snaps, key=lambda s: s.timestamp, reverse=True)

  # ensure dest dataset exists
  dest_exists: bool = any(dest_dataset == d.name for d in dest_cli.get_datasets())
  if not dest_exists:
    if initialize:
      print(f"Creating destination dataset by transferring the oldest snapshot")
      send_receive_initial(
        clis=(source_cli, dest_cli),
        dest_dataset=dest_dataset,
        snapshot=source_snaps[-1],
        holdtags=(holdtag_src, holdtag_dest)
      )
    else:
      raise RuntimeError(f'Destination dataset does not exists and will not be created')

  # get dest snaps
  dest_snaps = dest_cli.get_snapshots(dest_dataset, sort_by=ZfsProperty.CREATION, reverse=True)
  if not dest_snaps:
    raise RuntimeError(f'Destination dataset does not contain any snapshots')

  base = get_base_index(source_snaps, dest_snaps)

  # resolve hold tags
  source_tag = holdtag_src(dest_cli.get_dataset(dest_dataset))
  dest_tag = holdtag_dest(source_cli.get_dataset(next(iter(source_snaps)).dataset))
  release_obsolete_holds(source_cli, source_snaps, source_tag)
  release_obsolete_holds(dest_cli, dest_snaps, dest_tag)

  if base == 0:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  print(f'Transferring {base} snapshots')
  for i in range(base):
    send_receive_incremental(
      clis=(source_cli, dest_cli),
      dest_dataset=dest_dataset,
      holdtags=(source_tag, dest_tag),
      snapshot=source_snaps[base-i-1],
      base=source_snaps[base-i],
      unsafe_release=(i > 0)
    )
    print(f'{i+1}/{base} transferred')
  dest_snaps = [s.with_dataset(dest_dataset) for s in source_snaps[:base]] + dest_snaps
  print(f'Transfer completed')



def release_obsolete_holds(cli: ZfsCli, snaps: list[Snapshot], holdtag: str):
  """Releases all but the latest holds"""

  # determine hold tags for each snap
  holdtags = {s.longname: set() for s in snaps}
  for h in cli.get_holds([s.longname for s in snaps]):
    holdtags[h.snap_longname].add(h.tag)
  
  # find latest snap with holdtag
  for i, snap in enumerate(snaps):
    if holdtag in holdtags[snap.longname]:
      latest_hold_snap = i
      break
  else:
    # holdtag not set anywhere
    return
  
  # remove holdtag from all older snaps
  for snap in snaps[latest_hold_snap+1:]:
    if holdtag in holdtags[snap.longname]:
      cli.release([snap.longname], holdtag)
