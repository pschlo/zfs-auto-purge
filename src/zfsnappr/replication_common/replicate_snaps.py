from __future__ import annotations
from typing import Optional, cast
from collections.abc import Collection
import logging

from ..zfs import Snapshot, ZfsCli, ZfsProperty, Dataset
from .send_receive_snap import send_receive_incremental, send_receive_initial


log = logging.getLogger(__name__)


def holdtag_src(dest_dataset: Dataset):
  return f'zfsnappr-sendbase-{dest_dataset.guid}'

def holdtag_dest(src_dataset: Dataset):
  return f'zfsnappr-recvbase-{src_dataset.guid}'


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
    log.info(f'No source snapshots given, nothing to do')
    return

  # sorting is required
  source_snaps = sorted(source_snaps, key=lambda s: s.timestamp, reverse=True)

  # ensure dest dataset exists
  dest_exists: bool = any(dest_dataset == d.name for d in dest_cli.get_all_datasets())
  if not dest_exists:
    if initialize:
      log.info(f"Creating destination dataset by transferring the oldest snapshot")
      send_receive_initial(
        clis=(source_cli, dest_cli),
        dest_dataset=dest_dataset,
        snapshot=source_snaps[-1],
        holdtags=(holdtag_src, holdtag_dest)
      )
    else:
      raise RuntimeError(f'Destination dataset does not exists and will not be created')

  # get dest snaps
  dest_snaps = dest_cli.get_all_snapshots(dest_dataset, sort_by=ZfsProperty.CREATION, reverse=True)
  if not dest_snaps:
    raise RuntimeError(f'Destination dataset does not contain any snapshots')

  # figure out base index
  base = next((i for i, s in enumerate(source_snaps) if s.guid == dest_snaps[0].guid), None)
  if base is None:
    raise RuntimeError(f'Latest destination snapshot "{dest_snaps[0].shortname}" does not exist on source dataset')

  # resolve hold tags
  source_tag = holdtag_src(dest_cli.get_dataset(dest_dataset))
  dest_tag = holdtag_dest(source_cli.get_dataset(next(iter(source_snaps)).dataset))

  release_obsolete_holds((source_cli, dest_cli), (source_snaps, dest_snaps), (source_tag, dest_tag))

  if base == 0:
    log.info(f'Source dataset does not have any new snapshots, nothing to do')
    return

  log.info(f'Transferring {base} snapshots')
  for i in range(base):
    send_receive_incremental(
      clis=(source_cli, dest_cli),
      dest_dataset=dest_dataset,
      holdtags=(source_tag, dest_tag),
      snapshot=source_snaps[base-i-1],
      base=source_snaps[base-i],
      unsafe_release=(i > 0)
    )
    log.info(f'{i+1}/{base} transferred')
  dest_snaps = [s.with_dataset(dest_dataset) for s in source_snaps[:base]] + dest_snaps
  log.info(f'Transfer completed')



def release_obsolete_holds(clis: tuple[ZfsCli,ZfsCli], snaps: tuple[list[Snapshot],list[Snapshot]], holdtags: tuple[str,str]):
  """Finds the latest snapshot that exists on both sides and is held on both sides.
  Remove holds from any older snaps."""
  # find common snaps
  common_guids = {s.guid for s in snaps[0]} & {s.guid for s in snaps[1]}
  src_common_snaps = [(s,i) for i,s in enumerate(snaps[0]) if s.guid in common_guids]
  dest_common_snaps = [(s,i) for i,s in enumerate(snaps[1]) if s.guid in common_guids]

  # get source holds
  src_holds = {s.longname: set() for s in snaps[0]}
  for h in clis[0].get_holds([s.longname for s in snaps[0]]):
    src_holds[h.snap_longname].add(h.tag)

  # get dest holds
  dest_holds = {s.longname: set() for s in snaps[1]}
  for h in clis[1].get_holds([s.longname for s in snaps[1]]):
    dest_holds[h.snap_longname].add(h.tag)

  # find index in source and dest of latest common snap with holdtags
  for i in range(len(common_guids)):
    src_snap, src_index = src_common_snaps[i]
    dest_snap, dest_index = dest_common_snaps[i]
    if holdtags[0] in src_holds[src_snap.longname] and holdtags[1] in dest_holds[dest_snap.longname]:
      newest_common_snap = (src_index, dest_index)
      break
  else:
    # no commonly held snap
    newest_common_snap = (-1, -1)
  
  log.debug(f"Newest common snap is at indices {newest_common_snap}")
  
  # remove holdtag from all older snaps
  src_release = [s.longname for s in snaps[0][newest_common_snap[0]+1:] if holdtags[0] in src_holds[s.longname]]
  dest_release = [s.longname for s in snaps[1][newest_common_snap[1]+1:] if holdtags[1] in dest_holds[s.longname]]
  if src_release:
    log.info(f"Releasing {len(src_release)} obsolete holds in source")
  if dest_release:
    log.info(f"Releasing {len(dest_release)} obsolete holds in destination")
  clis[0].release(src_release, holdtags[0])
  clis[1].release(dest_release, holdtags[1])
