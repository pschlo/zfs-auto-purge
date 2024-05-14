from __future__ import annotations
from typing import Optional, cast
from collections.abc import Collection

from ..zfs import Snapshot, ZfsCli, ZfsProperty
from .get_base_index import get_base_index
from .send_receive_snap import send_receive


# TODO: raw send for encrypted datasets?
def replicate_snaps(source_cli: ZfsCli, source_snaps: Collection[Snapshot], dest_cli: ZfsCli, dest_dataset: str):
  """
  replicates source_snaps to dest_dataset
  all source_snaps must be of same dataset

  Let S and D be the snapshots on source and dest, newest first.
  Then D is a suffix of S, i.e. S[i:] = D for some i.
  We call this index i the base index. It is used as an incremental basis for sending snapshots S[:i].
  """
  if not source_snaps:
    print(f'No source snapshots given, nothing to do')
    return

  # --- determine hold tags ---
  source_pool = source_cli.get_pool_from_dataset(next(iter(source_snaps)).dataset)
  dest_pool = dest_cli.get_pool_from_dataset(dest_dataset)
  source_tag = f'zfsnap-sendbase-{dest_pool.guid}'
  dest_tag = f'zfsnap-recvbase-{source_pool.guid}'

  # sorting is required
  source_snaps = sorted(source_snaps, key=lambda s: s.timestamp, reverse=True)
  dest_snaps = dest_cli.get_snapshots(dest_dataset, sort_by=ZfsProperty.CREATION, reverse=True)

  base = get_base_index(source_snaps, dest_snaps)
  if base == 0:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  print(f'Transferring {base} snapshots')
  for i in range(base):
    send_receive(
      clis=(source_cli, dest_cli),
      dest_dataset=dest_dataset,
      hold_tags=(source_tag, dest_tag),
      snapshot=source_snaps[base-i-1],
      base=source_snaps[base-i],
      unsafe_release=(i > 0)
    )
    print(f'{i+1}/{base} transferred')
  print(f'Transfer completed')
