from __future__ import annotations
from typing import Optional, cast
from subprocess import CalledProcessError
from collections.abc import Collection

from ..zfs import Snapshot, ZfsCli
from .get_base_index import get_base_index


# TODO: recursive replication (only snapshots, i.e. less than -R)
# TODO: raw send for encrypted datasets?
"""
replicates source_snaps to dest_dataset
all source_snaps must be of same dataset

Let S and D be the snapshots on source and dest, newest first.
Then D is a suffix of S, i.e. S[i:] = D for some i.
We call this index i the base index. It is used as an incremental basis for sending snapshots S[:i].
"""
def replicate_snaps(source_cli: ZfsCli, source_snaps: Collection[Snapshot], dest_cli: ZfsCli, dest_dataset: str):
  source_snaps = sorted(source_snaps, key=lambda s: s.timestamp, reverse=True)
  dest_snaps = sorted(dest_cli.get_snapshots(dest_dataset), key=lambda s: s.timestamp, reverse=True)

  base = get_base_index(source_snaps, dest_snaps)
  if base == 0:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  print(f'Transferring {base} snapshots')

  source_pool = source_cli.get_pool_from_dataset(source_snaps[0].dataset)
  dest_pool = dest_cli.get_pool_from_dataset(dest_dataset)
  source_tag = f'zfsnap-sendbase-{dest_pool.guid}'
  dest_tag = f'zfsnap-recvbase-{source_pool.guid}'


  for i in range(base):
    transfer_snap, base_snap = source_snaps[base-i-1: base-i+1]
    source_cli.hold([transfer_snap], source_tag)

    send_proc = source_cli.send_snapshot_async(transfer_snap, base=base_snap)
    assert send_proc.stdout is not None
    recv_proc = dest_cli.receive_snapshot_async(dest_dataset, stdin=send_proc.stdout)
    for p in send_proc, recv_proc:
      p.wait()
      if p.returncode > 0:
        raise CalledProcessError(p.returncode, cmd=p.args)
    print(f'{i+1}/{base} transferred')

    dest_cli.hold([transfer_snap.with_dataset(dest_dataset)], dest_tag)

    # release snaps
    s = base_snap
    if i != 0 or (i == 0 and source_tag in source_cli.get_hold_tags([s])):
      source_cli.release([s], source_tag)
    s = base_snap.with_dataset(dest_dataset)
    if i != 0 or (i == 0 and dest_tag in dest_cli.get_hold_tags([s])):
      dest_cli.release([s], dest_tag)

  print(f'Transfer completed')
