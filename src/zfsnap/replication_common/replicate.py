from __future__ import annotations
from typing import Optional

from ..zfs import Snapshot, ZfsCli


# find source snap with same GUID as latest dest snap
def get_base_index(source_snaps: list[Snapshot], dest_snaps: list[Snapshot]) -> int:
  base = next((i for i, s in enumerate(source_snaps) if s.guid == dest_snaps[0].guid), None)
  if base is None:
    raise RuntimeError(f'Latest dest snapshot "{dest_snaps[0].short_name}" does not exist on source dataset')
  return base


# TODO: recursive replication (only snapshots, i.e. less than -R)
# TODO: raw send for encrypted datasets?
"""
Let S and D be the snapshots on source and dest, newest first.
Then D is a suffix of S, i.e. S[i:] = D for some i.
We call this index i the base index. It is used as an incremental basis for sending snapshots S[:i].
"""
def replicate(source_cli: ZfsCli, source_dataset: str, dest_cli: ZfsCli, dest_dataset: str):
  source_snaps = sorted(source_cli.get_snapshots(source_dataset), key=lambda s: s.timestamp, reverse=True)
  dest_snaps = sorted(dest_cli.get_snapshots(dest_dataset), key=lambda s: s.timestamp, reverse=True)

  base = get_base_index(source_snaps, dest_snaps)
  if base == 0:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  print(f'Transferring {base} snapshots')
  send_proc = source_cli.send_snapshot_async(source_snaps[0], base=source_snaps[base])
  assert send_proc.stdout is not None
  recv_proc = dest_cli.receive_snapshot_async(dest_dataset, stdin=send_proc.stdout)
  send_proc.wait()
  recv_proc.wait()

  # up to base, dest and source how have the same snaps
  dest_snaps = [s.with_dataset(dest_dataset) for s in source_snaps[:base]] + dest_snaps
  print(f'Transfer completed, updating holds')
  

  ## --- Manage holds ---
  source_pool = source_cli.get_pool_from_dataset(source_dataset)
  dest_pool = dest_cli.get_pool_from_dataset(dest_dataset)

  source_tag = f'zfsnap-sendbase-{dest_pool.guid}'
  dest_tag = f'zfsnap-recvbase-{source_pool.guid}'

  # hold the just transferred latest snap
  source_cli.hold(source_snaps[0], source_tag)
  dest_cli.hold(dest_snaps[0], dest_tag)

  # release base snaps
  hold = source_cli.get_hold(source_snaps[base], source_tag)
  if hold is not None:
    source_cli.release(hold)

  hold = dest_cli.get_hold(dest_snaps[base], dest_tag)
  if hold is not None:
    dest_cli.release(hold)
