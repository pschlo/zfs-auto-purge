from __future__ import annotations

from ..zfs import Snapshot, ZfsCli


def replicate(source_cli: ZfsCli, source_dataset: str, dest_cli: ZfsCli, dest_dataset: str):
  source_snaps = sorted(source_cli.get_snapshots(source_dataset), key=lambda s: s.timestamp, reverse=True)
  source_latest = source_snaps[0]

  dest_snaps = sorted(dest_cli.get_snapshots(dest_dataset), key=lambda s: s.timestamp, reverse=True)
  dest_latest = dest_snaps[0]

  if source_latest.guid == dest_latest.guid:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  # find source snap with same GUID as latest dest snap
  source_base_index = next((i for i, s in enumerate(source_snaps) if s.guid == dest_latest.guid), None)
  if source_base_index is None:
    raise RuntimeError(f'Latest dest snapshot "{dest_latest.short_name}" does not exist on source dataset')
  source_base = source_snaps[source_base_index]

  snaps_to_transfer: list[Snapshot] = source_snaps[:source_base_index]

  print(f'Transferring {len(snaps_to_transfer)} snapshots')
  # create send and receive processes
  send_proc = source_cli.send_snapshot_async(source_latest, base=source_base)
  assert send_proc.stdout is not None
  recv_proc = dest_cli.receive_snapshot_async(dest_dataset, stdin=send_proc.stdout)

  send_proc.wait()
  recv_proc.wait()

  # dest_latest is now same as source_latest
  dest_base = dest_latest
  dest_latest = source_latest.with_dataset(dest_dataset)
  print(f'Transfer completed')

  
  ## --- Manage holds ---
  source_pool = source_cli.get_pool_from_dataset(source_dataset)
  dest_pool = dest_cli.get_pool_from_dataset(dest_dataset)

  source_tag = f'zfsnap-sendbase-{dest_pool.guid}'
  dest_tag = f'zfsnap-recvbase-{source_pool.guid}'

  # hold the just transferred snap on source and dest, i.e. source_latest
  source_cli.hold(source_latest, source_tag)
  dest_cli.hold(dest_latest, dest_tag)

  # release previously held base snap, i.e. source_base
  hold = source_cli.get_hold(source_base, source_tag)
  if hold is not None:
    source_cli.release(hold)

  hold = dest_cli.get_hold(dest_base, dest_tag)
  if hold is not None:
    dest_cli.release(hold)
