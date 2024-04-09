from __future__ import annotations
from argparse import Namespace
from urllib.parse import urlparse
from subprocess import Popen, PIPE
from typing import cast, Optional

from ..zfs import Snapshot, LocalZfsCli, RemoteZfsCli


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  local_dataset: str = args.dataset
  endpoint: str = args.endpoint
  remote_dataset: str = args.remote_dataset

  netloc = urlparse('//' + endpoint)  # only recognizes netloc if it starts with //
  assert netloc.hostname is not None

  print(f'Pulling from remote dataset "{remote_dataset}" to local dataset "{local_dataset}"')

  local_cli = LocalZfsCli()
  remote_cli = RemoteZfsCli(host=netloc.hostname, user=netloc.username, port=netloc.port)


  # determine last local snapshot, use as incremental base
  local_snaps = sorted(local_cli.get_snapshots(local_dataset), key=lambda s: s.timestamp, reverse=True)
  local_latest = local_snaps[0]

  remote_snaps = sorted(remote_cli.get_snapshots(remote_dataset), key=lambda s: s.timestamp, reverse=True)
  remote_latest = remote_snaps[0]

  if local_latest.guid == remote_latest.guid:
    print(f'Remote dataset does not have any new snapshots, nothing to do')
    return

  # find remote snap with same GUID as latest local snap
  remote_base_index = next((i for i, s in enumerate(remote_snaps) if s.guid == local_latest.guid), None)
  if remote_base_index is None:
    raise RuntimeError(f'Latest local snapshot "{local_latest.short_name}" does not exist on remote dataset')
  remote_base = remote_snaps[remote_base_index]

  snaps_to_transfer: list[Snapshot] = remote_snaps[:remote_base_index]

  print(f'Transferring {len(snaps_to_transfer)} snapshots')
  # create send and receive processes
  send_proc = remote_cli.send_snapshot_async(remote_latest, base=remote_base)
  assert send_proc.stdout is not None
  recv_proc = local_cli.receive_snapshot_async(local_dataset, stdin=send_proc.stdout)

  send_proc.wait()
  recv_proc.wait()

  # release incremental base, hold snapshot we just sent over
