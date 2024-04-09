from __future__ import annotations
from argparse import Namespace
from urllib.parse import urlparse
from typing import cast, Optional

from ..zfs import Snapshot, LocalZfsCli, RemoteZfsCli


def parse_source(source: str) -> tuple[Optional[str], str, str]:
  user: Optional[str]
  host: str
  dataset: str

  netloc, dataset = source.split(':')
  if '@' in netloc:
    user, host = netloc.split('@')
  else:
    user = None
    host = netloc

  return user, host, dataset


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  dest_dataset: str = args.dataset
  user, host, source_dataset = parse_source(args.source)
  port: Optional[int] = args.port

  print(f'Pulling from remote dataset "{source_dataset}" to local dataset "{dest_dataset}"')

  dest_cli = LocalZfsCli()
  source_cli = RemoteZfsCli(host=host, user=user, port=port)

  dest_snaps = sorted(dest_cli.get_snapshots(dest_dataset), key=lambda s: s.timestamp, reverse=True)
  dest_latest = dest_snaps[0]

  source_snaps = sorted(source_cli.get_snapshots(source_dataset), key=lambda s: s.timestamp, reverse=True)
  source_latest = source_snaps[0]

  if source_latest.guid == dest_latest.guid:
    print(f'Source dataset does not have any new snapshots, nothing to do')
    return

  # find source snap with same GUID as latest dest snap
  source_base_index = next((i for i, s in enumerate(source_snaps) if s.guid == dest_latest.guid), None)
  if source_base_index is None:
    raise RuntimeError(f'Latest local snapshot "{dest_latest.short_name}" does not exist on remote dataset')
  source_base = source_snaps[source_base_index]

  snaps_to_transfer: list[Snapshot] = source_snaps[:source_base_index]

  print(f'Transferring {len(snaps_to_transfer)} snapshots')
  # create send and receive processes
  send_proc = source_cli.send_snapshot_async(source_latest, base=source_base)
  assert send_proc.stdout is not None
  recv_proc = dest_cli.receive_snapshot_async(dest_dataset, stdin=send_proc.stdout)

  send_proc.wait()
  recv_proc.wait()

  # release incremental base, hold snapshot we just sent over
