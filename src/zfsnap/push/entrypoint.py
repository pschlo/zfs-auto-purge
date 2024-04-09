from __future__ import annotations
from argparse import Namespace
from typing import cast, Optional

from ..zfs import LocalZfsCli, RemoteZfsCli
from ..replication_common import parse_remote, replicate


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  local_dataset: str = args.dataset
  user, host, remote_dataset = parse_remote(args.source)
  port: Optional[int] = args.port

  print(f'Pushing from local source dataset "{local_dataset}" to remote dest dataset "{remote_dataset}"')

  local_cli = LocalZfsCli()
  remote_cli = RemoteZfsCli(host=host, user=user, port=port)

  replicate(
    source_cli=local_cli,
    source_dataset=local_dataset,
    dest_cli=remote_cli,
    dest_dataset=remote_dataset
  )
