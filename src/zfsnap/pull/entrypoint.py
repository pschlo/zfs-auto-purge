from __future__ import annotations
from argparse import Namespace
from typing import cast, Optional

from ..zfs import LocalZfsCli, RemoteZfsCli
from ..replication_common import parse_remote, replicate
from .arguments import Args


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  if not args.dataset:
    raise ValueError(f"No dataset provided")
  local_dataset: str = args.dataset
  user, host, remote_dataset = parse_remote(args.remote)

  print(f'Pulling from remote source dataset "{remote_dataset}" to local dest dataset "{local_dataset}"')

  local_cli = LocalZfsCli()
  remote_cli = RemoteZfsCli(host=host, user=user, port=args.port)

  replicate(
    source_cli=remote_cli,
    source_dataset=remote_dataset,
    dest_cli=local_cli,
    dest_dataset=local_dataset,
    recursive=args.recursive
  )
