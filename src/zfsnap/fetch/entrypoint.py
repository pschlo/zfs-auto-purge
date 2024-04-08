from __future__ import annotations
from argparse import Namespace
from urllib.parse import urlparse


def entrypoint(args: Namespace) -> None:
  if not args.dataset:
    raise ValueError(f"No dataset provided")
  dataset: str = args.dataset
  endpoint: str = args.endpoint
  full_snapname: str = args.snapname

  netloc = urlparse('//' + endpoint)  # only recognizes netloc if it starts with //
  remote_dataset, remote_snapname = full_snapname.split('@')

  print(f'Fetching snapshot "{remote_snapname}" from dataset "{remote_dataset}" at address "{endpoint}"')

  cmd = ['zfs', 'send', full_snapname]
  # run_ssh_command(cmd, host=netloc.hostname, user=netloc.username, port=netloc.port)
  # receive_snapshot(dataset: str)

  
  # ssh user@host zfs send dataset@snap | zfs receive dataset