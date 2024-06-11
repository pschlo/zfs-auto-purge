from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast
import random
import string
import logging

from ..zfs import LocalZfsCli, ZfsProperty
from .arguments import Args


log = logging.getLogger(__name__)


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  if not args.dataset:
    raise ValueError(f"No dataset provided")

  cli = LocalZfsCli()
  
  # generate random 10 digit alnum string
  #   10 digit alnum -> (26+26+10)^10 values = 839299365868340224 values = ca. 59.5 bit
  #   ZFS GUID (64 bits) -> 2^64 values = 18446744073709551616 values
  chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
  shortname: str = ''.join(random.choices(chars, k=10))
  fullname = f'{args.dataset}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=args.recursive,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(args.tag)
    }
  )

  log.info(f'Created snapshot {fullname}')
