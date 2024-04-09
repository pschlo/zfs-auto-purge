from __future__ import annotations
from typing import Optional


def parse_remote(source: str) -> tuple[Optional[str], str, str]:
  user: Optional[str]
  host: str
  dataset: str

  netloc, dataset = source.split(':')
  if '@' in netloc:
    user, host = netloc.split('@')
  else:
    user = None
    host = netloc

  # validation
  if (
    user is not None and not user
    or not host
    or not dataset
  ):
    raise ValueError(f'Invalid remote')

  return user, host, dataset
