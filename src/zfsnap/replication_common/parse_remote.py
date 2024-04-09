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

  return user, host, dataset
