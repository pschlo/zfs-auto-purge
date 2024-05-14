from __future__ import annotations
from typing import Protocol
from dataclasses import dataclass

@dataclass
class Args(Protocol):
  dataset: str
  recursive: bool
  dry_run: bool
