from __future__ import annotations
from typing import Protocol, Optional
from dataclasses import dataclass

@dataclass
class Args(Protocol):
  dataset: Optional[str]
  recursive: bool
  dry_run: bool
