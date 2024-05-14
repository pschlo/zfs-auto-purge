from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  remote: str
  port: Optional[int]
