from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  set_from_prop: str
  set_from_name: bool
  snapshot: list[str]
