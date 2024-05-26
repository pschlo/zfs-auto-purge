from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  # filter options
  tag: list[str]

  # extraction options
  set_from_prop: Optional[str]
  add_from_prop: Optional[str]
  set_from_name: bool
  add_from_name: bool

  snapshot: list[str]
