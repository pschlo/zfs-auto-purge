from __future__ import annotations
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  snapname: Optional[str]
  tag: list[str]
