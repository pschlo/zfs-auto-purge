from __future__ import annotations
from argparse import ArgumentParser
from dataclasses import dataclass

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  ...
