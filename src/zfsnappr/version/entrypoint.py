from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast, Literal, Callable
import importlib.metadata

from .arguments import Args


TAG_SEPARATOR = "_"


def entrypoint(raw_args: Namespace) -> None:
  args = cast(Args, raw_args)

  version = importlib.metadata.version('zfsnappr')
  print(f"zfsnappr {version}")
