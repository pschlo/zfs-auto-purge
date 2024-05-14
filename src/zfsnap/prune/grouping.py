from typing import Optional, Any
from collections.abc import Collection, Callable
from enum import Enum, auto, StrEnum

from ..zfs import Snapshot


class GroupType(StrEnum):
  DATASET = 'dataset'


GET_GROUP: dict[GroupType, Callable[[Snapshot], str]] = {
  GroupType.DATASET: (lambda s: s.dataset)
}
