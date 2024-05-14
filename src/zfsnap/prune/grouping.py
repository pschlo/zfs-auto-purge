from typing import Optional, Any
from collections.abc import Callable
from enum import StrEnum

from ..zfs import Snapshot


class GroupType(StrEnum):
  DATASET = 'dataset'


GET_GROUP: dict[GroupType, Callable[[Snapshot], str]] = {
  GroupType.DATASET: (lambda s: s.dataset)
}
