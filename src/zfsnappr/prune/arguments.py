from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as GeneralArgs


@dataclass
class Args(GeneralArgs):
  tag: list[str]

  keep_last: int
  keep_hourly: int
  keep_daily: int
  keep_weekly: int
  keep_monthly: int
  keep_yearly: int

  keep_within: relativedelta
  keep_within_hourly: relativedelta
  keep_within_daily: relativedelta
  keep_within_weekly: relativedelta
  keep_within_monthly: relativedelta
  keep_within_yearly: relativedelta

  keep_name: re.Pattern
  group_by: str
  keep_tag: list[str]
