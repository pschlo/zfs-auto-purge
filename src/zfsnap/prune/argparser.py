from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from argparse import ArgumentParser

from .policy import parse_duration


COUNT_OPTS = [
  "--keep-last",
  "--keep-hourly",
  "--keep-daily",
  "--keep-weekly",
  "--keep-monthly",
  "--keep-yearly"
]

WITHIN_OPTS = [
  "--keep-within",
  "--keep-within-hourly",
  "--keep-within-daily",
  "--keep-within-weekly",
  "--keep-within-monthly",
  "--keep-within-yearly"
]


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('--tag', type=str, action='append', default=[])

  # keep policy arguments
  for opt in COUNT_OPTS:
    parser.add_argument(opt, type=int, metavar="N", default=0)
  for opt in WITHIN_OPTS:
    parser.add_argument(opt, type=parse_duration, metavar="DURATION", default=relativedelta())
  parser.add_argument('--keep-name', type=re.compile, metavar="REGEX", default=None)
  parser.add_argument('--group-by', type=str, metavar='GROUP', choices={'', 'dataset'}, default='dataset')
  parser.add_argument('--keep-tag', type=str, action='append', default=[])
