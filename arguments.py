from __future__ import annotations
import argparse
from dateutil.relativedelta import relativedelta
from policy import parse_duration


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

def get_args():
  parser = argparse.ArgumentParser("simple_example")

  for opt in COUNT_OPTS:
    parser.add_argument(opt, type=int, metavar="N", default=0)

  for opt in WITHIN_OPTS:
    parser.add_argument(opt, type=parse_duration, metavar="DURATION", default=relativedelta())

  parser.add_argument('--dry-run', action='store_true')
  parser.add_argument('--dataset', type=str, metavar="DATASET", default=None)

  return parser.parse_args()
