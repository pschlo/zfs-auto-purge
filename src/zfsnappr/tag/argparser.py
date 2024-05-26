from argparse import ArgumentParser


def setup(parser: ArgumentParser) -> None:
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--set-from-prop')
  group.add_argument('--set-from-name', action='store_true')

  parser.add_argument('--add-from-prop')
  parser.add_argument('--add-from-name', action='store_true')

  parser.add_argument('snapshot', nargs='*', type=str)
