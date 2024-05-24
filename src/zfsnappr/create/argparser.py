from argparse import ArgumentParser


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('-t', '--tag', action='append', default=[])
