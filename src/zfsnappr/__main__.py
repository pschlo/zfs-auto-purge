#!/usr/bin/env python3
from .entrypoint import entrypoint
from .setup_logging import setup_logging


setup_logging()

entrypoint()
