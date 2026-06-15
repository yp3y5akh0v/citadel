"""Console-script entry point for the ``citadeldb`` command."""

import sys


def main() -> None:
    from citadeldb._core import cli_main

    raise SystemExit(cli_main(sys.argv))
