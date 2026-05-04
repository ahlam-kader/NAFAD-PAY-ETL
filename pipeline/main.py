"""CLI entrypoint: `python -m pipeline.main {init|bronze|silver|gold|all|snapshot}`."""
from __future__ import annotations

import argparse
import json
import sys

from . import bronze, gold, init_db, silver, snapshot
from .common.logging_setup import get_logger

log = get_logger(__name__)

STEPS = {
    "init": init_db.run,
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
}


def cmd_all() -> None:
    init_db.run()
    bronze.run()
    silver.run()
    gold.run()


def cmd_snapshot() -> None:
    snap = snapshot.compute()
    json.dump(snap, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="NAFAD-PAY G3 Medallion pipeline")
    parser.add_argument(
        "step",
        choices=list(STEPS) + ["all", "snapshot"],
        help="which stage to run",
    )
    args = parser.parse_args()

    if args.step == "all":
        cmd_all()
    elif args.step == "snapshot":
        cmd_snapshot()
    else:
        STEPS[args.step]()


if __name__ == "__main__":
    main()
