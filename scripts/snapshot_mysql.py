#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hms_export.snapshot import create_snapshot, dump_table, iter_rows, load_config  # noqa: E402,F401


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Hive Metastore tables to a snapshot")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--snapshot-id", help="default: UTC timestamp")
    args = parser.parse_args()
    create_snapshot(load_config(args.config), args.snapshot_id)


if __name__ == "__main__":
    main()
