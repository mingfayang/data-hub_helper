#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hms_export.pipeline import run_pipeline  # noqa: E402
from hms_export.snapshot import load_config  # noqa: E402


def ensure_section(config: Dict[str, Any], name: str) -> Dict[str, Any]:
    value = config.setdefault(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"{name} section must be a mapping")
    return value


def set_if_present(section: Dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        section[key] = value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create an HMS snapshot, upload it to HDFS, and run the Spark transform job"
    )
    parser.add_argument("--config", type=Path, help="optional YAML config; CLI values override it")
    parser.add_argument("--snapshot-id", help="default: UTC timestamp")

    parser.add_argument("--mysql-host")
    parser.add_argument("--mysql-port", type=int)
    parser.add_argument("--mysql-database")
    parser.add_argument("--mysql-username")
    parser.add_argument("--mysql-password-env")
    parser.add_argument("--mysql-connect-timeout-seconds", type=int)
    parser.add_argument("--mysql-read-timeout-seconds", type=int)
    parser.add_argument(
        "--consistent-snapshot",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="use a MySQL repeatable-read consistent snapshot",
    )

    parser.add_argument("--snapshot-output-dir", type=Path)
    parser.add_argument("--snapshot-rows-per-file", type=int)
    parser.add_argument("--snapshot-fetch-size", type=int)

    parser.add_argument("--hdfs-bin", help="hdfs executable")
    parser.add_argument(
        "--hdfs-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="upload the snapshot with hdfs dfs before running Spark",
    )
    parser.add_argument("--hdfs-snapshot-dir", help="HDFS parent directory for uploaded snapshots")
    parser.add_argument("--hdfs-output", help="HDFS parent directory for Spark outputs")
    parser.add_argument(
        "--overwrite-hdfs",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="replace an existing HDFS snapshot before upload",
    )

    parser.add_argument("--spark-submit", help="spark-submit executable")
    parser.add_argument("--spark-job-file", type=Path, help="Spark job script path")
    parser.add_argument("--env", help="DataHub env, for example UAT or PROD")
    parser.add_argument("--platform", help="DataHub platform, default hive")
    parser.add_argument("--database-pattern", help="Hive database regex")
    parser.add_argument("--metastore-name", help="Root metastore container name")
    parser.add_argument("--source-timezone", help="Hive metastore source timezone")
    parser.add_argument(
        "--single-file",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="coalesce Spark output to one part file",
    )
    parser.add_argument(
        "--max-file-size",
        help="target maximum Spark output file size, for example 20k, 20M, or 1G",
    )
    parser.add_argument(
        "--spark-arg",
        action="append",
        default=[],
        help="extra spark-submit argument; repeat for multiple values",
    )

    args = parser.parse_args()

    config: Dict[str, Any] = load_config(args.config) if args.config else {"mysql": {}, "snapshot": {}}
    mysql = ensure_section(config, "mysql")
    snapshot = ensure_section(config, "snapshot")
    hdfs = ensure_section(config, "hdfs")
    spark = ensure_section(config, "spark")

    set_if_present(mysql, "host", args.mysql_host)
    set_if_present(mysql, "port", args.mysql_port)
    set_if_present(mysql, "database", args.mysql_database)
    set_if_present(mysql, "username", args.mysql_username)
    set_if_present(mysql, "password_env", args.mysql_password_env)
    set_if_present(mysql, "connect_timeout_seconds", args.mysql_connect_timeout_seconds)
    set_if_present(mysql, "read_timeout_seconds", args.mysql_read_timeout_seconds)
    set_if_present(mysql, "consistent_snapshot", args.consistent_snapshot)

    set_if_present(snapshot, "output_dir", str(args.snapshot_output_dir) if args.snapshot_output_dir else None)
    set_if_present(snapshot, "rows_per_file", args.snapshot_rows_per_file)
    set_if_present(snapshot, "fetch_size", args.snapshot_fetch_size)

    set_if_present(hdfs, "bin", args.hdfs_bin)
    set_if_present(hdfs, "enabled", args.hdfs_enabled)
    set_if_present(hdfs, "snapshot_dir", args.hdfs_snapshot_dir)
    set_if_present(hdfs, "output_dir", args.hdfs_output)
    set_if_present(hdfs, "overwrite", args.overwrite_hdfs)

    set_if_present(spark, "submit", args.spark_submit)
    set_if_present(spark, "job_file", str(args.spark_job_file) if args.spark_job_file else None)
    set_if_present(spark, "env", args.env)
    set_if_present(spark, "platform", args.platform)
    set_if_present(spark, "database_pattern", args.database_pattern)
    set_if_present(spark, "metastore_name", args.metastore_name)
    set_if_present(spark, "source_timezone", args.source_timezone)
    set_if_present(spark, "single_file", args.single_file)
    set_if_present(spark, "max_file_size", args.max_file_size)
    if args.spark_arg:
        spark["args"] = args.spark_arg

    run_pipeline(
        config,
        snapshot_id=args.snapshot_id,
        hdfs_snapshot_dir=args.hdfs_snapshot_dir,
        hdfs_output=args.hdfs_output,
        overwrite_hdfs=args.overwrite_hdfs,
    )


if __name__ == "__main__":
    main()
