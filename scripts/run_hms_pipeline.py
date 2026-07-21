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
    parser.add_argument("--spark-master", help="Spark master, for example yarn or local[2]")
    parser.add_argument("--spark-deploy-mode", choices=["client", "cluster"], help="Spark deploy mode")
    parser.add_argument("--spark-queue", help="YARN resource queue")
    parser.add_argument("--spark-driver-memory", help="driver memory, for example 2g")
    parser.add_argument("--spark-driver-cores", type=int, help="driver cores")
    parser.add_argument("--spark-executor-memory", help="executor memory, for example 4g")
    parser.add_argument("--spark-executor-cores", type=int, help="executor cores")
    parser.add_argument("--spark-num-executors", type=int, help="number of executors")
    parser.add_argument("--spark-app-name", help="Spark application name")
    parser.add_argument("--spark-archives", help="spark-submit --archives value, for example hdfs:///deps/venv.tar.gz#environment")
    parser.add_argument(
        "--package-current-venv",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="package the active virtualenv, upload it to HDFS, and pass it through spark-submit --archives",
    )
    parser.add_argument("--venv-path", help="virtualenv root to package; default uses VIRTUAL_ENV or the current Python executable")
    parser.add_argument("--venv-hdfs-dir", help="HDFS directory for the generated virtualenv archive; default hdfs:///deps")
    parser.add_argument("--venv-archive-name", help="generated virtualenv archive filename; default venv.tar.gz")
    parser.add_argument("--venv-archive-alias", help="spark --archives alias after #; default envir")
    parser.add_argument(
        "--overwrite-spark-output",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="delete hdfs.output_dir/mysql.database before spark-submit; default keeps Spark errorifexists behavior",
    )
    parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        help="spark-submit --conf value, for example spark.sql.shuffle.partitions=200; repeat for multiple values",
    )
    parser.add_argument("--env", help="DataHub env, for example UAT or PROD")
    parser.add_argument("--platform", help="DataHub platform, default hive")
    parser.add_argument("--platform-instance", help="DataHub platform_instance, default follows --platform")
    parser.add_argument("--database-pattern", help="Hive database regex")
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
    set_if_present(spark, "master", args.spark_master)
    set_if_present(spark, "deploy_mode", args.spark_deploy_mode)
    set_if_present(spark, "queue", args.spark_queue)
    set_if_present(spark, "driver_memory", args.spark_driver_memory)
    set_if_present(spark, "driver_cores", args.spark_driver_cores)
    set_if_present(spark, "executor_memory", args.spark_executor_memory)
    set_if_present(spark, "executor_cores", args.spark_executor_cores)
    set_if_present(spark, "num_executors", args.spark_num_executors)
    set_if_present(spark, "app_name", args.spark_app_name)
    set_if_present(spark, "archives", args.spark_archives)
    set_if_present(spark, "package_current_venv", args.package_current_venv)
    set_if_present(spark, "venv_path", args.venv_path)
    set_if_present(spark, "venv_hdfs_dir", args.venv_hdfs_dir)
    set_if_present(spark, "venv_archive_name", args.venv_archive_name)
    set_if_present(spark, "venv_archive_alias", args.venv_archive_alias)
    set_if_present(spark, "overwrite_output", args.overwrite_spark_output)
    if args.spark_conf:
        spark["conf"] = args.spark_conf
    set_if_present(spark, "env", args.env)
    set_if_present(spark, "platform", args.platform)
    set_if_present(spark, "platform_instance", args.platform_instance)
    set_if_present(spark, "database_pattern", args.database_pattern)
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
