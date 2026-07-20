from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from hms_export.common import parse_byte_size
from hms_export.snapshot import DEFAULT_HMS_TABLES, create_snapshot


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class PipelineResult:
    local_snapshot: Path
    hdfs_snapshot: str
    hdfs_output: str


def default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, check=True)


def join_hdfs_path(parent: str, child: str) -> str:
    return f"{parent.rstrip('/')}/{child}"


def upload_snapshot_to_hdfs(
    local_snapshot: Path,
    hdfs_snapshot_dir: str,
    hdfs_bin: str = "hdfs",
    overwrite: bool = False,
    runner: CommandRunner = default_runner,
) -> str:
    target = join_hdfs_path(hdfs_snapshot_dir, local_snapshot.name)
    runner([hdfs_bin, "dfs", "-mkdir", "-p", hdfs_snapshot_dir])
    if overwrite:
        runner([hdfs_bin, "dfs", "-rm", "-r", "-f", target])
    runner([hdfs_bin, "dfs", "-put", str(local_snapshot), hdfs_snapshot_dir])
    return target


def spark_submit_transform(
    snapshot: str,
    output: str,
    *,
    spark_submit: str = "spark-submit",
    job_file: Path = Path("jobs/transform_hms.py"),
    env: str = "PROD",
    platform: str = "hive",
    database_pattern: str = ".*",
    metastore_name: str = "hive_metastore",
    source_timezone: str = "UTC",
    single_file: bool = False,
    max_file_size: Optional[str] = None,
    spark_args: Sequence[str] = (),
    runner: CommandRunner = default_runner,
) -> None:
    command: List[str] = [
        spark_submit,
        *spark_args,
        str(job_file),
        "--snapshot",
        snapshot,
        "--output",
        output,
        "--env",
        env,
        "--platform",
        platform,
        "--database-pattern",
        database_pattern,
        "--metastore-name",
        metastore_name,
        "--source-timezone",
        source_timezone,
    ]
    if single_file:
        command.append("--single-file")
    if max_file_size is not None:
        command.extend(["--max-file-size", max_file_size])
    runner(command)


def _section(config: Mapping[str, Any], name: str) -> Dict[str, Any]:
    value = config.get(name, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{name} section must be a mapping")
    return dict(value)


def validate_pipeline_config(config: Mapping[str, Any]) -> None:
    mysql = _section(config, "mysql")
    snapshot = _section(config, "snapshot")
    hdfs = _section(config, "hdfs")
    spark = _section(config, "spark")

    required_mysql = ["host", "database", "username"]
    missing_mysql = [key for key in required_mysql if not mysql.get(key)]
    if missing_mysql:
        raise ValueError(f"mysql missing required fields: {', '.join(missing_mysql)}")
    if not snapshot.get("output_dir"):
        raise ValueError("snapshot.output_dir is required")
    if int(snapshot.get("rows_per_file", 200000)) <= 0:
        raise ValueError("snapshot.rows_per_file must be positive")
    if int(snapshot.get("fetch_size", 5000)) <= 0:
        raise ValueError("snapshot.fetch_size must be positive")
    if bool(hdfs.get("enabled", True)) and not hdfs.get("snapshot_dir"):
        raise ValueError("hdfs.snapshot_dir or --hdfs-snapshot-dir is required")
    if not hdfs.get("output_dir"):
        raise ValueError("hdfs.output_dir or --hdfs-output is required")
    if spark.get("args") is not None and not isinstance(spark["args"], list):
        raise ValueError("spark.args must be a list")
    if spark.get("max_file_size") is not None:
        parse_byte_size(str(spark["max_file_size"]))
    if spark.get("single_file") and spark.get("max_file_size") is not None:
        raise ValueError("spark.single_file cannot be used with spark.max_file_size")


def run_pipeline(
    config: Mapping[str, Any],
    *,
    snapshot_id: Optional[str] = None,
    hdfs_snapshot_dir: Optional[str] = None,
    hdfs_output: Optional[str] = None,
    overwrite_hdfs: Optional[bool] = None,
    runner: CommandRunner = default_runner,
) -> PipelineResult:
    config = dict(config)
    hdfs = _section(config, "hdfs")
    spark = _section(config, "spark")
    hdfs_enabled = bool(hdfs.get("enabled", True))
    hdfs_snapshot_dir = hdfs_snapshot_dir or hdfs.get("snapshot_dir")
    hdfs_output = hdfs_output or hdfs.get("output_dir")
    if hdfs_enabled and not hdfs_snapshot_dir:
        raise ValueError("hdfs.snapshot_dir or --hdfs-snapshot-dir is required")
    if not hdfs_output:
        raise ValueError("hdfs.output_dir or --hdfs-output is required")
    hdfs["snapshot_dir"] = hdfs_snapshot_dir
    hdfs["output_dir"] = hdfs_output
    if overwrite_hdfs is not None:
        hdfs["overwrite"] = overwrite_hdfs
    config["hdfs"] = hdfs
    snapshot = _section(config, "snapshot")
    snapshot["tables"] = list(snapshot.get("tables") or DEFAULT_HMS_TABLES)
    config["snapshot"] = snapshot
    validate_pipeline_config(config)

    local_snapshot = create_snapshot(config, snapshot_id)
    if hdfs_enabled:
        hdfs_snapshot = upload_snapshot_to_hdfs(
            local_snapshot,
            str(hdfs_snapshot_dir),
            str(hdfs.get("bin", "hdfs")),
            bool(hdfs.get("overwrite", False) if overwrite_hdfs is None else overwrite_hdfs),
            runner,
        )
    else:
        hdfs_snapshot = str(local_snapshot)
    spark_output = join_hdfs_path(hdfs_output, local_snapshot.name)
    spark_submit_transform(
        hdfs_snapshot,
        spark_output,
        spark_submit=str(spark.get("submit", "spark-submit")),
        job_file=Path(spark.get("job_file", "jobs/transform_hms.py")),
        env=str(spark.get("env", "PROD")),
        platform=str(spark.get("platform", "hive")),
        database_pattern=str(spark.get("database_pattern", ".*")),
        metastore_name=str(spark.get("metastore_name", "hive_metastore")),
        source_timezone=str(spark.get("source_timezone", "UTC")),
        single_file=bool(spark.get("single_file", False)),
        max_file_size=str(spark["max_file_size"]) if spark.get("max_file_size") is not None else None,
        spark_args=[str(item) for item in spark.get("args", [])],
        runner=runner,
    )

    result = PipelineResult(local_snapshot, hdfs_snapshot, spark_output)
    print(f"local_snapshot={result.local_snapshot} hdfs_snapshot={result.hdfs_snapshot} hdfs_output={result.hdfs_output}")
    return result
