from __future__ import annotations

import subprocess
import os
import shutil
import sys
import tarfile
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
    hdfs_archive: Optional[str] = None


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


def hdfs_parent(path: str) -> str:
    cleaned = path.rstrip("/")
    parent, separator, name = cleaned.rpartition("/")
    if not separator or not parent or not name:
        raise ValueError(f"invalid HDFS file path: {path}")
    return parent


def hdfs_basename(path: str) -> str:
    cleaned = path.rstrip("/")
    _, separator, name = cleaned.rpartition("/")
    if not separator or not name:
        raise ValueError(f"invalid HDFS file path: {path}")
    return name


def split_archive_spec(archives: str) -> tuple[str, Optional[str]]:
    archive_path, separator, alias = archives.partition("#")
    if not archive_path:
        raise ValueError("spark.archives path must not be empty")
    return archive_path, alias if separator and alias else None


def current_virtualenv_root() -> Path:
    virtual_env = os.environ.get("VIRTUAL_ENV")
    if virtual_env:
        return Path(virtual_env).resolve()

    if sys.prefix != getattr(sys, "base_prefix", sys.prefix):
        return Path(sys.prefix).resolve()

    executable = Path(sys.executable).resolve()
    candidate = executable.parent.parent if executable.parent.name == "bin" else executable.parent
    if (candidate / "pyvenv.cfg").exists():
        return candidate
    raise ValueError("cannot determine current virtualenv; activate it or set spark.venv_path")


def package_virtualenv(
    archive_path: Path,
    *,
    venv_root: Optional[Path] = None,
) -> Path:
    root = (venv_root or current_virtualenv_root()).resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"virtualenv root does not exist: {root}")
    if not (root / "bin" / "python").exists():
        raise ValueError(f"virtualenv root does not look valid: {root}")

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        for child in sorted(root.iterdir(), key=lambda path: path.name):
            archive.add(child, arcname=child.name)
    return archive_path


def upload_file_to_hdfs(
    local_file: Path,
    hdfs_dir: str,
    hdfs_bin: str = "hdfs",
    overwrite: bool = False,
    runner: CommandRunner = default_runner,
) -> str:
    target = join_hdfs_path(hdfs_dir, local_file.name)
    runner([hdfs_bin, "dfs", "-mkdir", "-p", hdfs_dir])
    if overwrite:
        runner([hdfs_bin, "dfs", "-rm", "-f", target])
    runner([hdfs_bin, "dfs", "-put", str(local_file), hdfs_dir])
    return target


def delete_local_snapshot(local_snapshot: Path) -> None:
    if local_snapshot.exists():
        shutil.rmtree(local_snapshot)


def delete_local_file(local_file: Path) -> None:
    local_file.unlink(missing_ok=True)


def delete_hdfs_snapshot(
    hdfs_snapshot: str,
    hdfs_bin: str = "hdfs",
    runner: CommandRunner = default_runner,
) -> None:
    runner([hdfs_bin, "dfs", "-rm", "-r", "-f", hdfs_snapshot])


def delete_hdfs_file(
    hdfs_file: str,
    hdfs_bin: str = "hdfs",
    runner: CommandRunner = default_runner,
) -> None:
    runner([hdfs_bin, "dfs", "-rm", "-f", hdfs_file])


def delete_local_output(local_output: str) -> None:
    path = Path(local_output)
    if path.exists():
        shutil.rmtree(path)


def delete_hdfs_output(
    hdfs_output: str,
    hdfs_bin: str = "hdfs",
    runner: CommandRunner = default_runner,
) -> None:
    runner([hdfs_bin, "dfs", "-rm", "-r", "-f", hdfs_output])


def chmod_hdfs_output(
    hdfs_output: str,
    hdfs_bin: str = "hdfs",
    runner: CommandRunner = default_runner,
) -> None:
    runner([hdfs_bin, "dfs", "-chmod", "-R", "777", hdfs_output])


def spark_submit_transform(
    snapshot: str,
    output: str,
    *,
    spark_submit: str = "spark-submit",
    job_file: Path = Path("jobs/transform_hms.py"),
    env: str = "PROD",
    platform: str = "hive",
    platform_instance: Optional[str] = None,
    database_pattern: str = ".*",
    metastore_name: str = "hive_metastore",
    source_timezone: str = "UTC",
    single_file: bool = False,
    max_file_size: Optional[str] = None,
    master: Optional[str] = None,
    deploy_mode: Optional[str] = None,
    queue: Optional[str] = None,
    driver_memory: Optional[str] = None,
    driver_cores: Optional[int] = None,
    executor_memory: Optional[str] = None,
    executor_cores: Optional[int] = None,
    num_executors: Optional[int] = None,
    app_name: Optional[str] = None,
    archives: Optional[str] = None,
    spark_conf: Sequence[str] = (),
    spark_args: Sequence[str] = (),
    runner: CommandRunner = default_runner,
) -> None:
    submit_options: List[str] = []
    option_pairs = [
        ("--master", master),
        ("--deploy-mode", deploy_mode),
        ("--queue", queue),
        ("--driver-memory", driver_memory),
        ("--driver-cores", driver_cores),
        ("--executor-memory", executor_memory),
        ("--executor-cores", executor_cores),
        ("--num-executors", num_executors),
        ("--name", app_name),
        ("--archives", archives),
    ]
    for option, value in option_pairs:
        if value is not None:
            submit_options.extend([option, str(value)])
    for item in spark_conf:
        submit_options.extend(["--conf", str(item)])

    command: List[str] = [
        spark_submit,
        *submit_options,
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
        "--platform-instance",
        platform_instance or platform,
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
    if spark.get("conf") is not None and not isinstance(spark["conf"], list):
        raise ValueError("spark.conf must be a list")
    if spark.get("package_current_venv") and not bool(hdfs.get("enabled", True)):
        raise ValueError("spark.package_current_venv requires hdfs.enabled")
    metastore_name = str(mysql.get("database", "")).strip()
    if not metastore_name:
        raise ValueError("mysql.database must not be empty")
    if "/" in metastore_name:
        raise ValueError("mysql.database must not contain '/'")
    for key in ("driver_cores", "executor_cores", "num_executors"):
        if spark.get(key) is not None and int(spark[key]) <= 0:
            raise ValueError(f"spark.{key} must be positive")
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
        hdfs_bin = str(hdfs.get("bin", "hdfs"))
        hdfs_snapshot = upload_snapshot_to_hdfs(
            local_snapshot,
            str(hdfs_snapshot_dir),
            hdfs_bin,
            bool(hdfs.get("overwrite", False) if overwrite_hdfs is None else overwrite_hdfs),
            runner,
        )
        delete_local_snapshot(local_snapshot)
    else:
        hdfs_snapshot = str(local_snapshot)
    metastore_name = str(_section(config, "mysql")["database"])
    spark_output = join_hdfs_path(str(hdfs_output), metastore_name)
    spark_archives = str(spark["archives"]) if spark.get("archives") is not None else None
    hdfs_archive: Optional[str] = None
    if spark.get("package_current_venv"):
        hdfs_bin = str(hdfs.get("bin", "hdfs"))
        alias = str(spark.get("venv_archive_alias", "envir"))
        if spark_archives:
            hdfs_archive, configured_alias = split_archive_spec(spark_archives)
            alias = configured_alias or alias
        else:
            hdfs_archive = join_hdfs_path(
                str(spark.get("venv_hdfs_dir", "hdfs:///deps")),
                str(spark.get("venv_archive_name", "venv.tar.gz")),
            )
        spark_archives = f"{hdfs_archive}#{alias}"
        local_archive = local_snapshot.parent / hdfs_basename(hdfs_archive)
        venv_root = Path(spark["venv_path"]) if spark.get("venv_path") is not None else None
        package_virtualenv(local_archive, venv_root=venv_root)
        upload_file_to_hdfs(
            local_archive,
            hdfs_parent(hdfs_archive),
            hdfs_bin,
            bool(hdfs.get("overwrite", False) if overwrite_hdfs is None else overwrite_hdfs),
            runner,
        )
        delete_local_file(local_archive)
    if bool(spark.get("overwrite_output", False)):
        if hdfs_enabled:
            delete_hdfs_output(spark_output, hdfs_bin, runner)
        else:
            delete_local_output(spark_output)
    spark_submit_transform(
        hdfs_snapshot,
        spark_output,
        spark_submit=str(spark.get("submit", "spark-submit")),
        job_file=Path(spark.get("job_file", "jobs/transform_hms.py")),
        env=str(spark.get("env", "PROD")),
        platform=str(spark.get("platform", "hive")),
        platform_instance=str(spark.get("platform_instance", spark.get("platform", "hive"))),
        database_pattern=str(spark.get("database_pattern", ".*")),
        metastore_name=metastore_name,
        source_timezone=str(spark.get("source_timezone", "UTC")),
        single_file=bool(spark.get("single_file", False)),
        max_file_size=str(spark["max_file_size"]) if spark.get("max_file_size") is not None else None,
        master=str(spark["master"]) if spark.get("master") is not None else None,
        deploy_mode=str(spark["deploy_mode"]) if spark.get("deploy_mode") is not None else None,
        queue=str(spark["queue"]) if spark.get("queue") is not None else None,
        driver_memory=str(spark["driver_memory"]) if spark.get("driver_memory") is not None else None,
        driver_cores=int(spark["driver_cores"]) if spark.get("driver_cores") is not None else None,
        executor_memory=str(spark["executor_memory"]) if spark.get("executor_memory") is not None else None,
        executor_cores=int(spark["executor_cores"]) if spark.get("executor_cores") is not None else None,
        num_executors=int(spark["num_executors"]) if spark.get("num_executors") is not None else None,
        app_name=str(spark["app_name"]) if spark.get("app_name") is not None else None,
        archives=spark_archives,
        spark_conf=[str(item) for item in spark.get("conf", [])],
        spark_args=[str(item) for item in spark.get("args", [])],
        runner=runner,
    )
    if hdfs_enabled:
        chmod_hdfs_output(spark_output, hdfs_bin, runner)
        delete_hdfs_snapshot(hdfs_snapshot, hdfs_bin, runner)
        if hdfs_archive is not None:
            delete_hdfs_file(hdfs_archive, hdfs_bin, runner)

    result = PipelineResult(local_snapshot, hdfs_snapshot, spark_output, hdfs_archive)
    print(
        f"local_snapshot={result.local_snapshot} "
        f"hdfs_snapshot={result.hdfs_snapshot} "
        f"hdfs_output={result.hdfs_output} "
        f"hdfs_archive={result.hdfs_archive}"
    )
    return result
