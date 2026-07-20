from pathlib import Path
from typing import Sequence
from unittest.mock import patch

import pytest

from hms_export.pipeline import (
    delete_hdfs_snapshot,
    delete_local_snapshot,
    join_hdfs_path,
    run_pipeline,
    spark_submit_transform,
    upload_snapshot_to_hdfs,
    validate_pipeline_config,
)
from hms_export.snapshot import DEFAULT_HMS_TABLES


class RecordingRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: Sequence[str]):
        self.commands.append(list(command))
        return None


def minimal_config(tmp_path: Path) -> dict:
    return {
        "mysql": {"host": "127.0.0.1", "database": "hive", "username": "hive"},
        "snapshot": {"output_dir": str(tmp_path / "snapshots")},
        "hdfs": {"snapshot_dir": "hdfs:///snapshots", "output_dir": "hdfs:///outputs"},
        "spark": {},
    }


def test_join_hdfs_path_normalizes_slashes() -> None:
    assert join_hdfs_path("hdfs:///tmp/snapshots/", "snapshot-1") == "hdfs:///tmp/snapshots/snapshot-1"


def test_upload_snapshot_to_hdfs_builds_hdfs_commands(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    target = upload_snapshot_to_hdfs(
        local_snapshot,
        "hdfs:///tmp/snapshots",
        hdfs_bin="hdfs",
        overwrite=True,
        runner=runner,
    )
    assert target == "hdfs:///tmp/snapshots/snapshot-1"
    assert runner.commands == [
        ["hdfs", "dfs", "-mkdir", "-p", "hdfs:///tmp/snapshots"],
        ["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///tmp/snapshots/snapshot-1"],
        ["hdfs", "dfs", "-put", str(local_snapshot), "hdfs:///tmp/snapshots"],
    ]


def test_delete_local_snapshot_removes_snapshot_directory(tmp_path: Path) -> None:
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    (local_snapshot / "manifest.json").write_text("{}\n")
    delete_local_snapshot(local_snapshot)
    assert not local_snapshot.exists()


def test_delete_hdfs_snapshot_builds_hdfs_rm_command() -> None:
    runner = RecordingRunner()
    delete_hdfs_snapshot("hdfs:///tmp/snapshots/snapshot-1", hdfs_bin="hdfs", runner=runner)
    assert runner.commands == [["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///tmp/snapshots/snapshot-1"]]


def test_spark_submit_transform_builds_command() -> None:
    runner = RecordingRunner()
    spark_submit_transform(
        "hdfs:///tmp/snapshots/snapshot-1",
        "hdfs:///tmp/output/snapshot-1",
        spark_submit="spark-submit",
        job_file=Path("jobs/transform_hms.py"),
        env="UAT",
        platform="hive",
        database_pattern="^hive$",
        metastore_name="hive_metastore_test",
        source_timezone="Asia/Shanghai",
        single_file=True,
        max_file_size=None,
        master="yarn",
        deploy_mode="cluster",
        queue="root.datahub",
        driver_memory="2g",
        driver_cores=1,
        executor_memory="4g",
        executor_cores=2,
        num_executors=8,
        app_name="hms-export-test",
        spark_conf=["spark.sql.shuffle.partitions=16"],
        spark_args=["--verbose"],
        runner=runner,
    )
    assert runner.commands == [[
        "spark-submit",
        "--master",
        "yarn",
        "--deploy-mode",
        "cluster",
        "--queue",
        "root.datahub",
        "--driver-memory",
        "2g",
        "--driver-cores",
        "1",
        "--executor-memory",
        "4g",
        "--executor-cores",
        "2",
        "--num-executors",
        "8",
        "--name",
        "hms-export-test",
        "--conf",
        "spark.sql.shuffle.partitions=16",
        "--verbose",
        "jobs/transform_hms.py",
        "--snapshot",
        "hdfs:///tmp/snapshots/snapshot-1",
        "--output",
        "hdfs:///tmp/output/snapshot-1",
        "--env",
        "UAT",
        "--platform",
        "hive",
        "--database-pattern",
        "^hive$",
        "--metastore-name",
        "hive_metastore_test",
        "--source-timezone",
        "Asia/Shanghai",
        "--single-file",
    ]]


def test_run_pipeline_requires_hdfs_config() -> None:
    with pytest.raises(ValueError, match="hdfs.snapshot_dir"):
        run_pipeline({"mysql": {}, "snapshot": {}}, runner=RecordingRunner())


def test_run_pipeline_requires_output_when_hdfs_is_disabled() -> None:
    with pytest.raises(ValueError, match="hdfs.output_dir"):
        run_pipeline({"mysql": {}, "snapshot": {}, "hdfs": {"enabled": False}}, runner=RecordingRunner())


def test_validate_pipeline_config_requires_mysql_snapshot_and_hdfs(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    validate_pipeline_config(config)
    config["mysql"].pop("host")
    with pytest.raises(ValueError, match="mysql missing"):
        validate_pipeline_config(config)


def test_validate_pipeline_config_rejects_invalid_max_file_size(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    config["spark"]["max_file_size"] = "nope"
    with pytest.raises(ValueError, match="invalid byte size"):
        validate_pipeline_config(config)


def test_validate_pipeline_config_rejects_single_file_with_max_file_size(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    config["spark"]["single_file"] = True
    config["spark"]["max_file_size"] = "20M"
    with pytest.raises(ValueError, match="single_file"):
        validate_pipeline_config(config)


def test_validate_pipeline_config_rejects_invalid_spark_resource_counts(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    config["spark"]["num_executors"] = 0
    with pytest.raises(ValueError, match="num_executors"):
        validate_pipeline_config(config)


def test_run_pipeline_orchestrates_snapshot_upload_and_spark(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    config = minimal_config(tmp_path)
    config["spark"] = {
        "env": "UAT",
        "master": "yarn",
        "queue": "root.datahub",
        "driver_memory": "2g",
        "executor_memory": "4g",
        "args": ["--verbose"],
        "max_file_size": "20k",
    }
    with patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot) as create_snapshot:
        result = run_pipeline(config, snapshot_id="snapshot-1", runner=runner)
    assert create_snapshot.call_args.args[0]["snapshot"]["tables"] == list(DEFAULT_HMS_TABLES)
    assert result.local_snapshot == local_snapshot
    assert result.hdfs_snapshot == "hdfs:///snapshots/snapshot-1"
    assert result.hdfs_output == "hdfs:///outputs/snapshot-1"
    assert runner.commands[0] == ["hdfs", "dfs", "-mkdir", "-p", "hdfs:///snapshots"]
    assert runner.commands[1] == ["hdfs", "dfs", "-put", str(local_snapshot), "hdfs:///snapshots"]
    assert not local_snapshot.exists()
    assert runner.commands[2][:11] == [
        "spark-submit",
        "--master",
        "yarn",
        "--queue",
        "root.datahub",
        "--driver-memory",
        "2g",
        "--executor-memory",
        "4g",
        "--verbose",
        "jobs/transform_hms.py",
    ]
    assert runner.commands[2][-2:] == ["--max-file-size", "20k"]
    assert runner.commands[3] == ["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///snapshots/snapshot-1"]


def test_run_pipeline_can_skip_hdfs(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    config = minimal_config(tmp_path)
    config["hdfs"] = {"enabled": False, "output_dir": str(tmp_path / "outputs")}
    config["spark"] = {"env": "UAT"}
    with patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot):
        result = run_pipeline(config, snapshot_id="snapshot-1", runner=runner)
    assert result.hdfs_snapshot == str(local_snapshot)
    assert result.hdfs_output == str(tmp_path / "outputs" / "snapshot-1")
    assert local_snapshot.exists()
    assert runner.commands[0][0] == "spark-submit"
