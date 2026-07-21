import tarfile
from pathlib import Path
from typing import Sequence
from unittest.mock import patch

import pytest

from hms_export.pipeline import (
    chmod_hdfs_output,
    delete_hdfs_file,
    delete_hdfs_output,
    delete_hdfs_snapshot,
    delete_local_file,
    delete_local_output,
    delete_local_snapshot,
    hdfs_basename,
    hdfs_parent,
    join_hdfs_path,
    package_virtualenv,
    run_pipeline,
    spark_submit_transform,
    split_archive_spec,
    upload_file_to_hdfs,
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


def test_hdfs_file_path_helpers() -> None:
    assert hdfs_parent("hdfs:///deps/venv.tar.gz") == "hdfs:///deps"
    assert hdfs_basename("hdfs:///deps/venv.tar.gz") == "venv.tar.gz"
    assert split_archive_spec("hdfs:///deps/venv.tar.gz#envir") == ("hdfs:///deps/venv.tar.gz", "envir")
    assert split_archive_spec("hdfs:///deps/venv.tar.gz") == ("hdfs:///deps/venv.tar.gz", None)


def test_package_virtualenv_archives_contents_without_env_directory(tmp_path: Path) -> None:
    venv = tmp_path / "env"
    (venv / "bin").mkdir(parents=True)
    (venv / "lib").mkdir()
    (venv / "bin" / "python").write_text("#!/usr/bin/env python\n")
    (venv / "lib" / "site.py").write_text("# fake site\n")
    (venv / "pyvenv.cfg").write_text("home = /opt/python\n")

    archive = package_virtualenv(tmp_path / "venv.tar.gz", venv_root=venv)

    with tarfile.open(archive, "r:gz") as tar:
        names = set(tar.getnames())
    assert "bin" in names
    assert "bin/python" in names
    assert "lib/site.py" in names
    assert "pyvenv.cfg" in names
    assert "env" not in names
    assert "env/bin/python" not in names


def test_upload_file_to_hdfs_builds_hdfs_commands(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_archive = tmp_path / "venv.tar.gz"
    local_archive.write_text("archive")
    target = upload_file_to_hdfs(
        local_archive,
        "hdfs:///deps",
        hdfs_bin="hdfs",
        overwrite=True,
        runner=runner,
    )
    assert target == "hdfs:///deps/venv.tar.gz"
    assert runner.commands == [
        ["hdfs", "dfs", "-mkdir", "-p", "hdfs:///deps"],
        ["hdfs", "dfs", "-rm", "-f", "hdfs:///deps/venv.tar.gz"],
        ["hdfs", "dfs", "-put", str(local_archive), "hdfs:///deps"],
    ]


def test_delete_local_snapshot_removes_snapshot_directory(tmp_path: Path) -> None:
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    (local_snapshot / "manifest.json").write_text("{}\n")
    delete_local_snapshot(local_snapshot)
    assert not local_snapshot.exists()


def test_delete_local_file_removes_file(tmp_path: Path) -> None:
    local_file = tmp_path / "venv.tar.gz"
    local_file.write_text("archive")
    delete_local_file(local_file)
    assert not local_file.exists()


def test_delete_hdfs_snapshot_builds_hdfs_rm_command() -> None:
    runner = RecordingRunner()
    delete_hdfs_snapshot("hdfs:///tmp/snapshots/snapshot-1", hdfs_bin="hdfs", runner=runner)
    assert runner.commands == [["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///tmp/snapshots/snapshot-1"]]


def test_delete_hdfs_file_builds_hdfs_rm_command() -> None:
    runner = RecordingRunner()
    delete_hdfs_file("hdfs:///deps/venv.tar.gz", hdfs_bin="hdfs", runner=runner)
    assert runner.commands == [["hdfs", "dfs", "-rm", "-f", "hdfs:///deps/venv.tar.gz"]]


def test_delete_local_output_removes_output_directory(tmp_path: Path) -> None:
    output = tmp_path / "output-1"
    output.mkdir()
    (output / "part-00000").write_text("{}\n")
    delete_local_output(str(output))
    assert not output.exists()


def test_delete_hdfs_output_builds_hdfs_rm_command() -> None:
    runner = RecordingRunner()
    delete_hdfs_output("hdfs:///tmp/output/snapshot-1", hdfs_bin="hdfs", runner=runner)
    assert runner.commands == [["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///tmp/output/snapshot-1"]]


def test_chmod_hdfs_output_builds_hdfs_chmod_command() -> None:
    runner = RecordingRunner()
    chmod_hdfs_output("hdfs:///tmp/output/snapshot-1", hdfs_bin="hdfs", runner=runner)
    assert runner.commands == [["hdfs", "dfs", "-chmod", "-R", "777", "hdfs:///tmp/output/snapshot-1"]]


def test_spark_submit_transform_builds_command() -> None:
    runner = RecordingRunner()
    spark_submit_transform(
        "hdfs:///tmp/snapshots/snapshot-1",
        "hdfs:///tmp/output/snapshot-1",
        spark_submit="spark-submit",
        job_file=Path("jobs/transform_hms.py"),
        env="UAT",
        platform="hive",
        platform_instance="hive",
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
        archives="hdfs:///deps/venv.tar.gz#environment",
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
        "--archives",
        "hdfs:///deps/venv.tar.gz#environment",
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
        "--platform-instance",
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


def test_validate_pipeline_config_rejects_packaged_venv_without_hdfs(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    config["hdfs"] = {"enabled": False, "output_dir": str(tmp_path / "outputs")}
    config["spark"]["package_current_venv"] = True
    with pytest.raises(ValueError, match="package_current_venv"):
        validate_pipeline_config(config)


def test_validate_pipeline_config_rejects_invalid_mysql_database_for_output_dir(tmp_path: Path) -> None:
    config = minimal_config(tmp_path)
    config["mysql"]["database"] = "bad/name"
    with pytest.raises(ValueError, match="mysql.database"):
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
    assert result.hdfs_output == "hdfs:///outputs/hive"
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
    assert runner.commands[3] == ["hdfs", "dfs", "-chmod", "-R", "777", "hdfs:///outputs/hive"]
    assert runner.commands[4] == ["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///snapshots/snapshot-1"]


def test_run_pipeline_packages_current_venv_and_cleans_hdfs_archive(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    local_archive = tmp_path / "venv.tar.gz"
    config = minimal_config(tmp_path)
    config["hdfs"]["overwrite"] = True
    config["spark"] = {
        "env": "UAT",
        "package_current_venv": True,
        "archives": "hdfs:///deps/venv.tar.gz#envir",
        "conf": ["spark.pyspark.python=./envir/bin/python"],
    }

    def fake_package(archive_path: Path, *, venv_root: Path | None = None) -> Path:
        assert archive_path == local_archive
        assert venv_root is None
        archive_path.write_text("archive")
        return archive_path

    with (
        patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot),
        patch("hms_export.pipeline.package_virtualenv", side_effect=fake_package),
    ):
        result = run_pipeline(config, snapshot_id="snapshot-1", runner=runner)

    assert result.hdfs_archive == "hdfs:///deps/venv.tar.gz"
    assert not local_snapshot.exists()
    assert not local_archive.exists()
    assert runner.commands[3] == ["hdfs", "dfs", "-mkdir", "-p", "hdfs:///deps"]
    assert runner.commands[4] == ["hdfs", "dfs", "-rm", "-f", "hdfs:///deps/venv.tar.gz"]
    assert runner.commands[5] == ["hdfs", "dfs", "-put", str(local_archive), "hdfs:///deps"]
    spark_command = runner.commands[6]
    assert spark_command[spark_command.index("--archives") + 1] == "hdfs:///deps/venv.tar.gz#envir"
    assert spark_command[spark_command.index("--conf") + 1] == "spark.pyspark.python=./envir/bin/python"
    assert runner.commands[7] == ["hdfs", "dfs", "-chmod", "-R", "777", "hdfs:///outputs/hive"]
    assert runner.commands[8] == ["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///snapshots/snapshot-1"]
    assert runner.commands[9] == ["hdfs", "dfs", "-rm", "-f", "hdfs:///deps/venv.tar.gz"]


def test_run_pipeline_uses_mysql_database_for_fixed_spark_output(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    config = minimal_config(tmp_path)
    config["mysql"]["database"] = "prod_metastore"
    config["spark"] = {"env": "UAT"}
    with patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot):
        result = run_pipeline(config, snapshot_id="snapshot-1", runner=runner)
    assert result.hdfs_output == "hdfs:///outputs/prod_metastore"
    assert "--output" in runner.commands[2]
    assert runner.commands[2][runner.commands[2].index("--output") + 1] == "hdfs:///outputs/prod_metastore"
    assert runner.commands[2][runner.commands[2].index("--platform-instance") + 1] == "hive"
    assert runner.commands[2][runner.commands[2].index("--metastore-name") + 1] == "prod_metastore"


def test_run_pipeline_can_overwrite_spark_output(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    config = minimal_config(tmp_path)
    config["spark"] = {"env": "UAT", "overwrite_output": True}
    with patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot):
        run_pipeline(config, snapshot_id="snapshot-1", runner=runner)
    assert runner.commands[2] == ["hdfs", "dfs", "-rm", "-r", "-f", "hdfs:///outputs/hive"]
    assert runner.commands[3][0] == "spark-submit"


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
    assert result.hdfs_output == str(tmp_path / "outputs" / "hive")
    assert local_snapshot.exists()
    assert runner.commands[0][0] == "spark-submit"


def test_run_pipeline_can_overwrite_local_spark_output_when_hdfs_is_disabled(tmp_path: Path) -> None:
    runner = RecordingRunner()
    local_snapshot = tmp_path / "snapshot-1"
    local_snapshot.mkdir()
    output = tmp_path / "outputs" / "hive"
    output.mkdir(parents=True)
    (output / "part-00000").write_text("{}\n")
    config = minimal_config(tmp_path)
    config["hdfs"] = {"enabled": False, "output_dir": str(tmp_path / "outputs")}
    config["spark"] = {"env": "UAT", "overwrite_output": True}
    with patch("hms_export.pipeline.create_snapshot", return_value=local_snapshot):
        run_pipeline(config, snapshot_id="snapshot-1", runner=runner)
    assert not output.exists()
    assert runner.commands[0][0] == "spark-submit"
