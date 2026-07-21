import gzip
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from hms_export.compare import load_records


def write_table(root: Path, name: str, rows: list) -> None:
    directory = root / name
    directory.mkdir()
    with gzip.open(directory / "part-00000.jsonl.gz", "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")


def create_snapshot(root: Path, complete: bool = True) -> None:
    root.mkdir()
    (root / "manifest.json").write_text(json.dumps({"complete": complete}, indent=2))
    write_table(root, "DBS", [
        {"DB_ID": 1, "NAME": "hive", "DESC": "main database", "DB_LOCATION_URI": "hdfs:///warehouse/hive", "OWNER_NAME": "dba"},
        {"DB_ID": 2, "NAME": "ignored", "DESC": "filtered database"},
    ])
    write_table(root, "TBLS", [
        {"TBL_ID": 10, "DB_ID": 1, "SD_ID": 20, "TBL_NAME": "orders", "TBL_TYPE": "EXTERNAL_TABLE", "OWNER": "alice", "CREATE_TIME": 100},
        {"TBL_ID": 11, "DB_ID": 1, "SD_ID": 21, "TBL_NAME": "order_view", "TBL_TYPE": "VIRTUAL_VIEW", "OWNER": None, "CREATE_TIME": 101, "VIEW_ORIGINAL_TEXT": "select id from orders"},
        {"TBL_ID": 12, "DB_ID": 2, "SD_ID": 22, "TBL_NAME": "not_exported", "TBL_TYPE": "MANAGED_TABLE"},
    ])
    write_table(root, "SDS", [
        {"SD_ID": 20, "CD_ID": 30, "LOCATION": "s3://bucket/orders", "INPUT_FORMAT": "parquet.input", "OUTPUT_FORMAT": "parquet.output"},
        {"SD_ID": 21, "CD_ID": 31, "LOCATION": "", "INPUT_FORMAT": "text.input", "OUTPUT_FORMAT": "text.output"},
        {"SD_ID": 22, "CD_ID": 32},
    ])
    write_table(root, "COLUMNS_V2", [
        {"CD_ID": 30, "COLUMN_NAME": "id", "TYPE_NAME": "bigint", "COMMENT": "order id", "INTEGER_IDX": 0},
        {"CD_ID": 30, "COLUMN_NAME": "amount", "TYPE_NAME": "decimal(10,2)", "COMMENT": None, "INTEGER_IDX": 1},
        {"CD_ID": 31, "COLUMN_NAME": "id", "TYPE_NAME": "bigint", "COMMENT": "", "INTEGER_IDX": 0},
        {"CD_ID": 32, "COLUMN_NAME": "x", "TYPE_NAME": "string", "INTEGER_IDX": 0},
    ])
    write_table(root, "PARTITION_KEYS", [
        {"TBL_ID": 10, "PKEY_NAME": "dt", "PKEY_TYPE": "date", "PKEY_COMMENT": "partition date", "INTEGER_IDX": 0},
    ])
    write_table(root, "TABLE_PARAMS", [
        {"TBL_ID": 10, "PARAM_KEY": "comment", "PARAM_VALUE": "orders table"},
        {"TBL_ID": 10, "PARAM_KEY": "classification", "PARAM_VALUE": "gold"},
        {"TBL_ID": 11, "PARAM_KEY": "comment", "PARAM_VALUE": "orders view"},
    ])


@pytest.fixture(scope="session")
def spark_submit() -> str:
    executable = shutil.which("spark-submit") or str(Path(sys.executable).parent / "spark-submit")
    if not Path(executable).is_file():
        executable = None
    if not executable:
        pytest.skip("spark-submit is not installed")
    return executable


def run_job(spark_submit: str, snapshot: Path, output: Path, *extra: str) -> subprocess.CompletedProcess:
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1")
    environment["PATH"] = f"{Path(sys.executable).parent}:{environment.get('PATH', '')}"
    environment["PYSPARK_PYTHON"] = sys.executable
    return subprocess.run(
        [spark_submit, "--master", "local[2]", str(root / "jobs/transform_hms.py"),
         "--snapshot", str(snapshot), "--output", str(output), "--env", "UAT",
         "--database-pattern", "^hive$", *extra],
        cwd=root, env=environment, text=True, capture_output=True, timeout=180,
    )


def test_transform_complete_snapshot_end_to_end(tmp_path: Path, spark_submit: str) -> None:
    snapshot, output = tmp_path / "snapshot", tmp_path / "output"
    create_snapshot(snapshot)
    result = run_job(spark_submit, snapshot, output)
    assert result.returncode == 0, result.stdout + result.stderr
    json_files = sorted(output.glob("mcp-*.json"))
    assert json_files
    assert not list(output.glob("part-*"))
    for file in json_files:
        parsed = json.loads(file.read_text())
        assert isinstance(parsed, list)
        assert all(isinstance(item, dict) for item in parsed)
    records = load_records(output)
    # 5 root-container + 6 schema-container + 6 per target dataset + one viewProperties.
    assert len(records) == 24
    by_key = {(record.urn, record.aspect_name): record.aspect for record in records}
    orders = "urn:li:dataset:(urn:li:dataPlatform:hive,hive.orders,UAT)"
    view = "urn:li:dataset:(urn:li:dataPlatform:hive,hive.order_view,UAT)"
    assert not any("not_exported" in record.urn for record in records)
    assert by_key[(orders, "datasetProperties")]["description"] == "orders table"
    assert by_key[(orders, "datasetProperties")]["customProperties"]["classification"] == "gold"
    assert (orders, "ownership") not in by_key
    fields = by_key[(orders, "schemaMetadata")]["fields"]
    assert [field["fieldPath"] for field in fields] == [
        "[version=2.0].[type=long].id",
        "[version=2.0].[type=bytes].amount",
        "[version=2.0].[type=int].dt",
    ]
    assert fields[-1]["isPartitioningKey"] is True
    assert by_key[(orders, "subTypes")] == {"typeNames": ["Table"]}
    assert (orders, "browsePathsV2") in by_key
    assert by_key[(view, "subTypes")] == {"typeNames": ["View"]}
    assert by_key[(view, "viewProperties")]["viewLogic"] == "select id from orders"


def test_transform_max_file_size_keeps_each_file_valid_json(tmp_path: Path, spark_submit: str) -> None:
    snapshot, output = tmp_path / "snapshot", tmp_path / "output"
    create_snapshot(snapshot)
    result = run_job(spark_submit, snapshot, output, "--max-file-size", "1k")
    assert result.returncode == 0, result.stdout + result.stderr
    json_files = sorted(output.glob("mcp-*.json"))
    assert len(json_files) > 1
    loaded = []
    for file in json_files:
        parsed = json.loads(file.read_text())
        assert isinstance(parsed, list)
        loaded.extend(parsed)
    records = load_records(output)
    assert len(loaded) == len(records) == 24


def test_transform_rejects_incomplete_snapshot(tmp_path: Path, spark_submit: str) -> None:
    snapshot, output = tmp_path / "snapshot", tmp_path / "output"
    create_snapshot(snapshot, complete=False)
    result = run_job(spark_submit, snapshot, output)
    assert result.returncode != 0
    assert "snapshot manifest is missing or incomplete" in result.stdout + result.stderr
