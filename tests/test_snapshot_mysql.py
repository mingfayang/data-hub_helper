import gzip
import json
from pathlib import Path

import pytest

from scripts.snapshot_mysql import dump_table, iter_rows, load_config


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.offset = 0
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        result = self.rows[self.offset:self.offset + size]
        self.offset += len(result)
        return result


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.last_cursor = None

    def cursor(self):
        self.last_cursor = FakeCursor(self.rows)
        return self.last_cursor


def read_parts(path: Path):
    rows = []
    for part in sorted(path.glob("*.jsonl.gz")):
        with gzip.open(part, "rt", encoding="utf-8") as stream:
            rows.extend(json.loads(line) for line in stream if line.strip())
    return rows


def test_iter_rows_fetches_in_batches() -> None:
    cursor = FakeCursor([{"id": index} for index in range(5)])
    assert list(iter_rows(cursor, 2)) == [{"id": index} for index in range(5)]


def test_dump_table_streams_normalizes_and_shards(tmp_path: Path) -> None:
    connection = FakeConnection([
        {"ID": 1, "VALUE": b"a"}, {"ID": 2, "VALUE": b"b"}, {"ID": 3, "VALUE": None},
    ])
    result = dump_table(connection, "TBLS", tmp_path, rows_per_file=2, fetch_size=1)
    assert result == {"table": "TBLS", "rows": 3, "files": 2}
    assert connection.last_cursor.sql == "SELECT * FROM `TBLS`"
    assert read_parts(tmp_path / "TBLS") == [
        {"ID": 1, "VALUE": "a"}, {"ID": 2, "VALUE": "b"}, {"ID": 3, "VALUE": None},
    ]


def test_dump_empty_table_writes_spark_readable_part(tmp_path: Path) -> None:
    result = dump_table(FakeConnection([]), "DBS", tmp_path, 2, 1)
    assert result == {"table": "DBS", "rows": 0, "files": 1}
    assert read_parts(tmp_path / "DBS") == []


def test_dump_rejects_unsafe_table_before_query(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        dump_table(FakeConnection([]), "TBLS; DROP", tmp_path, 2, 1)


def test_load_config_validates_sections(tmp_path: Path) -> None:
    good = tmp_path / "good.yml"
    good.write_text("mysql: {}\nsnapshot: {}\n")
    assert load_config(good) == {"mysql": {}, "snapshot": {}}
    bad = tmp_path / "bad.yml"
    bad.write_text("mysql: {}\n")
    with pytest.raises(ValueError, match="mysql and snapshot"):
        load_config(bad)

