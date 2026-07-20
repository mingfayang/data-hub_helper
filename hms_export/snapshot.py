from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pymysql
import yaml

from hms_export.common import mysql_json_value, validate_table_name


DEFAULT_HMS_TABLES = (
    "DBS",
    "DATABASE_PARAMS",
    "TBLS",
    "TABLE_PARAMS",
    "SDS",
    "SERDES",
    "SERDE_PARAMS",
    "COLUMNS_V2",
    "PARTITION_KEYS",
)


def load_config(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not isinstance(config, dict) or "mysql" not in config or "snapshot" not in config:
        raise ValueError("config must contain mysql and snapshot sections")
    return config


def iter_rows(cursor: Any, fetch_size: int) -> Iterable[Dict[str, Any]]:
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            return
        yield from rows


def dump_table(
    connection: Any, table: str, root: Path, rows_per_file: int, fetch_size: int
) -> Dict[str, Any]:
    table = validate_table_name(table)
    table_dir = root / table
    table_dir.mkdir(parents=True, exist_ok=False)
    row_count = 0
    file_count = 0
    output = None
    try:
        with connection.cursor() as cursor:
            # Identifier is validated above; values are not interpolated into SQL.
            cursor.execute(f"SELECT * FROM `{table}`")
            for row in iter_rows(cursor, fetch_size):
                if row_count % rows_per_file == 0:
                    if output:
                        output.close()
                    output = gzip.open(
                        table_dir / f"part-{file_count:05d}.jsonl.gz",
                        mode="wt",
                        encoding="utf-8",
                    )
                    file_count += 1
                normalized = {k: mysql_json_value(v) for k, v in row.items()}
                output.write(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")))
                output.write("\n")
                row_count += 1
    finally:
        if output:
            output.close()
    # Keep an empty table Spark-readable.
    if file_count == 0:
        gzip.open(table_dir / "part-00000.jsonl.gz", mode="wt").close()
        file_count = 1
    return {"table": table, "rows": row_count, "files": file_count}


def connect_mysql(mysql: Dict[str, Any]) -> Any:
    password_env = mysql.get("password_env", "HMS_MYSQL_PASSWORD")
    password = os.environ.get(password_env)
    if password is None:
        raise RuntimeError(f"required environment variable {password_env!r} is not set")
    return pymysql.connect(
        host=mysql["host"],
        port=int(mysql.get("port", 3306)),
        user=mysql["username"],
        password=password,
        database=mysql["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSDictCursor,
        connect_timeout=int(mysql.get("connect_timeout_seconds", 30)),
        read_timeout=int(mysql.get("read_timeout_seconds", 3600)),
        autocommit=False,
    )


def create_snapshot(config: Dict[str, Any], snapshot_id: Optional[str] = None) -> Path:
    mysql = config["mysql"]
    snapshot = config["snapshot"]
    snapshot_id = snapshot_id or datetime.now(timezone.utc).strftime("snapshot-%Y%m%dT%H%M%SZ")
    root = Path(snapshot["output_dir"]) / snapshot_id
    root.mkdir(parents=True, exist_ok=False)
    manifest: Dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "database": mysql["database"],
        "consistent_snapshot": bool(mysql.get("consistent_snapshot", True)),
        "tables": [],
        "complete": False,
    }

    connection = connect_mysql(mysql)
    try:
        if mysql.get("consistent_snapshot", True):
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        else:
            connection.begin()
        for table in snapshot.get("tables") or DEFAULT_HMS_TABLES:
            result = dump_table(
                connection,
                table,
                root,
                int(snapshot.get("rows_per_file", 200000)),
                int(snapshot.get("fetch_size", 5000)),
            )
            manifest["tables"].append(result)
            print(f"{table}: {result['rows']} rows, {result['files']} files", flush=True)
        connection.commit()
        manifest["complete"] = True
        manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
        return root
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
        with (root / "manifest.json").open("w", encoding="utf-8") as stream:
            json.dump(manifest, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
