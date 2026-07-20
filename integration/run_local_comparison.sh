#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON_BASE="${PYTHON_BASE:-/Users/ymf/private/dev_soft/python/python_3.10/bin/python3}"
MYSQL_BIN="${MYSQL_BIN:-/opt/homebrew/opt/mysql@8.4/bin/mysql}"
VENV="$ROOT/.venv"
OUTPUT_ROOT="$ROOT/tests/output/local-comparison"
BASELINE_OUTPUT="$OUTPUT_ROOT/baseline-datahub.json"
SNAPSHOT_ROOT="$OUTPUT_ROOT/snapshots"
SPARK_OUTPUT_ROOT="$OUTPUT_ROOT/spark-datahub-mcp"
SPARK_OUTPUT="$SPARK_OUTPUT_ROOT/local-integration"
COMPARE_REPORT="$OUTPUT_ROOT/compare-report.json"

if [[ ! -x "$VENV/bin/datahub" ]] || ! "$VENV/bin/python" -c "import cryptography, importlib.metadata as m; assert m.version('acryl-datahub') == '1.6.0'" 2>/dev/null; then
  "$PYTHON_BASE" -m venv --clear "$VENV"
  "$VENV/bin/pip" install -r requirements-test.txt 'acryl-datahub[hive-metastore]==1.6.0'
fi

export PATH="$VENV/bin:$PATH"
export PYSPARK_PYTHON="$VENV/bin/python3"
export SPARK_LOCAL_IP="127.0.0.1"
export HMS_MYSQL_PASSWORD="hive_test_password"

mkdir -p "$OUTPUT_ROOT"
"$MYSQL_BIN" -u root < integration/mysql/init_hms.sql
"$MYSQL_BIN" -u root < integration/mysql/seed_100_tables_and_update.sql

rm -f "$BASELINE_OUTPUT" "$COMPARE_REPORT"
rm -rf "$SNAPSHOT_ROOT/local-integration" "$SPARK_OUTPUT_ROOT"

datahub ingest -c recipes.yml
python scripts/run_hms_pipeline.py \
  --config config/local-integration.yml \
  --snapshot-id local-integration \
  --snapshot-output-dir "$SNAPSHOT_ROOT" \
  --no-hdfs-enabled \
  --hdfs-output "$SPARK_OUTPUT_ROOT" \
  --spark-arg=--master \
  --spark-arg 'local[2]' \
  --env UAT \
  --platform hive \
  --database-pattern '^hive$' \
  --metastore-name hive_metastore_test \
  --source-timezone Asia/Shanghai \
  --max-file-size 2k
python scripts/compare_datahub_output.py \
  --baseline "$BASELINE_OUTPUT" \
  --candidate "$SPARK_OUTPUT" \
  --report "$COMPARE_REPORT" \
  --exact

python - <<'PY'
from pathlib import Path
from hms_export.compare import load_records

candidate = Path("tests/output/local-comparison/spark-datahub-mcp/local-integration")
records = load_records(candidate)
target = "urn:li:dataset:(urn:li:dataPlatform:hive,hive.test_table_042,UAT)"
by_key = {(record.urn, record.aspect_name): record.aspect for record in records}
properties = by_key[(target, "datasetProperties")]
schema = by_key[(target, "schemaMetadata")]

assert properties["description"] == "Updated integration table 042 comment"
assert properties["customProperties"]["table_location"] == "file:///tmp/hive/warehouse/test_table_042_updated"
value_field = next(field for field in schema["fields"] if field["fieldPath"].endswith(".value"))
assert value_field["description"] == "Updated value column 042"
PY

echo "Local integration comparison passed: $COMPARE_REPORT"
