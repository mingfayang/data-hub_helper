# Hive Metastore 离线导出到 DataHub

这个项目把 Hive Metastore 的读取和 DataHub 元数据生成拆成两段：

1. `snapshot_mysql.py` 只对 MySQL 做逐表 `SELECT *`，使用服务端游标流式读取，按分片写成 gzip JSONL；不在 MySQL 执行 JOIN、GROUP BY 或临时表查询。
2. `transform_hms.py` 用 Spark 读取快照，在 Spark 中关联 HMS 表并输出 DataHub MetadataChangeProposal（MCP）JSONL。

输出覆盖数据库容器、表/视图、字段、分区字段、存储位置/格式、owner、comment 和 HMS table parameters。默认不读取 `PARTITIONS`、`PARTITION_PARAMS`：这两张表通常占 HMS 的绝大部分，且 DataHub 的表级目录并不需要逐分区记录。

## 目录结构

```text
config/example.yml                 配置模板
scripts/snapshot_mysql.py          MySQL 流式快照
jobs/transform_hms.py              Spark 转换作业
recipes/ingest_output.yml          把结果导入 DataHub 的 recipe
tests/test_helpers.py              不依赖 Spark/MySQL 的单元测试
```

## 1. 安装快照工具

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/example.yml config/uat.yml
```

密码建议通过环境变量提供，不要写进 YAML：

```bash
export HMS_MYSQL_PASSWORD='***'
python scripts/snapshot_mysql.py --config config/uat.yml
```

输出类似：

```text
data/snapshot-20260624T090000Z/
  manifest.json
  DBS/part-00000.jsonl.gz
  TBLS/part-00000.jsonl.gz
  ...
```

快照使用一个只读事务和一致性快照。若生产库不允许长事务，可将 `consistent_snapshot` 设为 `false`；代价是快照期间发生的 DDL 可能让不同表的数据略微不一致。建议连接 MySQL 只读副本。

## 2. Spark 转换

本地示例：

```bash
spark-submit jobs/transform_hms.py \
  --snapshot data/snapshot-20260624T090000Z \
  --output data/datahub-mcp \
  --env UAT \
  --platform hive \
  --database-pattern '^hive$'
```

集群运行时，`--snapshot` 和 `--output` 可以是 HDFS/S3 路径。输出是一个目录，其中每个 `part-*` 文件均为一行一个 MCP。Spark 默认会写多个 part 文件，不应强制 `coalesce(1)`；大数据量下单文件会重新形成瓶颈。

如确实需要一个文件，可在数据量可控时增加 `--single-file`。该选项只减少 MCP 输出文件数，不改变内容。

## 3. 主程序：快照、上传 HDFS、Spark 输出

如果要把完整逻辑作为一个主流程运行，可使用 `scripts/run_hms_pipeline.py`。配置文件可选；所有主要参数都能在入口参数中覆盖，参考 `config/example.yml`：

```bash
python scripts/run_hms_pipeline.py \
  --config config/uat.yml \
  --snapshot-id snapshot-20260624T090000Z
```

该命令会依次执行：

1. 读取 MySQL HMS 表，并在 `snapshot.output_dir/snapshot-id` 保存 gzip JSONL 快照；
2. 用 `hdfs dfs -put` 上传快照目录到 `hdfs.snapshot_dir/snapshot-id`；
3. 用 `spark-submit` 运行 `jobs/transform_hms.py`，读取 HDFS 快照，并把 DataHub MCP JSONL 输出到 `hdfs.output_dir/snapshot-id`。

生产常用参数可以在命令行覆盖：

```bash
python scripts/run_hms_pipeline.py \
  --config config/uat.yml \
  --mysql-host mysql-uat.example.com \
  --mysql-port 3306 \
  --mysql-database hive \
  --mysql-username hive_reader \
  --mysql-password-env HMS_MYSQL_PASSWORD \
  --snapshot-output-dir data \
  --snapshot-rows-per-file 200000 \
  --snapshot-fetch-size 5000 \
  --hdfs-bin hdfs \
  --hdfs-snapshot-dir hdfs:///warehouse/hms-snapshots \
  --hdfs-output hdfs:///warehouse/datahub-mcp \
  --spark-submit spark-submit \
  --spark-job-file jobs/transform_hms.py \
  --env UAT \
  --platform hive \
  --database-pattern '^hive$' \
  --metastore-name hive_metastore \
  --source-timezone UTC \
  --max-file-size 20M \
  --spark-arg=--master \
  --spark-arg yarn
```

若需要重跑同一个 `snapshot-id`，增加 `--overwrite-hdfs` 会先删除已有 HDFS 快照目录。Spark 输出目录仍使用 Spark 的 `errorifexists` 模式，避免误覆盖结果。`--max-file-size` 支持 `20k`、`20M`、`1G` 等写法，Spark 会按输出 JSONL 总字节数估算分区数，从而控制 part 文件大小。`--single-file` 不能和 `--max-file-size` 同时使用。

主程序不执行 `datahub ingest`，也不执行结果比较。它只负责校验参数、生成 snapshot、上传 HDFS、提交 Spark job。要验证程序输出与 DataHub 1.6.0 官方 connector 的 `datahub ingest -c recipes.yml` 输出一致，请使用集成测试脚本：

```bash
bash integration/run_local_comparison.sh
```

`--no-hdfs-enabled` 只用于本地验证；生产默认开启 HDFS 上传。

## 4. 校验和导入

先执行作业自带的结构校验；转换遇到损坏的快照、重复主键或非法 URN 名称时会直接失败。然后逐个导入 Spark part 文件：

```bash
for file in data/datahub-mcp/part-*; do
  MCP_FILE="$file" datahub ingest -c recipes/ingest_output.yml || exit 1
done
```

也可以使用 `datahub ingest mcps <part-file>`。不要把 `_SUCCESS` 等非 JSON 文件混入输入。

这里的 MCP 指 MetadataChangeProposal，不是 Model Context Protocol。

## 5. 与原 DataHub 输出对比

先用原 recipe 针对一个小范围数据库输出基线文件，再对相同范围运行 Spark 作业：

```bash
python scripts/compare_datahub_output.py \
  --baseline tests/output/local-comparison/baseline-datahub.json \
  --candidate tests/output/local-comparison/spark-datahub-mcp/local-integration \
  --report tests/output/local-comparison/compare-report.json \
  --exact
```

比较器支持两种模式。默认模式用于开发排查，会把不同输入形态展开为可比较的 aspect：

- DataHub MCE、展开 MCP、`aspect.value` 包装 MCP；
- JSON 数组、JSONL 文件或 Spark `part-*` 目录；
- 不同 container URN：按 `containerProperties.name` 归一化；
- schema 字段顺序、owner 顺序、subtype 顺序；
- 忽略 `created`、`lastModified` 等运行时审计字段。

`--exact` 模式用于最终验收：它只展开 DataHub MCE/MCP 的包装格式，然后逐个比较 `entityUrn`、`aspectName`、aspect key 和 aspect value；不会归一化 container URN，不会重排字段，也不会忽略 `created`、`lastModified` 等值。

默认只比较本项目生成的 aspect。增加 `--strict` 会连原 connector 的额外 aspect 一起比较；`--exact` 会按完整 key/value 内容比较。返回码为 `0` 表示一致、`1` 表示存在差异、`2` 表示输入或解析错误。报告会列出缺少、多出和字段级差异。

## 6. 测试

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-test.txt
SPARK_LOCAL_IP=127.0.0.1 .venv/bin/pytest -q
```

测试包含纯函数、MCE/MCP 输入展开、字段级比较、exact key/value 比较、MySQL 流式分片，以及真实 Spark 3.5 本地端到端转换。端到端 fixture 覆盖普通表、外部表、视图、分区字段、owner、table parameters、database 过滤和不完整快照失败。

## 7. 本地 MySQL + 官方 DataHub 完整对比

本项目包含可重复初始化的 HMS 测试库和一键对比脚本：

```bash
brew services start mysql@8.4
bash integration/run_local_comparison.sh
```

脚本固定使用 `/Users/ymf/private/dev_soft/python/python_3.10/bin/python3` 创建 `.venv`，并确保 `acryl-datahub[hive-metastore]==1.6.0`。它会依次执行：初始化 MySQL、插入 100 张测试表、更新 `test_table_042`、`datahub ingest -c recipes.yml`、主程序离线快照、Spark 转换和 exact 内容对比。成功标准为：

```text
equal=True baseline=630 candidate=630 missing=0 extra=0 different=0
```

官方 connector 使用 MCE 打包多个 aspect，Spark 作业输出逐条 MCP，因此不比较物理行数或 JSON 排版；exact 对比会展开 DataHub 文件并逐个比较实体 URN、aspect key 和 aspect value，包括 DataHub 1.6.0 生成的 `browsePathsV2`、`dataPlatformInstance`、`created`、`lastModified`、`jsonProps`，以及 `test_table_042` 更新后的 comment、location 和字段 comment。

## 与原 recipe 的对应关系

- `env` → Spark 的 `--env`，会进入 dataset URN，必须与现有 DataHub 环境完全一致。
- `schema_pattern.allow` → `--database-pattern`，这里匹配 Hive 数据库名（`DBS.NAME`）。
- `host_port/database/username/password` → `config/*.yml` 的 MySQL 连接配置。
- file sink → Spark 输出的 MCP JSONL 目录。

建议第一次仅选择一个 Hive database，与原 connector 的输出做 URN、字段数、comment、owner 抽样比对；确认后再跑全量。
