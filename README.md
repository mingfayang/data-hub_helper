# Hive Metastore 离线导出到 DataHub

这个项目把 Hive Metastore 的读取和 DataHub 元数据生成拆成两段：

1. `snapshot_mysql.py` 只对 MySQL 做逐表 `SELECT *`，使用服务端游标流式读取，按分片写成 gzip JSONL；不在 MySQL 执行 JOIN、GROUP BY 或临时表查询。
2. `transform_hms.py` 用 Spark 读取快照，在 Spark 中关联 HMS 表并输出 DataHub MetadataChangeProposal（MCP）JSON 文件。

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

集群运行时，`--snapshot` 和 `--output` 可以是 HDFS/S3 路径。输出是一个目录，其中每个 `mcp-*.json` 文件都是合法 JSON 数组，数组内每个对象是一条 MCP。文件名使用递增后缀区分，例如 `mcp-00000.json`、`mcp-00001.json`。Spark 默认会写多个 JSON 文件，不应强制 `coalesce(1)`；大数据量下单文件会重新形成瓶颈。

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
3. 上传成功后删除本地 snapshot 目录；
4. 用 `spark-submit` 运行 `jobs/transform_hms.py`，读取 HDFS 快照，并把 DataHub MCP JSON 文件输出到固定目录 `hdfs.output_dir/mysql.database`；
5. Spark 成功生成 JSON 输出后执行 `hdfs dfs -chmod -R 777 hdfs.output_dir/mysql.database`；
6. 删除 HDFS snapshot 目录。

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
  --overwrite-hdfs \
  --spark-submit spark-submit \
  --spark-job-file jobs/transform_hms.py \
  --spark-master yarn \
  --spark-deploy-mode cluster \
  --spark-queue root.datahub \
  --spark-driver-memory 2g \
  --spark-driver-cores 1 \
  --spark-executor-memory 4g \
  --spark-executor-cores 2 \
  --spark-num-executors 4 \
  --spark-app-name hms-snapshot-to-datahub \
  --package-current-venv \
  --spark-archives hdfs:///deps/venv.tar.gz#envir \
  --spark-conf spark.pyspark.python=./envir/bin/python \
  --overwrite-spark-output \
  --spark-conf spark.sql.shuffle.partitions=200 \
  --env UAT \
  --platform hive \
  --platform-instance hive \
  --database-pattern '.*' \
  --source-timezone UTC \
  --max-file-size 20M \
  --spark-arg=--verbose
```

HDFS 上会使用两个目录：

- `hdfs.snapshot_dir/snapshot-id`：临时 snapshot 目录，Spark 成功后会自动删除。
- `hdfs.output_dir/mysql.database`：固定 Spark 最终 JSON 输出目录。

例如 `--hdfs-output hdfs:///warehouse/datahub-mcp --mysql-database hive_metastore` 会始终输出到 `hdfs:///warehouse/datahub-mcp/hive_metastore`。`snapshot-id` 不参与最终输出目录命名，因此 HDFS 上最终只保留这一份 Spark JSON 输出目录。

最终目录中只应该保留 `mcp-*.json` 文件。每个文件都是 UTF-8 文本 JSON 数组，可以直接查看：

```bash
hdfs dfs -cat hdfs:///warehouse/datahub-mcp/hive_metastore/mcp-00000.json | head
```

下载时只下载 `mcp-*.json`，不要下载隐藏校验文件或 `_SUCCESS`：

```bash
hdfs dfs -get 'hdfs:///warehouse/datahub-mcp/hive_metastore/mcp-*.json' ./datahub-mcp/
```

`snapshot-id` 只用于临时 snapshot 目录命名：本地目录为 `snapshot.output_dir/snapshot-id`，HDFS 临时目录为 `hdfs.snapshot_dir/snapshot-id`。如果不传 `--snapshot-id`，程序会自动生成 UTC 秒级名称，如 `snapshot-20260720T042201Z`。

若需要重跑同一个 `snapshot-id`，可以分别控制 snapshot 和 Spark 输出是否覆盖：

- `--overwrite-hdfs`：只影响 HDFS snapshot 目录。它会先删除已有 `hdfs.snapshot_dir/snapshot-id`，再重新上传本地 snapshot。
- `--overwrite-spark-output`：只影响 Spark 最终 JSON 输出目录。它会在提交 Spark 前删除已有 `hdfs.output_dir/mysql.database`，然后让 Spark 重新生成 JSON。

如果不配置 `--overwrite-spark-output`，程序不会删除已有 Spark 输出目录；当 `hdfs.output_dir/mysql.database` 已存在时，Spark 会按 `errorifexists` 行为报错退出，避免误覆盖结果。

`--max-file-size` 支持 `20k`、`20M`、`1G` 等写法，Spark 会按输出 JSON 总字节数估算分区数，从而控制 `mcp-*.json` 文件大小。拆分只发生在 MCP 记录之间，每个输出文件仍然是完整合法的 JSON 数组。`--single-file` 不能和 `--max-file-size` 同时使用。

常用 Spark 资源参数都可以在入口暴露或写入 `config/*.yml` 的 `spark:` 段：`master`、`deploy_mode`、`queue`、`driver_memory`、`driver_cores`、`executor_memory`、`executor_cores`、`num_executors`、`app_name`、`archives` 和多条 `conf`。`--spark-archives` 会直接映射为 `spark-submit --archives`；`--spark-arg` 仍可用于补充更少用的 `spark-submit` 参数，例如 `--jars`。

如果需要把当前已激活的 Python 虚拟环境一起提交给 Spark，增加 `--package-current-venv`。程序会优先读取 `VIRTUAL_ENV`，否则使用当前 Python 可执行文件路径来反推虚拟环境根目录，这和在 shell 中看 `which python` 的路径是同一个思路。例如当前 Python 是 `/opt/app/env/bin/python`，程序会打包 `/opt/app/env/` 下的内容，但 tar 包内不会包含外层 `env/` 目录；解压后的根目录直接包含 `bin/`、`lib/`、`pyvenv.cfg` 等内容。

推荐用法：

```bash
python scripts/run_hms_pipeline.py \
  --config config/uat.yml \
  --package-current-venv \
  --spark-archives hdfs:///deps/venv.tar.gz#envir \
  --spark-conf spark.pyspark.python=./envir/bin/python
```

这会生成本地 `venv.tar.gz`，上传到 `hdfs:///deps/venv.tar.gz`，并提交 Spark 参数 `--archives hdfs:///deps/venv.tar.gz#envir`。本地 tar 上传成功后会立即删除；Spark 成功结束后，程序会删除 HDFS 上的 `hdfs:///deps/venv.tar.gz`。如果没有传 `--spark-archives`，默认会使用 `hdfs:///deps/venv.tar.gz#envir`；也可以通过 `--venv-hdfs-dir`、`--venv-archive-name`、`--venv-archive-alias` 分别控制 HDFS 目录、文件名和 `#` 后的解压目录名。

主程序不执行 `datahub ingest`，也不执行结果比较。它只负责校验参数、生成 snapshot、上传 HDFS、提交 Spark job。要验证程序输出与 DataHub 1.6.0 官方 connector 的 `datahub ingest -c recipes.yml` 输出一致，请使用集成测试脚本：

```bash
bash integration/run_local_comparison.sh
```

`--no-hdfs-enabled` 只用于本地验证；生产默认开启 HDFS 上传。本地验证模式下 Spark 直接读取本地 snapshot，因此不会自动删除本地 snapshot。

## 4. 校验和导入

先执行作业自带的结构校验；转换遇到损坏的快照、重复主键或非法 URN 名称时会直接失败。然后逐个导入 Spark part 文件：

```bash
for file in data/datahub-mcp/mcp-*.json; do
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
  --candidate tests/output/local-comparison/spark-datahub-mcp/hive_metastore_test \
  --report tests/output/local-comparison/compare-report.json \
  --exact
```

比较器支持两种模式。默认模式用于开发排查，会把不同输入形态展开为可比较的 aspect：

- DataHub MCE、展开 MCP、`aspect.value` 包装 MCP；
- JSON 数组、JSONL 文件或 Spark `mcp-*.json` 目录；
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
equal=True baseline=746 candidate=746 missing=0 extra=0 different=0
```

官方 connector 使用 MCE 打包多个 aspect，Spark 作业输出逐条 MCP，因此不比较物理行数或 JSON 排版；exact 对比会展开 DataHub 文件并逐个比较实体 URN、aspect key 和 aspect value，包括 DataHub 1.6.0 生成的 `browsePathsV2`、`dataPlatformInstance`、`created`、`lastModified`、`jsonProps`，以及 `test_table_042` 更新后的 comment、location 和字段 comment。

## 与原 recipe 的对应关系

- `env` → Spark 的 `--env`，会进入 dataset URN，必须与现有 DataHub 环境完全一致。
- `platform_instance` → Spark 的 `--platform-instance`，会进入 dataset URN、container GUID 和 `dataPlatformInstance` aspect；当前 recipe 固定为 `hive`。
- `schema_pattern.allow: [".*"]` → 程序侧默认 `--database-pattern '.*'`，即不过滤 Hive 数据库名（`DBS.NAME`）。
- `host_port/database/username/password` → `config/*.yml` 的 MySQL 连接配置；`database` 同时作为 DataHub root container 名称和 Spark 输出子目录名。
- file sink → Spark 输出的 MCP JSON 目录。

建议第一次仅选择一个 Hive database，与原 connector 的输出做 URN、字段数、comment、owner 抽样比对；确认后再跑全量。
