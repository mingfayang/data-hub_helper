#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


TABLE_SCHEMAS: Dict[str, T.StructType] = {
    "DBS": T.StructType([
        T.StructField("DB_ID", T.LongType()), T.StructField("DESC", T.StringType()),
        T.StructField("DB_LOCATION_URI", T.StringType()), T.StructField("NAME", T.StringType()),
        T.StructField("OWNER_NAME", T.StringType()), T.StructField("OWNER_TYPE", T.StringType()),
        T.StructField("CTLG_NAME", T.StringType()),
    ]),
    "TBLS": T.StructType([
        T.StructField("TBL_ID", T.LongType()), T.StructField("CREATE_TIME", T.LongType()),
        T.StructField("DB_ID", T.LongType()), T.StructField("LAST_ACCESS_TIME", T.LongType()),
        T.StructField("OWNER", T.StringType()), T.StructField("RETENTION", T.LongType()),
        T.StructField("SD_ID", T.LongType()), T.StructField("TBL_NAME", T.StringType()),
        T.StructField("TBL_TYPE", T.StringType()), T.StructField("VIEW_EXPANDED_TEXT", T.StringType()),
        T.StructField("VIEW_ORIGINAL_TEXT", T.StringType()), T.StructField("IS_REWRITE_ENABLED", T.BooleanType()),
    ]),
    "SDS": T.StructType([
        T.StructField("SD_ID", T.LongType()), T.StructField("CD_ID", T.LongType()),
        T.StructField("INPUT_FORMAT", T.StringType()), T.StructField("IS_COMPRESSED", T.BooleanType()),
        T.StructField("LOCATION", T.StringType()), T.StructField("NUM_BUCKETS", T.LongType()),
        T.StructField("OUTPUT_FORMAT", T.StringType()), T.StructField("SERDE_ID", T.LongType()),
        T.StructField("STORED_AS_SUB_DIRECTORIES", T.BooleanType()),
    ]),
    "COLUMNS_V2": T.StructType([
        T.StructField("CD_ID", T.LongType()), T.StructField("COMMENT", T.StringType()),
        T.StructField("COLUMN_NAME", T.StringType()), T.StructField("TYPE_NAME", T.StringType()),
        T.StructField("INTEGER_IDX", T.IntegerType()),
    ]),
    "PARTITION_KEYS": T.StructType([
        T.StructField("TBL_ID", T.LongType()), T.StructField("PKEY_COMMENT", T.StringType()),
        T.StructField("PKEY_NAME", T.StringType()), T.StructField("PKEY_TYPE", T.StringType()),
        T.StructField("INTEGER_IDX", T.IntegerType()),
    ]),
    "TABLE_PARAMS": T.StructType([
        T.StructField("TBL_ID", T.LongType()), T.StructField("PARAM_KEY", T.StringType()),
        T.StructField("PARAM_VALUE", T.StringType()),
    ]),
}


def parse_byte_size(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([kKmMgG]?[bB]?)?\s*", value)
    if not match:
        raise ValueError(f"invalid byte size: {value!r}")
    amount = int(match.group(1))
    unit = (match.group(2) or "").lower().removesuffix("b")
    multiplier = {"": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[unit]
    size = amount * multiplier
    if size <= 0:
        raise ValueError("--max-file-size must be positive")
    return size


def mcp(entity_type: str, urn: Any, aspect_name: str, aspect: Any) -> Any:
    return F.struct(
        F.lit(entity_type).alias("entityType"), urn.alias("entityUrn"),
        F.lit("UPSERT").alias("changeType"), F.lit(aspect_name).alias("aspectName"),
        F.struct(
            F.to_json(aspect).alias("value"),
            F.lit("application/json").alias("contentType"),
        ).alias("aspect"),
    )


def dataset_urn(platform: str, name: Any, env: str, platform_instance: str | None = None) -> Any:
    dataset_name = F.concat(F.lit(f"{platform_instance}."), name) if platform_instance else name
    return F.concat(
        F.lit(f"urn:li:dataset:(urn:li:dataPlatform:{platform},"), dataset_name,
        F.lit(f",{env.upper()})"),
    )


def container_urn(platform: str, database: Any, env: str, platform_instance: str | None = None) -> Any:
    guid_json = F.to_json(F.struct(
        database.alias("database"),
        F.lit(platform_instance or env.upper()).alias("instance"),
        F.lit(platform).alias("platform"),
    ))
    return F.concat(F.lit("urn:li:container:"), F.md5(guid_json))


def schema_container_urn(
    platform: str,
    database: Any,
    schema: Any,
    env: str,
    platform_instance: str | None = None,
) -> Any:
    guid_json = F.to_json(F.struct(
        database.alias("database"),
        F.lit(platform_instance or env.upper()).alias("instance"),
        F.lit(platform).alias("platform"),
        schema.alias("schema"),
    ))
    return F.concat(F.lit("urn:li:container:"), F.md5(guid_json))


def data_platform_instance_aspect(platform: str, platform_instance: str | None = None) -> Any:
    platform_urn = F.lit(f"urn:li:dataPlatform:{platform}")
    if platform_instance:
        return F.struct(
            platform_urn.alias("platform"),
            F.lit(f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{platform},{platform_instance})").alias("instance"),
        )
    return F.struct(platform_urn.alias("platform"))


def schema_type(native_type: Any) -> Any:
    t = F.lower(native_type)
    type_name = (
        F.when(t.rlike(r"^(tinyint|smallint|int|bigint|float|double|decimal)"), F.lit("com.linkedin.pegasus2avro.schema.NumberType"))
        .when(t.rlike(r"^boolean"), F.lit("com.linkedin.pegasus2avro.schema.BooleanType"))
        .when(t.rlike(r"^binary"), F.lit("com.linkedin.pegasus2avro.schema.BytesType"))
        .when(t.rlike(r"^date"), F.lit("com.linkedin.pegasus2avro.schema.DateType"))
        .when(t.rlike(r"^timestamp"), F.lit("com.linkedin.pegasus2avro.schema.TimeType"))
        .otherwise(F.lit("com.linkedin.pegasus2avro.schema.StringType"))
    )
    return F.struct(F.create_map(type_name, F.create_map()).alias("type"))


def schema_json_props(native_type: Any) -> Any:
    native = F.lower(native_type)
    decimal_match = F.regexp_extract(native, r"^decimal\((\d+),(\d+)\)", 1)
    decimal_scale = F.regexp_extract(native, r"^decimal\((\d+),(\d+)\)", 2)
    return (
        F.when(
            native.rlike(r"^decimal\(\d+,\d+\)"),
            F.concat(
                F.lit('{"logicalType": "decimal", "precision": '),
                decimal_match,
                F.lit(', "scale": '),
                decimal_scale,
                F.lit("}"),
            ),
        )
        .when(native.rlike(r"^date"), F.lit('{"logicalType": "date"}'))
    )


def v2_field_path(name: Any, native_type: Any) -> Any:
    native = F.lower(native_type)
    avro_type = (
        F.when(native.rlike(r"^bigint"), F.lit("long"))
        .when(native.rlike(r"^(tinyint|smallint|int|date)"), F.lit("int"))
        .when(native.rlike(r"^(float|double)"), F.lit("double"))
        .when(native.rlike(r"^(decimal|binary)"), F.lit("bytes"))
        .when(native.rlike(r"^boolean"), F.lit("boolean"))
        .otherwise(F.lit("string"))
    )
    return F.concat(F.lit("[version=2.0].[type="), avro_type, F.lit("]."), name)


def read_table(spark: SparkSession, root: str, name: str) -> DataFrame:
    schema = TABLE_SCHEMAS.get(name)
    reader = spark.read
    if schema is not None:
        reader = reader.schema(schema)
    return reader.json(f"{root.rstrip('/')}/{name}/*.jsonl.gz")


def rename_part_files_as_json(spark: SparkSession, output: str) -> None:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(output)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    statuses = filesystem.listStatus(path)
    part_statuses = sorted(
        [
            status
            for status in statuses
            if status.isFile() and status.getPath().getName().startswith("part-")
        ],
        key=lambda item: item.getPath().getName(),
    )
    part_files = []
    for status in part_statuses:
        if status.getLen() == 0:
            filesystem.delete(status.getPath(), False)
        else:
            part_files.append(status.getPath())
    part_files = sorted(
        part_files,
        key=lambda item: item.getName(),
    )
    for index, source in enumerate(part_files):
        target = jvm.org.apache.hadoop.fs.Path(path, f"mcp-{index:05d}.json")
        if source.getName() != target.getName():
            filesystem.rename(source, target)


def cleanup_non_json_output_files(spark: SparkSession, output: str) -> None:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(output)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    for status in filesystem.listStatus(path):
        name = status.getPath().getName()
        if not (status.isFile() and name.startswith("mcp-") and name.endswith(".json")):
            filesystem.delete(status.getPath(), True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert an HMS snapshot to DataHub MCP JSON arrays")
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--env", default="PROD")
    parser.add_argument("--platform", default="hive")
    parser.add_argument("--platform-instance")
    parser.add_argument("--database-pattern", default=".*")
    parser.add_argument("--metastore-name", default="hive_metastore")
    parser.add_argument("--source-timezone", default="UTC")
    parser.add_argument("--single-file", action="store_true")
    parser.add_argument(
        "--max-file-size",
        help="target maximum Spark output file size, for example 20k, 20M, or 1G",
    )
    args = parser.parse_args()
    max_file_size = parse_byte_size(args.max_file_size) if args.max_file_size else None
    if args.single_file and max_file_size is not None:
        raise ValueError("--single-file cannot be used with --max-file-size")

    spark = (
        SparkSession.builder.appName("hms-snapshot-to-datahub")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", args.source_timezone)
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    manifest = spark.read.option("multiline", True).json(f"{args.snapshot.rstrip('/')}/manifest.json")
    if manifest.filter(F.col("complete") == F.lit(True)).limit(1).count() != 1:
        raise RuntimeError("snapshot manifest is missing or incomplete")
    dbs = read_table(spark, args.snapshot, "DBS").filter(F.col("NAME").rlike(args.database_pattern))
    tbls = read_table(spark, args.snapshot, "TBLS")
    sds = read_table(spark, args.snapshot, "SDS")
    columns = read_table(spark, args.snapshot, "COLUMNS_V2")
    partition_keys = read_table(spark, args.snapshot, "PARTITION_KEYS")
    params = read_table(spark, args.snapshot, "TABLE_PARAMS")

    duplicate_db = dbs.groupBy("DB_ID").count().filter("count > 1").limit(1).count()
    duplicate_tbl = tbls.groupBy("TBL_ID").count().filter("count > 1").limit(1).count()
    if duplicate_db or duplicate_tbl:
        raise RuntimeError("snapshot contains duplicate DBS.DB_ID or TBLS.TBL_ID")

    base = (
        tbls.join(F.broadcast(dbs), "DB_ID", "inner")
        .join(sds, "SD_ID", "left")
        .withColumn("DATASET_NAME", F.concat_ws(".", F.col("NAME"), F.col("TBL_NAME")))
        .withColumn("URN", dataset_urn(args.platform, F.col("DATASET_NAME"), args.env, args.platform_instance))
        .withColumn("CONTAINER_URN", schema_container_urn(
            args.platform, F.lit(args.metastore_name), F.col("NAME"), args.env, args.platform_instance
        ))
    )
    if base.filter(F.col("DATASET_NAME").rlike(r"[(),]")).limit(1).count():
        raise RuntimeError("database/table name contains a DataHub tuple URN delimiter: (, ) or ,")

    normal_fields = columns.select(
        "CD_ID", F.col("INTEGER_IDX").alias("POS"), F.col("COLUMN_NAME").alias("FIELD_PATH"),
        F.col("TYPE_NAME").alias("NATIVE_TYPE"), F.col("COMMENT").alias("DESCRIPTION"),
        F.lit(False).alias("IS_PARTITION"),
    )
    partition_fields = partition_keys.select(
        "TBL_ID", (F.col("INTEGER_IDX") + F.lit(1000000)).alias("POS"),
        F.col("PKEY_NAME").alias("FIELD_PATH"), F.col("PKEY_TYPE").alias("NATIVE_TYPE"),
        F.col("PKEY_COMMENT").alias("DESCRIPTION"), F.lit(True).alias("IS_PARTITION"),
    )
    table_fields = (
        base.select("TBL_ID", "CD_ID")
        .join(normal_fields, "CD_ID", "left")
        .select("TBL_ID", "POS", "FIELD_PATH", "NATIVE_TYPE", "DESCRIPTION", "IS_PARTITION")
        .unionByName(partition_fields)
        .filter(F.col("FIELD_PATH").isNotNull())
        .withColumn(
            "FIELD", F.struct(
                # DataHub's Hive connector emits v2 field paths by default.
                v2_field_path(F.col("FIELD_PATH"), F.col("NATIVE_TYPE")).alias("fieldPath"),
                F.col("NATIVE_TYPE").alias("nativeDataType"),
                schema_type(F.col("NATIVE_TYPE")).alias("type"),
                F.coalesce(F.col("DESCRIPTION"), F.lit("")).alias("description"),
                F.lit(True).alias("nullable"),
                F.lit(False).alias("recursive"),
                F.lit(False).alias("isPartOfKey"),
                F.when(F.col("IS_PARTITION"), F.lit(True)).alias("isPartitioningKey"),
                schema_json_props(F.col("NATIVE_TYPE")).alias("jsonProps"),
            )
        )
        .groupBy("TBL_ID").agg(
            F.array_sort(
                F.collect_list(F.struct("POS", "FIELD")),
                lambda left, right: (
                    F.when(left["POS"] < right["POS"], F.lit(-1))
                    .when(left["POS"] > right["POS"], F.lit(1))
                    .otherwise(F.lit(0))
                ),
            ).alias("SORTED")
        )
        .select("TBL_ID", F.transform("SORTED", lambda x: x["FIELD"]).alias("FIELDS"))
    )
    param_maps = (
        params.filter(F.col("PARAM_KEY").isNotNull() & F.col("PARAM_VALUE").isNotNull())
        .groupBy("TBL_ID")
        .agg(F.map_from_entries(F.collect_list(F.struct("PARAM_KEY", "PARAM_VALUE"))).alias("PARAMS"))
    )
    enriched = base.join(table_fields, "TBL_ID", "left").join(param_maps, "TBL_ID", "left")
    table_properties = F.create_map(
        F.lit("table_type"), F.coalesce(F.col("TBL_TYPE"), F.lit("")),
        F.lit("table_location"), F.coalesce(F.col("LOCATION"), F.lit("")),
        F.lit("create_date"), F.from_unixtime(F.col("CREATE_TIME"), "yyyy-MM-dd"),
    )
    partition_names = partition_keys.groupBy("TBL_ID").agg(
        F.concat_ws(",", F.sort_array(F.collect_list("PKEY_NAME"))).alias("PARTITIONED_COLUMNS")
    )
    enriched = enriched.join(partition_names, "TBL_ID", "left")

    schema_urn = schema_container_urn(
        args.platform, F.lit(args.metastore_name), F.col("NAME"), args.env, args.platform_instance
    )
    root_urn = container_urn(args.platform, F.lit(args.metastore_name), args.env, args.platform_instance)
    root_custom_properties = [
        F.lit("platform"), F.lit(args.platform),
        F.lit("env"), F.lit(args.env.upper()),
        F.lit("database"), F.lit(args.metastore_name),
    ]
    schema_custom_properties = [
        F.lit("platform"), F.lit(args.platform),
        F.lit("env"), F.lit(args.env.upper()),
        F.lit("database"), F.lit(args.metastore_name),
        F.lit("schema"), F.col("NAME"),
    ]
    if args.platform_instance:
        root_custom_properties[2:2] = [F.lit("instance"), F.lit(args.platform_instance)]
        schema_custom_properties[2:2] = [F.lit("instance"), F.lit(args.platform_instance)]
    root_browse_path_items = []
    schema_browse_path_items = [F.struct(root_urn.alias("id"), root_urn.alias("urn"))]
    dataset_browse_path_items = [
        F.struct(root_urn.alias("id"), root_urn.alias("urn")),
        F.struct(F.col("CONTAINER_URN").alias("id"), F.col("CONTAINER_URN").alias("urn")),
    ]
    if args.platform_instance:
        data_platform_instance_urn = F.lit(
            f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{args.platform},{args.platform_instance})"
        )
        data_platform_instance_path = F.struct(
            data_platform_instance_urn.alias("id"),
            data_platform_instance_urn.alias("urn"),
        )
        root_browse_path_items.append(data_platform_instance_path)
        schema_browse_path_items.insert(0, data_platform_instance_path)
        dataset_browse_path_items.insert(0, data_platform_instance_path)
    root_events = spark.range(1).select(
        F.explode(F.array(
            mcp("container", root_urn, "containerProperties", F.struct(
                F.create_map(*root_custom_properties).alias("customProperties"),
                F.lit(args.metastore_name).alias("name"),
                F.lit(args.env.upper()).alias("env"),
            )),
            mcp("container", root_urn, "status", F.struct(F.lit(False).alias("removed"))),
            mcp("container", root_urn, "subTypes", F.struct(F.array(F.lit("Database")).alias("typeNames"))),
            mcp("container", root_urn, "dataPlatformInstance", data_platform_instance_aspect(
                args.platform, args.platform_instance
            )),
            mcp("container", root_urn, "browsePathsV2", F.struct(F.array(*root_browse_path_items).alias("path"))),
        )).alias("mcp")
    ).select("mcp.*")
    schema_events = dbs.select(
        F.explode(F.array(
            mcp("container", schema_urn, "container", F.struct(root_urn.alias("container"))),
            mcp("container", schema_urn, "containerProperties", F.struct(
                F.create_map(*schema_custom_properties).alias("customProperties"),
                F.col("NAME").alias("name"),
                F.lit(args.env.upper()).alias("env"),
            )),
            mcp("container", schema_urn, "status", F.struct(F.lit(False).alias("removed"))),
            mcp("container", schema_urn, "subTypes", F.struct(F.array(F.lit("Schema")).alias("typeNames"))),
            mcp("container", schema_urn, "dataPlatformInstance", data_platform_instance_aspect(
                args.platform, args.platform_instance
            )),
            mcp("container", schema_urn, "browsePathsV2", F.struct(
                F.array(*schema_browse_path_items).alias("path")
            )),
        )).alias("mcp")
    ).select("mcp.*")

    dataset_event_items = [
        mcp("dataset", F.col("URN"), "datasetProperties", F.struct(
                F.col("TBL_NAME").alias("name"),
                F.when(
                    ~F.col("TBL_TYPE").isin("VIRTUAL_VIEW", "MATERIALIZED_VIEW"),
                    F.element_at("PARAMS", "comment"),
                ).alias("description"),
                F.when(
                    F.col("TBL_TYPE").isin("VIRTUAL_VIEW", "MATERIALIZED_VIEW"),
                    F.create_map(F.lit("is_view"), F.lit("True")),
                ).otherwise(
                    F.map_concat(
                        F.coalesce("PARAMS", F.create_map()),
                        table_properties,
                        F.when(
                            F.col("PARTITIONED_COLUMNS").isNotNull(),
                            F.create_map(F.lit("partitioned_columns"), F.col("PARTITIONED_COLUMNS")),
                        ).otherwise(F.create_map()),
                    )
                ).alias("customProperties"),
                F.array().alias("tags"),
            )),
        mcp("dataset", F.col("URN"), "schemaMetadata", F.struct(
                F.col("DATASET_NAME").alias("schemaName"),
                F.lit(f"urn:li:dataPlatform:{args.platform}").alias("platform"),
                F.lit(0).cast("long").alias("version"),
                F.struct(
                    F.lit(0).cast("long").alias("time"),
                    F.lit("urn:li:corpuser:unknown").alias("actor"),
                ).alias("created"),
                F.struct(
                    F.lit(0).cast("long").alias("time"),
                    F.lit("urn:li:corpuser:unknown").alias("actor"),
                ).alias("lastModified"),
                F.lit("").alias("hash"),
                F.create_map(
                    F.lit("com.linkedin.pegasus2avro.schema.MySqlDDL"),
                    F.struct(F.lit("").alias("tableSchema")),
                ).alias("platformSchema"),
                F.coalesce("FIELDS", F.array()).alias("fields"),
            )),
        mcp("dataset", F.col("URN"), "status", F.struct(F.lit(False).alias("removed"))),
        mcp("dataset", F.col("URN"), "container", F.struct(F.col("CONTAINER_URN").alias("container"))),
        mcp("dataset", F.col("URN"), "browsePathsV2", F.struct(
                F.array(*dataset_browse_path_items).alias("path")
            )),
        mcp("dataset", F.col("URN"), "subTypes", F.struct(
                F.array(
                    F.when(F.col("TBL_TYPE").isin("VIRTUAL_VIEW", "MATERIALIZED_VIEW"), F.lit("View"))
                    .otherwise(F.lit("Table"))
                ).alias("typeNames"),
            )),
    ]
    if args.platform_instance:
        dataset_event_items.append(mcp("dataset", F.col("URN"), "dataPlatformInstance", data_platform_instance_aspect(
                args.platform, args.platform_instance
            )))
    dataset_events = enriched.select(
        F.explode(F.array(*dataset_event_items)).alias("mcp")
    ).select("mcp.*")

    view_events = enriched.filter(F.col("TBL_TYPE").isin("VIRTUAL_VIEW", "MATERIALIZED_VIEW")).select(
        mcp("dataset", F.col("URN"), "viewProperties", F.struct(
            (F.col("TBL_TYPE") == F.lit("MATERIALIZED_VIEW")).alias("materialized"),
            F.lit("SQL").alias("viewLanguage"),
            F.coalesce(F.col("VIEW_EXPANDED_TEXT"), F.col("VIEW_ORIGINAL_TEXT"), F.lit("")).alias("viewLogic"),
        )).alias("mcp")
    ).select("mcp.*")

    events = root_events.unionByName(schema_events).unionByName(dataset_events).unionByName(view_events)
    if args.single_file:
        events = events.coalesce(1)
    output = events.select(F.to_json(F.struct("*")).alias("value"))
    if max_file_size is not None:
        total_bytes = output.select(
            F.sum(F.length(F.encode(F.col("value"), "UTF-8")) + F.lit(2)).alias("bytes")
        ).first()["bytes"] or 0
        output = output.repartition(max(1, int(math.ceil(total_bytes / max_file_size))))
    json_arrays = output.rdd.mapPartitions(
        lambda rows: (
            ["[\n" + ",\n".join(row.value for row in rows) + "\n]\n"]
        )
    ).filter(lambda value: value != "[\n\n]\n")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("mapreduce.output.fileoutputformat.compress", "false")
    hadoop_conf.set("mapred.output.compress", "false")
    json_arrays.saveAsTextFile(args.output)
    rename_part_files_as_json(spark, args.output)
    cleanup_non_json_output_files(spark, args.output)
    print(json.dumps({"databases": dbs.count(), "tables": base.count(), "mcps": events.count()}))
    spark.stop()


if __name__ == "__main__":
    main()
