# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze incremental ingestion
# MAGIC
# MAGIC Incrementally ingests supply-chain CSV and JSON files from the Unity Catalog
# MAGIC landing volume into managed Delta tables using Databricks Auto Loader.
# MAGIC
# MAGIC Bronze design principles:
# MAGIC - Preserve source values as strings.
# MAGIC - Append records without business cleansing.
# MAGIC - Capture file lineage and ingestion metadata.
# MAGIC - Rescue unexpected fields instead of silently dropping them.
# MAGIC - Use independent checkpoints for every source.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.sql("SELECT current_catalog()").first()[0]

BRONZE_SCHEMA = "scm_bronze"
CONTROL_SCHEMA = "scm_control"

LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
PIPELINE_PATH = (
    f"/Volumes/{CATALOG}/{CONTROL_SCHEMA}/pipeline_files"
    "/bronze_autoloader"
)

RUN_ID = str(uuid4())

# Keep False during normal operation.
# Change to True only when you intentionally want to rebuild every Bronze table
# from the source files.
RESET_BRONZE = False

print(f"Catalog: {CATALOG}")
print(f"Landing path: {LANDING_PATH}")
print(f"Pipeline path: {PIPELINE_PATH}")
print(f"Run ID: {RUN_ID}")
print(f"Reset Bronze: {RESET_BRONZE}")

# COMMAND ----------

def string_schema(*column_names: str) -> StructType:
    """Return a nullable all-string schema for raw Bronze ingestion."""
    return StructType(
        [
            StructField(column_name, StringType(), True)
            for column_name in column_names
        ]
    )


SOURCE_CONFIG = {
    "suppliers": {
        "format": "csv",
        "schema": string_schema(
            "supplier_id",
            "supplier_name",
            "country",
            "lead_time_days",
            "reliability_score",
            "active_flag",
            "updated_at",
        ),
        "table": "bronze_suppliers",
    },
    "products": {
        "format": "csv",
        "schema": string_schema(
            "product_id",
            "sku",
            "product_name",
            "category",
            "brand",
            "package_size",
            "unit_price",
            "unit_cost",
            "supplier_id",
            "weight_kg",
            "active_flag",
            "updated_at",
        ),
        "table": "bronze_products",
    },
    "warehouses": {
        "format": "csv",
        "schema": string_schema(
            "warehouse_id",
            "warehouse_name",
            "province",
            "city",
            "capacity_units",
            "opened_date",
            "status",
            "updated_at",
        ),
        "table": "bronze_warehouses",
    },
    "stores": {
        "format": "csv",
        "schema": string_schema(
            "store_id",
            "store_name",
            "channel",
            "province",
            "city",
            "priority_level",
            "active_flag",
            "updated_at",
        ),
        "table": "bronze_stores",
    },
    "orders": {
        "format": "csv",
        "schema": string_schema(
            "order_id",
            "store_id",
            "order_date",
            "requested_delivery_date",
            "order_status",
            "currency",
            "source_system",
            "updated_at",
        ),
        "table": "bronze_orders",
    },
    "order_items": {
        "format": "csv",
        "schema": string_schema(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity_ordered",
            "unit_price",
            "discount_pct",
            "line_amount",
            "updated_at",
        ),
        "table": "bronze_order_items",
    },
    "shipments": {
        "format": "csv",
        "schema": string_schema(
            "shipment_id",
            "order_id",
            "warehouse_id",
            "shipment_date",
            "actual_delivery_date",
            "carrier",
            "shipment_status",
            "shipped_quantity",
            "delivered_quantity",
            "shipping_cost",
            "updated_at",
        ),
        "table": "bronze_shipments",
    },
    "inventory_snapshots": {
        "format": "json",
        "schema": string_schema(
            "snapshot_date",
            "warehouse_id",
            "product_id",
            "on_hand_qty",
            "reserved_qty",
            "available_qty",
            "reorder_point",
            "updated_at",
        ),
        "table": "bronze_inventory_snapshots",
    },
}

print(f"Configured sources: {len(SOURCE_CONFIG)}")
print(list(SOURCE_CONFIG.keys()))

# COMMAND ----------

AUDIT_TABLE = (
    f"{CATALOG}.{CONTROL_SCHEMA}.bronze_load_audit"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        run_id STRING,
        source_name STRING,
        target_table STRING,
        status STRING,
        input_rows BIGINT,
        total_table_rows BIGINT,
        distinct_source_files BIGINT,
        rescued_rows BIGINT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    )
    COMMENT 'Execution-level audit records for Bronze Auto Loader ingestion'
    """
)

print(f"Audit table ready: {AUDIT_TABLE}")

# COMMAND ----------

if RESET_BRONZE:
    print("Resetting Bronze tables, checkpoints, schemas, and audit history.")

    for source_name, config in SOURCE_CONFIG.items():
        target_table = (
            f"{CATALOG}.{BRONZE_SCHEMA}.{config['table']}"
        )

        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        dbutils.fs.rm(
            f"{PIPELINE_PATH}/{source_name}",
            recurse=True,
        )

    spark.sql(f"TRUNCATE TABLE {AUDIT_TABLE}")
    print("Bronze reset completed.")
else:
    print("Normal incremental mode: existing state will be preserved.")

# COMMAND ----------

def source_reader(source_name: str, config: dict):
    """Create an Auto Loader streaming DataFrame for one source."""
    input_path = f"{LANDING_PATH}/{source_name}"
    schema_path = (
        f"{PIPELINE_PATH}/{source_name}/schema"
    )

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config["format"])
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("mode", "PERMISSIVE")
        .schema(config["schema"])
    )

    if config["format"] == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("delimiter", ",")
            .option("quote", '"')
            .option("escape", '"')
        )

    raw_df = reader.load(input_path)

    batch_expression = F.regexp_extract(
        F.col("_metadata.file_name"),
        r"(batch_[0-9]+)",
        1,
    )

    return (
        raw_df
        .select(
            "*",
            F.col("_metadata.file_path").alias(
                "_source_file_path"
            ),
            F.col("_metadata.file_name").alias(
                "_source_file_name"
            ),
            F.col("_metadata.file_size").alias(
                "_source_file_size"
            ),
            F.col(
                "_metadata.file_modification_time"
            ).alias("_source_file_modification_time"),
        )
        .withColumn(
            "_batch_id",
            F.when(
                F.length(batch_expression) > 0,
                batch_expression,
            ).otherwise(F.lit("master")),
        )
        .withColumn(
            "_source_name",
            F.lit(source_name),
        )
        .withColumn(
            "_ingestion_run_id",
            F.lit(RUN_ID),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )


def streaming_input_rows(query) -> int:
    """Sum input rows from completed progress events safely."""
    total_rows = 0

    for progress in query.recentProgress or []:
        num_input_rows = progress.get("numInputRows")

        if num_input_rows is not None:
            total_rows += int(num_input_rows)

    return total_rows


def append_audit_record(
    source_name: str,
    target_table: str,
    status: str,
    input_rows: int,
    total_table_rows: int,
    distinct_source_files: int,
    rescued_rows: int,
    started_at: datetime,
    finished_at: datetime,
    error_message=None,
):
    audit_record = [
        (
            RUN_ID,
            source_name,
            target_table,
            status,
            input_rows,
            total_table_rows,
            distinct_source_files,
            rescued_rows,
            started_at,
            finished_at,
            error_message,
        )
    ]

    audit_schema = """
        run_id STRING,
        source_name STRING,
        target_table STRING,
        status STRING,
        input_rows LONG,
        total_table_rows LONG,
        distinct_source_files LONG,
        rescued_rows LONG,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    """

    (
        spark.createDataFrame(
            audit_record,
            schema=audit_schema,
        )
        .write
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


def ingest_source(source_name: str, config: dict) -> dict:
    """Ingest one source into a Unity Catalog managed Delta table."""
    target_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{config['table']}"
    )
    checkpoint_path = (
        f"{PIPELINE_PATH}/{source_name}/checkpoint"
    )

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    input_rows = 0

    print("-" * 80)
    print(f"Source: {source_name}")
    print(f"Target: {target_table}")
    print(f"Checkpoint: {checkpoint_path}")

    try:
        bronze_df = source_reader(source_name, config)

        query = (
            bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                checkpoint_path,
            )
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        query.awaitTermination()

        input_rows = streaming_input_rows(query)

        target_df = spark.table(target_table)

        total_table_rows = target_df.count()
        distinct_source_files = (
            target_df
            .select("_source_file_path")
            .distinct()
            .count()
        )
        rescued_rows = (
            target_df
            .filter(F.col("_rescued_data").isNotNull())
            .count()
        )

        finished_at = (
            datetime.now(timezone.utc).replace(tzinfo=None)
        )

        append_audit_record(
            source_name=source_name,
            target_table=target_table,
            status="SUCCESS",
            input_rows=input_rows,
            total_table_rows=total_table_rows,
            distinct_source_files=distinct_source_files,
            rescued_rows=rescued_rows,
            started_at=started_at,
            finished_at=finished_at,
        )

        result = {
            "source_name": source_name,
            "target_table": target_table,
            "status": "SUCCESS",
            "input_rows": input_rows,
            "total_table_rows": total_table_rows,
            "distinct_source_files": distinct_source_files,
            "rescued_rows": rescued_rows,
        }

        print(result)
        return result

    except Exception as error:
        finished_at = (
            datetime.now(timezone.utc).replace(tzinfo=None)
        )

        error_message = str(error)[:4000]

        append_audit_record(
            source_name=source_name,
            target_table=target_table,
            status="FAILED",
            input_rows=input_rows,
            total_table_rows=0,
            distinct_source_files=0,
            rescued_rows=0,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
        )

        print(f"FAILED: {source_name}")
        print(error_message)
        raise

# COMMAND ----------

ingestion_results = []

for source_name, config in SOURCE_CONFIG.items():
    result = ingest_source(source_name, config)
    ingestion_results.append(result)

results_df = spark.createDataFrame(ingestion_results)
display(results_df.orderBy("source_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected first-run result
# MAGIC
# MAGIC The exact order-item count is generated by the previous notebook, but the
# MAGIC remaining counts should normally be:
# MAGIC
# MAGIC - suppliers: 15
# MAGIC - products: 70
# MAGIC - warehouses: 5
# MAGIC - stores: 61
# MAGIC - orders: 501
# MAGIC - shipments: 500
# MAGIC - inventory_snapshots: 350
# MAGIC
# MAGIC `input_rows` should match `total_table_rows` on the first run.

# COMMAND ----------

# Consolidated table counts.
table_count_rows = []

for source_name, config in SOURCE_CONFIG.items():
    target_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{config['table']}"
    )

    table_count_rows.append(
        (
            source_name,
            target_table,
            spark.table(target_table).count(),
        )
    )

table_counts_df = spark.createDataFrame(
    table_count_rows,
    [
        "source_name",
        "target_table",
        "row_count",
    ],
)

display(table_counts_df.orderBy("source_name"))

# COMMAND ----------

# Verify that the metadata needed for traceability is present.
metadata_validation_rows = []

for source_name, config in SOURCE_CONFIG.items():
    target_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{config['table']}"
    )

    table_df = spark.table(target_table)

    null_metadata_rows = (
        table_df
        .filter(
            F.col("_source_file_path").isNull()
            | F.col("_source_file_name").isNull()
            | F.col("_ingested_at").isNull()
            | F.col("_ingestion_run_id").isNull()
        )
        .count()
    )

    metadata_validation_rows.append(
        (
            source_name,
            table_df.count(),
            null_metadata_rows,
            null_metadata_rows == 0,
        )
    )

metadata_validation_df = spark.createDataFrame(
    metadata_validation_rows,
    [
        "source_name",
        "total_rows",
        "rows_with_missing_metadata",
        "passed",
    ],
)

display(metadata_validation_df.orderBy("source_name"))

# COMMAND ----------

# Inspect raw records and lineage.
display(
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_orders"
    )
    .select(
        "order_id",
        "store_id",
        "order_date",
        "order_status",
        "_batch_id",
        "_source_file_name",
        "_ingested_at",
        "_rescued_data",
    )
    .orderBy("order_id")
    .limit(20)
)

# COMMAND ----------

# Review the audit records for this run.
display(
    spark.table(AUDIT_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .orderBy("source_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency test
# MAGIC
# MAGIC Run this entire notebook again without changing `RESET_BRONZE`.
# MAGIC Auto Loader should report `input_rows = 0` for all eight sources, while
# MAGIC `total_table_rows` remains unchanged. This proves that existing files are
# MAGIC not ingested twice because each source has its own checkpoint.

# COMMAND ----------

# Optional SQL-style inspection.
spark.sql(
    f"""
    SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}
    """
).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN scm_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     store_id,
# MAGIC     order_date,
# MAGIC     order_status,
# MAGIC     _batch_id,
# MAGIC     _source_file_name,
# MAGIC     _ingested_at
# MAGIC FROM scm_bronze.bronze_orders
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     COUNT(*) AS occurrences
# MAGIC FROM scm_bronze.bronze_orders
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_bronze.bronze_orders
# MAGIC WHERE order_date = '2026-13-40';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     source_name,
# MAGIC     status,
# MAGIC     input_rows,
# MAGIC     total_table_rows,
# MAGIC     distinct_source_files,
# MAGIC     rescued_rows,
# MAGIC     started_at,
# MAGIC     finished_at
# MAGIC FROM scm_control.bronze_load_audit
# MAGIC ORDER BY started_at DESC, source_name;