# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver transformation and data quality
# MAGIC
# MAGIC Builds typed, deduplicated, validated Silver tables from Bronze.
# MAGIC
# MAGIC The notebook:
# MAGIC - standardizes strings and data types;
# MAGIC - keeps the most recent record for each business key;
# MAGIC - validates business rules and referential integrity;
# MAGIC - stores accepted records in Silver Delta tables;
# MAGIC - stores invalid and duplicate records in quarantine tables;
# MAGIC - records execution metrics in an audit table;
# MAGIC - synchronizes Silver tables with Delta Lake MERGE.
# MAGIC - avoids Spark cache APIs for Databricks Serverless compatibility.

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]

BRONZE_SCHEMA = "scm_bronze"
SILVER_SCHEMA = "scm_silver"
CONTROL_SCHEMA = "scm_control"

SILVER_RUN_ID = str(uuid4())

# Keep False during normal operation.
# Change to True only when you intentionally want to rebuild all Silver and
# quarantine tables.
RESET_SILVER = False

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Silver run ID: {SILVER_RUN_ID}")
print(f"Reset Silver: {RESET_SILVER}")

# COMMAND ----------

METADATA_COLUMNS = [
    "_source_file_path",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modification_time",
    "_batch_id",
    "_source_name",
    "_ingestion_run_id",
    "_ingested_at",
    "_rescued_data",
]


def normalize_string(column_name: str):
    """Trim a string and convert blank values to null."""
    value = F.trim(F.col(column_name))
    return F.when(value == "", F.lit(None)).otherwise(value)


def try_cast_column(column_name: str, data_type: str):
    """
    Safely cast a trimmed source value.

    Databricks SQL `try_cast` returns null for malformed values instead of
    stopping the complete ETL job, allowing the row to be quarantined.
    """
    return F.expr(
        "try_cast("
        f"NULLIF(TRIM(CAST(`{column_name}` AS STRING)), '') "
        f"AS {data_type}"
        ")"
    )


def parse_boolean(column_name: str):
    """Parse common text representations of booleans."""
    value = F.lower(normalize_string(column_name))

    return (
        F.when(value.isin("true", "1", "yes", "y"), F.lit(True))
        .when(value.isin("false", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def raw_record_json(df: DataFrame, business_columns: list[str]):
    """Serialize original business columns for troubleshooting."""
    return F.to_json(
        F.struct(
            *[
                F.col(column_name).alias(column_name)
                for column_name in business_columns
                if column_name in df.columns
            ]
        )
    )


def rescued_data_present():
    """Return a condition that identifies rescued schema content."""
    return (
        F.col("_rescued_data").isNotNull()
        & (F.length(F.trim(F.col("_rescued_data"))) > 0)
    )


def add_quality_results(
    df: DataFrame,
    rules: list[tuple],
) -> DataFrame:
    """
    Add a readable error list, error count, and final data-quality status.

    Each rule is a tuple:
        (invalid_condition, "ERROR_CODE")
    """
    error_messages = [
        F.when(condition, F.lit(error_code))
        for condition, error_code in rules
    ]

    error_count = reduce(
        lambda total, condition: (
            total
            + F.when(condition, F.lit(1)).otherwise(F.lit(0))
        ),
        [condition for condition, _ in rules],
        F.lit(0),
    )

    return (
        df
        .withColumn(
            "_dq_errors",
            F.concat_ws("; ", *error_messages),
        )
        .withColumn("_dq_error_count", error_count)
        .withColumn(
            "_dq_status",
            F.when(
                F.col("_dq_error_count") == 0,
                F.lit("VALID"),
            ).otherwise(F.lit("REJECTED")),
        )
        .withColumn(
            "_silver_run_id",
            F.lit(SILVER_RUN_ID),
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp(),
        )
    )


def attach_reference_flag(
    df: DataFrame,
    local_column: str,
    reference_table: str,
    reference_column: str,
    flag_column: str,
) -> DataFrame:
    """Left join to a valid Silver table and add an existence flag."""
    reference_alias = f"__{flag_column}_reference"

    reference_df = (
        spark.table(reference_table)
        .select(
            F.col(reference_column).alias(reference_alias)
        )
        .dropDuplicates()
    )

    return (
        df
        .join(
            F.broadcast(reference_df),
            F.col(local_column)
            == F.col(reference_alias),
            "left",
        )
        .withColumn(
            flag_column,
            F.col(reference_alias).isNotNull(),
        )
        .drop(reference_alias)
    )


def split_latest_and_duplicates(
    df: DataFrame,
    key_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """
    Keep the latest record for every complete business key.

    Rows with incomplete keys are not considered duplicates; they continue to
    validation so that they can be rejected for their actual missing-key error.
    """
    complete_key_condition = reduce(
        lambda left, right: left & right,
        [F.col(column_name).isNotNull() for column_name in key_columns],
    )

    complete_key_df = df.filter(complete_key_condition)
    incomplete_key_df = df.filter(~complete_key_condition)

    dedupe_window = (
        Window
        .partitionBy(*key_columns)
        .orderBy(
            F.col("updated_at").desc_nulls_last(),
            F.col("_ingested_at").desc_nulls_last(),
            F.col(
                "_source_file_modification_time"
            ).desc_nulls_last(),
            F.col("_source_file_name").desc_nulls_last(),
        )
    )

    ranked_df = complete_key_df.withColumn(
        "_dedupe_rank",
        F.row_number().over(dedupe_window),
    )

    latest_complete_df = (
        ranked_df
        .filter(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )

    duplicate_df = (
        ranked_df
        .filter(F.col("_dedupe_rank") > 1)
        .drop("_dedupe_rank")
        .withColumn(
            "_dq_errors",
            F.when(
                F.length(F.col("_dq_errors")) > 0,
                F.concat(
                    F.col("_dq_errors"),
                    F.lit("; DUPLICATE_BUSINESS_KEY"),
                ),
            ).otherwise(
                F.lit("DUPLICATE_BUSINESS_KEY")
            ),
        )
        .withColumn(
            "_dq_error_count",
            F.col("_dq_error_count") + 1,
        )
        .withColumn(
            "_dq_status",
            F.lit("REJECTED"),
        )
    )

    latest_df = latest_complete_df.unionByName(
        incomplete_key_df,
        allowMissingColumns=True,
    )

    return latest_df, duplicate_df


def merge_condition(
    key_columns: list[str],
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    """Build a null-safe Delta MERGE condition for one or more keys."""
    return " AND ".join(
        [
            (
                f"{target_alias}.`{column_name}` "
                f"<=> {source_alias}.`{column_name}`"
            )
            for column_name in key_columns
        ]
    )


def synchronize_delta_table(
    df: DataFrame,
    table_name: str,
    key_columns: list[str],
):
    """
    Synchronize a managed Delta table with the current valid Silver dataset.

    MERGE updates changed rows, inserts new rows, and deletes target rows that
    are no longer present in the valid source dataset.
    """
    if not spark.catalog.tableExists(table_name):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        return

    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_condition(key_columns),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def overwrite_quarantine(
    df: DataFrame,
    table_name: str,
):
    """Store the current complete quarantine result deterministically."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def add_rejection_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "_rejected_at",
        F.current_timestamp(),
    )

# COMMAND ----------

SILVER_AUDIT_TABLE = (
    f"{CATALOG}.{CONTROL_SCHEMA}.silver_load_audit"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SILVER_AUDIT_TABLE} (
        run_id STRING,
        source_name STRING,
        bronze_table STRING,
        silver_table STRING,
        quarantine_table STRING,
        status STRING,
        bronze_rows BIGINT,
        latest_rows BIGINT,
        accepted_rows BIGINT,
        rejected_rows BIGINT,
        duplicate_rows BIGINT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    )
    COMMENT 'Execution audit for Silver transformation and data-quality jobs'
    """
)

print(f"Audit table ready: {SILVER_AUDIT_TABLE}")

# COMMAND ----------

def append_silver_audit(
    source_name: str,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
    status: str,
    bronze_rows: int,
    latest_rows: int,
    accepted_rows: int,
    rejected_rows: int,
    duplicate_rows: int,
    started_at: datetime,
    finished_at: datetime,
    error_message=None,
):
    audit_rows = [
        (
            SILVER_RUN_ID,
            source_name,
            bronze_table,
            silver_table,
            quarantine_table,
            status,
            bronze_rows,
            latest_rows,
            accepted_rows,
            rejected_rows,
            duplicate_rows,
            started_at,
            finished_at,
            error_message,
        )
    ]

    audit_schema = """
        run_id STRING,
        source_name STRING,
        bronze_table STRING,
        silver_table STRING,
        quarantine_table STRING,
        status STRING,
        bronze_rows LONG,
        latest_rows LONG,
        accepted_rows LONG,
        rejected_rows LONG,
        duplicate_rows LONG,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    """

    (
        spark.createDataFrame(
            audit_rows,
            schema=audit_schema,
        )
        .write
        .mode("append")
        .saveAsTable(SILVER_AUDIT_TABLE)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source-specific transformations and validation rules

# COMMAND ----------

def build_suppliers(df: DataFrame) -> DataFrame:
    columns = [
        "supplier_id",
        "supplier_name",
        "country",
        "lead_time_days",
        "reliability_score",
        "active_flag",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "supplier_id",
            F.upper(normalize_string("supplier_id")),
        )
        .withColumn(
            "supplier_name",
            normalize_string("supplier_name"),
        )
        .withColumn(
            "country",
            F.initcap(normalize_string("country")),
        )
        .withColumn(
            "lead_time_days",
            try_cast_column("lead_time_days", "INT"),
        )
        .withColumn(
            "reliability_score",
            try_cast_column("reliability_score", "DOUBLE"),
        )
        .withColumn(
            "active_flag",
            parse_boolean("active_flag"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    rules = [
        (F.col("supplier_id").isNull(), "MISSING_SUPPLIER_ID"),
        (F.col("supplier_name").isNull(), "MISSING_SUPPLIER_NAME"),
        (F.col("country").isNull(), "MISSING_COUNTRY"),
        (
            F.col("lead_time_days").isNull()
            | (F.col("lead_time_days") <= 0)
            | (F.col("lead_time_days") > 365),
            "INVALID_LEAD_TIME_DAYS",
        ),
        (
            F.col("reliability_score").isNull()
            | (F.col("reliability_score") < 0)
            | (F.col("reliability_score") > 1),
            "INVALID_RELIABILITY_SCORE",
        ),
        (F.col("active_flag").isNull(), "INVALID_ACTIVE_FLAG"),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return add_quality_results(typed_df, rules)


def build_warehouses(df: DataFrame) -> DataFrame:
    columns = [
        "warehouse_id",
        "warehouse_name",
        "province",
        "city",
        "capacity_units",
        "opened_date",
        "status",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "warehouse_id",
            F.upper(normalize_string("warehouse_id")),
        )
        .withColumn(
            "warehouse_name",
            normalize_string("warehouse_name"),
        )
        .withColumn(
            "province",
            F.initcap(normalize_string("province")),
        )
        .withColumn(
            "city",
            F.initcap(normalize_string("city")),
        )
        .withColumn(
            "capacity_units",
            try_cast_column("capacity_units", "BIGINT"),
        )
        .withColumn(
            "opened_date",
            try_cast_column("opened_date", "DATE"),
        )
        .withColumn(
            "status",
            F.upper(normalize_string("status")),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    rules = [
        (F.col("warehouse_id").isNull(), "MISSING_WAREHOUSE_ID"),
        (F.col("warehouse_name").isNull(), "MISSING_WAREHOUSE_NAME"),
        (F.col("province").isNull(), "MISSING_PROVINCE"),
        (F.col("city").isNull(), "MISSING_CITY"),
        (
            F.col("capacity_units").isNull()
            | (F.col("capacity_units") <= 0),
            "INVALID_CAPACITY_UNITS",
        ),
        (F.col("opened_date").isNull(), "INVALID_OPENED_DATE"),
        (
            F.col("status").isNull()
            | ~F.col("status").isin(
                "ACTIVE",
                "INACTIVE",
                "CLOSED",
            ),
            "INVALID_WAREHOUSE_STATUS",
        ),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return add_quality_results(typed_df, rules)


def build_stores(df: DataFrame) -> DataFrame:
    columns = [
        "store_id",
        "store_name",
        "channel",
        "province",
        "city",
        "priority_level",
        "active_flag",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "store_id",
            F.upper(normalize_string("store_id")),
        )
        .withColumn(
            "store_name",
            normalize_string("store_name"),
        )
        .withColumn(
            "channel",
            F.upper(normalize_string("channel")),
        )
        .withColumn(
            "province",
            F.initcap(normalize_string("province")),
        )
        .withColumn(
            "city",
            F.initcap(normalize_string("city")),
        )
        .withColumn(
            "priority_level",
            F.upper(normalize_string("priority_level")),
        )
        .withColumn(
            "active_flag",
            parse_boolean("active_flag"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    rules = [
        (F.col("store_id").isNull(), "MISSING_STORE_ID"),
        (F.col("store_name").isNull(), "MISSING_STORE_NAME"),
        (
            F.col("channel").isNull()
            | ~F.col("channel").isin(
                "SUPERMARKET",
                "PHARMACY",
                "WHOLESALE",
                "CONVENIENCE",
                "E_COMMERCE",
            ),
            "INVALID_STORE_CHANNEL",
        ),
        (F.col("province").isNull(), "MISSING_PROVINCE"),
        (F.col("city").isNull(), "MISSING_CITY"),
        (
            F.col("priority_level").isNull()
            | ~F.col("priority_level").isin(
                "HIGH",
                "MEDIUM",
                "LOW",
            ),
            "INVALID_PRIORITY_LEVEL",
        ),
        (F.col("active_flag").isNull(), "INVALID_ACTIVE_FLAG"),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return add_quality_results(typed_df, rules)


def build_products(df: DataFrame) -> DataFrame:
    columns = [
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
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "product_id",
            F.upper(normalize_string("product_id")),
        )
        .withColumn(
            "sku",
            F.upper(normalize_string("sku")),
        )
        .withColumn(
            "product_name",
            normalize_string("product_name"),
        )
        .withColumn(
            "category",
            F.initcap(normalize_string("category")),
        )
        .withColumn(
            "brand",
            F.initcap(normalize_string("brand")),
        )
        .withColumn(
            "package_size",
            F.initcap(normalize_string("package_size")),
        )
        .withColumn(
            "unit_price",
            try_cast_column("unit_price", "DECIMAL(12,2)"),
        )
        .withColumn(
            "unit_cost",
            try_cast_column("unit_cost", "DECIMAL(12,2)"),
        )
        .withColumn(
            "supplier_id",
            F.upper(normalize_string("supplier_id")),
        )
        .withColumn(
            "weight_kg",
            try_cast_column("weight_kg", "DECIMAL(12,3)"),
        )
        .withColumn(
            "active_flag",
            parse_boolean("active_flag"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="supplier_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_suppliers"
        ),
        reference_column="supplier_id",
        flag_column="_supplier_exists",
    )

    rules = [
        (F.col("product_id").isNull(), "MISSING_PRODUCT_ID"),
        (F.col("sku").isNull(), "MISSING_SKU"),
        (F.col("product_name").isNull(), "MISSING_PRODUCT_NAME"),
        (F.col("category").isNull(), "MISSING_CATEGORY"),
        (F.col("brand").isNull(), "MISSING_BRAND"),
        (F.col("package_size").isNull(), "MISSING_PACKAGE_SIZE"),
        (
            F.col("unit_price").isNull()
            | (F.col("unit_price") <= 0),
            "INVALID_UNIT_PRICE",
        ),
        (
            F.col("unit_cost").isNull()
            | (F.col("unit_cost") < 0),
            "INVALID_UNIT_COST",
        ),
        (
            F.col("unit_price").isNotNull()
            & F.col("unit_cost").isNotNull()
            & (F.col("unit_price") < F.col("unit_cost")),
            "PRICE_BELOW_COST",
        ),
        (F.col("supplier_id").isNull(), "MISSING_SUPPLIER_ID"),
        (
            F.col("supplier_id").isNotNull()
            & ~F.col("_supplier_exists"),
            "SUPPLIER_NOT_FOUND",
        ),
        (
            F.col("weight_kg").isNull()
            | (F.col("weight_kg") <= 0),
            "INVALID_WEIGHT_KG",
        ),
        (F.col("active_flag").isNull(), "INVALID_ACTIVE_FLAG"),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return (
        add_quality_results(typed_df, rules)
        .drop("_supplier_exists")
    )


def build_orders(df: DataFrame) -> DataFrame:
    columns = [
        "order_id",
        "store_id",
        "order_date",
        "requested_delivery_date",
        "order_status",
        "currency",
        "source_system",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "order_id",
            F.upper(normalize_string("order_id")),
        )
        .withColumn(
            "store_id",
            F.upper(normalize_string("store_id")),
        )
        .withColumn(
            "order_date",
            try_cast_column("order_date", "DATE"),
        )
        .withColumn(
            "requested_delivery_date",
            try_cast_column(
                "requested_delivery_date",
                "DATE",
            ),
        )
        .withColumn(
            "order_status",
            F.upper(normalize_string("order_status")),
        )
        .withColumn(
            "currency",
            F.upper(normalize_string("currency")),
        )
        .withColumn(
            "source_system",
            F.upper(normalize_string("source_system")),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="store_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_stores"
        ),
        reference_column="store_id",
        flag_column="_store_exists",
    )

    rules = [
        (F.col("order_id").isNull(), "MISSING_ORDER_ID"),
        (F.col("store_id").isNull(), "MISSING_STORE_ID"),
        (
            F.col("store_id").isNotNull()
            & ~F.col("_store_exists"),
            "STORE_NOT_FOUND",
        ),
        (F.col("order_date").isNull(), "INVALID_ORDER_DATE"),
        (
            F.col("requested_delivery_date").isNull(),
            "INVALID_REQUESTED_DELIVERY_DATE",
        ),
        (
            F.col("order_date").isNotNull()
            & F.col("requested_delivery_date").isNotNull()
            & (
                F.col("requested_delivery_date")
                < F.col("order_date")
            ),
            "REQUESTED_DATE_BEFORE_ORDER_DATE",
        ),
        (
            F.col("order_status").isNull()
            | ~F.col("order_status").isin(
                "CREATED",
                "PROCESSING",
                "SHIPPED",
                "DELIVERED",
                "CANCELLED",
            ),
            "INVALID_ORDER_STATUS",
        ),
        (
            F.col("currency").isNull()
            | (F.col("currency") != "USD"),
            "INVALID_CURRENCY",
        ),
        (
            F.col("source_system").isNull()
            | ~F.col("source_system").isin(
                "ERP",
                "B2B_PORTAL",
                "MOBILE_SALES",
            ),
            "INVALID_SOURCE_SYSTEM",
        ),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return (
        add_quality_results(typed_df, rules)
        .drop("_store_exists")
    )


def build_order_items(df: DataFrame) -> DataFrame:
    columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity_ordered",
        "unit_price",
        "discount_pct",
        "line_amount",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "order_item_id",
            F.upper(normalize_string("order_item_id")),
        )
        .withColumn(
            "order_id",
            F.upper(normalize_string("order_id")),
        )
        .withColumn(
            "product_id",
            F.upper(normalize_string("product_id")),
        )
        .withColumn(
            "quantity_ordered",
            try_cast_column("quantity_ordered", "INT"),
        )
        .withColumn(
            "unit_price",
            try_cast_column("unit_price", "DECIMAL(12,2)"),
        )
        .withColumn(
            "discount_pct",
            try_cast_column("discount_pct", "DOUBLE"),
        )
        .withColumn(
            "line_amount",
            try_cast_column("line_amount", "DECIMAL(14,2)"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="order_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_orders"
        ),
        reference_column="order_id",
        flag_column="_order_exists",
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="product_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_products"
        ),
        reference_column="product_id",
        flag_column="_product_exists",
    )

    expected_line_amount = (
        F.col("quantity_ordered").cast("double")
        * F.col("unit_price").cast("double")
        * (F.lit(1.0) - F.col("discount_pct"))
    )

    rules = [
        (
            F.col("order_item_id").isNull(),
            "MISSING_ORDER_ITEM_ID",
        ),
        (F.col("order_id").isNull(), "MISSING_ORDER_ID"),
        (
            F.col("order_id").isNotNull()
            & ~F.col("_order_exists"),
            "ORDER_NOT_FOUND",
        ),
        (F.col("product_id").isNull(), "MISSING_PRODUCT_ID"),
        (
            F.col("product_id").isNotNull()
            & ~F.col("_product_exists"),
            "PRODUCT_NOT_FOUND",
        ),
        (
            F.col("quantity_ordered").isNull()
            | (F.col("quantity_ordered") <= 0),
            "INVALID_QUANTITY_ORDERED",
        ),
        (
            F.col("unit_price").isNull()
            | (F.col("unit_price") <= 0),
            "INVALID_UNIT_PRICE",
        ),
        (
            F.col("discount_pct").isNull()
            | (F.col("discount_pct") < 0)
            | (F.col("discount_pct") > 1),
            "INVALID_DISCOUNT_PCT",
        ),
        (
            F.col("line_amount").isNull()
            | (F.col("line_amount") < 0),
            "INVALID_LINE_AMOUNT",
        ),
        (
            F.col("quantity_ordered").isNotNull()
            & F.col("unit_price").isNotNull()
            & F.col("discount_pct").isNotNull()
            & F.col("line_amount").isNotNull()
            & (
                F.abs(
                    F.col("line_amount").cast("double")
                    - expected_line_amount
                )
                > F.lit(0.02)
            ),
            "LINE_AMOUNT_MISMATCH",
        ),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return (
        add_quality_results(typed_df, rules)
        .drop("_order_exists", "_product_exists")
    )


def build_shipments(df: DataFrame) -> DataFrame:
    columns = [
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
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "shipment_id",
            F.upper(normalize_string("shipment_id")),
        )
        .withColumn(
            "order_id",
            F.upper(normalize_string("order_id")),
        )
        .withColumn(
            "warehouse_id",
            F.upper(normalize_string("warehouse_id")),
        )
        .withColumn(
            "shipment_date",
            try_cast_column("shipment_date", "DATE"),
        )
        .withColumn(
            "_actual_delivery_date_raw",
            normalize_string("actual_delivery_date"),
        )
        .withColumn(
            "actual_delivery_date",
            try_cast_column("_actual_delivery_date_raw", "DATE"),
        )
        .withColumn(
            "carrier",
            normalize_string("carrier"),
        )
        .withColumn(
            "shipment_status",
            F.upper(normalize_string("shipment_status")),
        )
        .withColumn(
            "shipped_quantity",
            try_cast_column("shipped_quantity", "INT"),
        )
        .withColumn(
            "delivered_quantity",
            try_cast_column("delivered_quantity", "INT"),
        )
        .withColumn(
            "shipping_cost",
            try_cast_column("shipping_cost", "DECIMAL(14,2)"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="order_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_orders"
        ),
        reference_column="order_id",
        flag_column="_order_exists",
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="warehouse_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_warehouses"
        ),
        reference_column="warehouse_id",
        flag_column="_warehouse_exists",
    )

    rules = [
        (F.col("shipment_id").isNull(), "MISSING_SHIPMENT_ID"),
        (F.col("order_id").isNull(), "MISSING_ORDER_ID"),
        (
            F.col("order_id").isNotNull()
            & ~F.col("_order_exists"),
            "ORDER_NOT_FOUND",
        ),
        (F.col("warehouse_id").isNull(), "MISSING_WAREHOUSE_ID"),
        (
            F.col("warehouse_id").isNotNull()
            & ~F.col("_warehouse_exists"),
            "WAREHOUSE_NOT_FOUND",
        ),
        (
            F.col("shipment_date").isNull(),
            "INVALID_SHIPMENT_DATE",
        ),
        (
            F.col("_actual_delivery_date_raw").isNotNull()
            & F.col("actual_delivery_date").isNull(),
            "INVALID_ACTUAL_DELIVERY_DATE",
        ),
        (
            F.col("shipment_date").isNotNull()
            & F.col("actual_delivery_date").isNotNull()
            & (
                F.col("actual_delivery_date")
                < F.col("shipment_date")
            ),
            "DELIVERY_BEFORE_SHIPMENT",
        ),
        (
            (F.col("shipment_status") == "DELIVERED")
            & F.col("actual_delivery_date").isNull(),
            "DELIVERED_STATUS_REQUIRES_ACTUAL_DATE",
        ),
        (
            F.col("shipment_status").isNull()
            | ~F.col("shipment_status").isin(
                "CREATED",
                "PROCESSING",
                "IN_TRANSIT",
                "DELIVERED",
                "CANCELLED",
            ),
            "INVALID_SHIPMENT_STATUS",
        ),
        (
            F.col("shipped_quantity").isNull()
            | (F.col("shipped_quantity") <= 0),
            "INVALID_SHIPPED_QUANTITY",
        ),
        (
            F.col("delivered_quantity").isNull()
            | (F.col("delivered_quantity") < 0),
            "INVALID_DELIVERED_QUANTITY",
        ),
        (
            F.col("shipped_quantity").isNotNull()
            & F.col("delivered_quantity").isNotNull()
            & (
                F.col("delivered_quantity")
                > F.col("shipped_quantity")
            ),
            "DELIVERED_EXCEEDS_SHIPPED",
        ),
        (
            F.col("shipping_cost").isNull()
            | (F.col("shipping_cost") < 0),
            "INVALID_SHIPPING_COST",
        ),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return (
        add_quality_results(typed_df, rules)
        .drop(
            "_order_exists",
            "_warehouse_exists",
            "_actual_delivery_date_raw",
        )
    )


def build_inventory_snapshots(df: DataFrame) -> DataFrame:
    columns = [
        "snapshot_date",
        "warehouse_id",
        "product_id",
        "on_hand_qty",
        "reserved_qty",
        "available_qty",
        "reorder_point",
        "updated_at",
    ]

    typed_df = (
        df
        .withColumn("_raw_record", raw_record_json(df, columns))
        .withColumn(
            "snapshot_date",
            try_cast_column("snapshot_date", "DATE"),
        )
        .withColumn(
            "warehouse_id",
            F.upper(normalize_string("warehouse_id")),
        )
        .withColumn(
            "product_id",
            F.upper(normalize_string("product_id")),
        )
        .withColumn(
            "on_hand_qty",
            try_cast_column("on_hand_qty", "INT"),
        )
        .withColumn(
            "reserved_qty",
            try_cast_column("reserved_qty", "INT"),
        )
        .withColumn(
            "available_qty",
            try_cast_column("available_qty", "INT"),
        )
        .withColumn(
            "reorder_point",
            try_cast_column("reorder_point", "INT"),
        )
        .withColumn(
            "updated_at",
            try_cast_column("updated_at", "TIMESTAMP"),
        )
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="warehouse_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_warehouses"
        ),
        reference_column="warehouse_id",
        flag_column="_warehouse_exists",
    )

    typed_df = attach_reference_flag(
        typed_df,
        local_column="product_id",
        reference_table=(
            f"{CATALOG}.{SILVER_SCHEMA}.silver_products"
        ),
        reference_column="product_id",
        flag_column="_product_exists",
    )

    rules = [
        (
            F.col("snapshot_date").isNull(),
            "INVALID_SNAPSHOT_DATE",
        ),
        (F.col("warehouse_id").isNull(), "MISSING_WAREHOUSE_ID"),
        (
            F.col("warehouse_id").isNotNull()
            & ~F.col("_warehouse_exists"),
            "WAREHOUSE_NOT_FOUND",
        ),
        (F.col("product_id").isNull(), "MISSING_PRODUCT_ID"),
        (
            F.col("product_id").isNotNull()
            & ~F.col("_product_exists"),
            "PRODUCT_NOT_FOUND",
        ),
        (
            F.col("on_hand_qty").isNull()
            | (F.col("on_hand_qty") < 0),
            "INVALID_ON_HAND_QTY",
        ),
        (
            F.col("reserved_qty").isNull()
            | (F.col("reserved_qty") < 0),
            "INVALID_RESERVED_QTY",
        ),
        (
            F.col("available_qty").isNull()
            | (F.col("available_qty") < 0),
            "INVALID_AVAILABLE_QTY",
        ),
        (
            F.col("on_hand_qty").isNotNull()
            & F.col("reserved_qty").isNotNull()
            & (
                F.col("reserved_qty")
                > F.col("on_hand_qty")
            ),
            "RESERVED_EXCEEDS_ON_HAND",
        ),
        (
            F.col("on_hand_qty").isNotNull()
            & F.col("reserved_qty").isNotNull()
            & F.col("available_qty").isNotNull()
            & (
                F.col("available_qty")
                != (
                    F.col("on_hand_qty")
                    - F.col("reserved_qty")
                )
            ),
            "AVAILABLE_QTY_MISMATCH",
        ),
        (
            F.col("reorder_point").isNull()
            | (F.col("reorder_point") < 0),
            "INVALID_REORDER_POINT",
        ),
        (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        (rescued_data_present(), "RESCUED_SCHEMA_DATA"),
    ]

    return (
        add_quality_results(typed_df, rules)
        .drop("_warehouse_exists", "_product_exists")
    )

# COMMAND ----------

PIPELINE_CONFIG = [
    {
        "source_name": "suppliers",
        "bronze_table": "bronze_suppliers",
        "silver_table": "silver_suppliers",
        "quarantine_table": "rejected_suppliers",
        "key_columns": ["supplier_id"],
        "build_function": build_suppliers,
    },
    {
        "source_name": "warehouses",
        "bronze_table": "bronze_warehouses",
        "silver_table": "silver_warehouses",
        "quarantine_table": "rejected_warehouses",
        "key_columns": ["warehouse_id"],
        "build_function": build_warehouses,
    },
    {
        "source_name": "stores",
        "bronze_table": "bronze_stores",
        "silver_table": "silver_stores",
        "quarantine_table": "rejected_stores",
        "key_columns": ["store_id"],
        "build_function": build_stores,
    },
    {
        "source_name": "products",
        "bronze_table": "bronze_products",
        "silver_table": "silver_products",
        "quarantine_table": "rejected_products",
        "key_columns": ["product_id"],
        "build_function": build_products,
    },
    {
        "source_name": "orders",
        "bronze_table": "bronze_orders",
        "silver_table": "silver_orders",
        "quarantine_table": "rejected_orders",
        "key_columns": ["order_id"],
        "build_function": build_orders,
    },
    {
        "source_name": "order_items",
        "bronze_table": "bronze_order_items",
        "silver_table": "silver_order_items",
        "quarantine_table": "rejected_order_items",
        "key_columns": ["order_item_id"],
        "build_function": build_order_items,
    },
    {
        "source_name": "shipments",
        "bronze_table": "bronze_shipments",
        "silver_table": "silver_shipments",
        "quarantine_table": "rejected_shipments",
        "key_columns": ["shipment_id"],
        "build_function": build_shipments,
    },
    {
        "source_name": "inventory_snapshots",
        "bronze_table": "bronze_inventory_snapshots",
        "silver_table": "silver_inventory_snapshots",
        "quarantine_table": "rejected_inventory_snapshots",
        "key_columns": [
            "snapshot_date",
            "warehouse_id",
            "product_id",
        ],
        "build_function": build_inventory_snapshots,
    },
]

print(f"Configured Silver sources: {len(PIPELINE_CONFIG)}")

# COMMAND ----------

if RESET_SILVER:
    print("Dropping Silver and quarantine tables.")

    for config in PIPELINE_CONFIG:
        spark.sql(
            f"""
            DROP TABLE IF EXISTS
            {CATALOG}.{SILVER_SCHEMA}.{config["silver_table"]}
            """
        )
        spark.sql(
            f"""
            DROP TABLE IF EXISTS
            {CATALOG}.{SILVER_SCHEMA}.{config["quarantine_table"]}
            """
        )

    spark.sql(f"TRUNCATE TABLE {SILVER_AUDIT_TABLE}")
    print("Silver reset completed.")
else:
    print("Normal idempotent synchronization mode.")

# COMMAND ----------

def process_silver_source(config: dict) -> dict:
    source_name = config["source_name"]

    bronze_table = (
        f"{CATALOG}.{BRONZE_SCHEMA}."
        f"{config['bronze_table']}"
    )
    silver_table = (
        f"{CATALOG}.{SILVER_SCHEMA}."
        f"{config['silver_table']}"
    )
    quarantine_table = (
        f"{CATALOG}.{SILVER_SCHEMA}."
        f"{config['quarantine_table']}"
    )

    key_columns = config["key_columns"]
    build_function = config["build_function"]

    started_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    bronze_rows = 0
    latest_rows = 0
    accepted_rows = 0
    rejected_rows = 0
    duplicate_rows = 0

    print("=" * 80)
    print(f"Source: {source_name}")
    print(f"Bronze: {bronze_table}")
    print(f"Silver: {silver_table}")
    print(f"Quarantine: {quarantine_table}")

    try:
        bronze_df = spark.table(bronze_table)
        bronze_rows = bronze_df.count()

        transformed_df = build_function(bronze_df)

        latest_df, duplicate_df = (
            split_latest_and_duplicates(
                transformed_df,
                key_columns,
            )
        )

        # Serverless: keep latest_df lazy instead of caching it.
        # Serverless: keep duplicate_df lazy instead of caching it.

        valid_df = (
            latest_df
            .filter(F.col("_dq_status") == "VALID")
        )

        invalid_df = (
            latest_df
            .filter(F.col("_dq_status") == "REJECTED")
        )

        rejected_df = (
            add_rejection_timestamp(invalid_df)
            .unionByName(
                add_rejection_timestamp(duplicate_df),
                allowMissingColumns=True,
            )
        )

        latest_rows = latest_df.count()
        accepted_rows = valid_df.count()
        duplicate_rows = duplicate_df.count()
        rejected_rows = rejected_df.count()

        if bronze_rows != accepted_rows + rejected_rows:
            raise ValueError(
                "Reconciliation failed: "
                f"bronze_rows={bronze_rows}, "
                f"accepted_rows={accepted_rows}, "
                f"rejected_rows={rejected_rows}"
            )

        synchronize_delta_table(
            valid_df,
            silver_table,
            key_columns,
        )

        overwrite_quarantine(
            rejected_df,
            quarantine_table,
        )

        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        append_silver_audit(
            source_name=source_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            quarantine_table=quarantine_table,
            status="SUCCESS",
            bronze_rows=bronze_rows,
            latest_rows=latest_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
            duplicate_rows=duplicate_rows,
            started_at=started_at,
            finished_at=finished_at,
        )

        result = {
            "source_name": source_name,
            "status": "SUCCESS",
            "bronze_rows": bronze_rows,
            "latest_rows": latest_rows,
            "accepted_rows": accepted_rows,
            "rejected_rows": rejected_rows,
            "duplicate_rows": duplicate_rows,
        }

        print(result)
        return result

    except Exception as error:
        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        error_message = str(error)[:4000]

        append_silver_audit(
            source_name=source_name,
            bronze_table=bronze_table,
            silver_table=silver_table,
            quarantine_table=quarantine_table,
            status="FAILED",
            bronze_rows=bronze_rows,
            latest_rows=latest_rows,
            accepted_rows=accepted_rows,
            rejected_rows=rejected_rows,
            duplicate_rows=duplicate_rows,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
        )

        print(f"FAILED: {source_name}")
        print(error_message)
        raise

# COMMAND ----------

silver_results = []

# The order is intentional:
# master entities are built before tables that depend on their valid keys.
for config in PIPELINE_CONFIG:
    result = process_silver_source(config)
    silver_results.append(result)

silver_results_df = spark.createDataFrame(silver_results)

display(
    silver_results_df.orderBy("source_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data-quality reconciliation tests
# MAGIC
# MAGIC Every Bronze record must end in exactly one of these outcomes:
# MAGIC
# MAGIC 1. accepted in a Silver table; or
# MAGIC 2. stored in the corresponding quarantine table.
# MAGIC
# MAGIC Duplicate rows count as rejected records.

# COMMAND ----------

reconciliation_df = (
    silver_results_df
    .withColumn(
        "reconciled_rows",
        F.col("accepted_rows") + F.col("rejected_rows"),
    )
    .withColumn(
        "passed",
        F.col("bronze_rows") == F.col("reconciled_rows"),
    )
)

display(reconciliation_df.orderBy("source_name"))

failed_reconciliations = (
    reconciliation_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_reconciliations == 0, (
    f"{failed_reconciliations} reconciliation tests failed."
)

print("PASS: every Bronze row was accepted or quarantined.")

# COMMAND ----------

# Validate primary/business key uniqueness in every accepted Silver table.
uniqueness_test_rows = []

for config in PIPELINE_CONFIG:
    silver_table = (
        f"{CATALOG}.{SILVER_SCHEMA}."
        f"{config['silver_table']}"
    )

    key_columns = config["key_columns"]
    silver_df = spark.table(silver_table)

    duplicate_key_groups = (
        silver_df
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_key_rows = (
        silver_df
        .filter(
            reduce(
                lambda left, right: left | right,
                [
                    F.col(column_name).isNull()
                    for column_name in key_columns
                ],
            )
        )
        .count()
    )

    uniqueness_test_rows.append(
        (
            config["source_name"],
            silver_table,
            duplicate_key_groups,
            missing_key_rows,
            (
                duplicate_key_groups == 0
                and missing_key_rows == 0
            ),
        )
    )

uniqueness_tests_df = spark.createDataFrame(
    uniqueness_test_rows,
    [
        "source_name",
        "silver_table",
        "duplicate_key_groups",
        "missing_key_rows",
        "passed",
    ],
)

display(uniqueness_tests_df.orderBy("source_name"))

failed_key_tests = (
    uniqueness_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_key_tests == 0, (
    f"{failed_key_tests} business-key tests failed."
)

print("PASS: all Silver business keys are complete and unique.")

# COMMAND ----------

# Review the most frequent rejection reasons across all quarantine tables.
rejection_reason_frames = []

for config in PIPELINE_CONFIG:
    quarantine_table = (
        f"{CATALOG}.{SILVER_SCHEMA}."
        f"{config['quarantine_table']}"
    )

    rejection_reason_frames.append(
        spark.table(quarantine_table)
        .select(
            F.lit(config["source_name"]).alias(
                "source_name"
            ),
            F.explode(
                F.split(F.col("_dq_errors"), r";\s*")
            ).alias("error_code"),
        )
        .filter(F.length(F.col("error_code")) > 0)
    )

all_rejection_reasons_df = reduce(
    lambda left, right: left.unionByName(right),
    rejection_reason_frames,
)

rejection_summary_df = (
    all_rejection_reasons_df
    .groupBy("source_name", "error_code")
    .count()
    .orderBy(
        F.col("count").desc(),
        "source_name",
        "error_code",
    )
)

display(rejection_summary_df)

# COMMAND ----------

# Inspect rejected records with their original raw payload and lineage.
display(
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.rejected_orders"
    )
    .select(
        "order_id",
        "store_id",
        "order_date",
        "_dq_errors",
        "_raw_record",
        "_source_file_name",
        "_batch_id",
        "_rejected_at",
    )
    .orderBy("_dq_errors", "order_id")
)

# COMMAND ----------

display(
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.rejected_shipments"
    )
    .select(
        "shipment_id",
        "order_id",
        "warehouse_id",
        "shipment_date",
        "actual_delivery_date",
        "_dq_errors",
        "_source_file_name",
        "_rejected_at",
    )
    .orderBy("_dq_errors", "shipment_id")
)

# COMMAND ----------

# Review audit records for this Silver run.
display(
    spark.table(SILVER_AUDIT_TABLE)
    .filter(F.col("run_id") == SILVER_RUN_ID)
    .orderBy("source_name")
)

# COMMAND ----------

# Validate referential integrity in accepted Silver tables.
referential_tests = [
    (
        "products_to_suppliers",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_products p
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_suppliers s
            ON p.supplier_id = s.supplier_id
            """
        ).first()["failures"],
    ),
    (
        "orders_to_stores",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_orders o
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_stores s
            ON o.store_id = s.store_id
            """
        ).first()["failures"],
    ),
    (
        "order_items_to_orders",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_order_items i
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_orders o
            ON i.order_id = o.order_id
            """
        ).first()["failures"],
    ),
    (
        "order_items_to_products",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_order_items i
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_products p
            ON i.product_id = p.product_id
            """
        ).first()["failures"],
    ),
    (
        "shipments_to_orders",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_shipments sh
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_orders o
            ON sh.order_id = o.order_id
            """
        ).first()["failures"],
    ),
    (
        "shipments_to_warehouses",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{SILVER_SCHEMA}.silver_shipments sh
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_warehouses w
            ON sh.warehouse_id = w.warehouse_id
            """
        ).first()["failures"],
    ),
    (
        "inventory_to_products",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM
                {CATALOG}.{SILVER_SCHEMA}.silver_inventory_snapshots i
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_products p
            ON i.product_id = p.product_id
            """
        ).first()["failures"],
    ),
    (
        "inventory_to_warehouses",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM
                {CATALOG}.{SILVER_SCHEMA}.silver_inventory_snapshots i
            LEFT ANTI JOIN
                {CATALOG}.{SILVER_SCHEMA}.silver_warehouses w
            ON i.warehouse_id = w.warehouse_id
            """
        ).first()["failures"],
    ),
]

referential_tests_df = spark.createDataFrame(
    [
        (
            test_name,
            int(failures),
            int(failures) == 0,
        )
        for test_name, failures in referential_tests
    ],
    ["test_name", "failure_count", "passed"],
)

display(referential_tests_df.orderBy("test_name"))

referential_failures = (
    referential_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert referential_failures == 0, (
    f"{referential_failures} referential-integrity tests failed."
)

print("PASS: all accepted Silver relationships are valid.")

# COMMAND ----------

# Show all objects created in Silver.
display(
    spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency test
# MAGIC
# MAGIC Run this notebook again with `RESET_SILVER = False`.
# MAGIC
# MAGIC Expected behavior:
# MAGIC - accepted and rejected counts remain unchanged;
# MAGIC - no duplicate keys appear in Silver;
# MAGIC - quarantine tables do not accumulate duplicate copies;
# MAGIC - all reconciliation, uniqueness, and referential tests pass;
# MAGIC - a new audit record is created for each source.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_control.silver_load_audit
# MAGIC ORDER BY started_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     store_id,
# MAGIC     order_date,
# MAGIC     _dq_errors,
# MAGIC     _raw_record,
# MAGIC     _source_file_name,
# MAGIC     _rejected_at
# MAGIC FROM scm_silver.rejected_orders
# MAGIC ORDER BY _dq_errors;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     shipment_id,
# MAGIC     order_id,
# MAGIC     warehouse_id,
# MAGIC     shipment_date,
# MAGIC     actual_delivery_date,
# MAGIC     shipped_quantity,
# MAGIC     delivered_quantity,
# MAGIC     shipping_cost,
# MAGIC     _dq_errors
# MAGIC FROM scm_silver.rejected_shipments
# MAGIC ORDER BY _dq_errors;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     source_name,
# MAGIC     status,
# MAGIC     bronze_rows,
# MAGIC     latest_rows,
# MAGIC     accepted_rows,
# MAGIC     rejected_rows,
# MAGIC     duplicate_rows,
# MAGIC     started_at,
# MAGIC     finished_at
# MAGIC FROM scm_control.silver_load_audit
# MAGIC ORDER BY started_at DESC, source_name;