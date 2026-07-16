# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - End-to-end pipeline health check
# MAGIC
# MAGIC Final quality gate for the FMCG Supply Chain Lakehouse workflow.
# MAGIC
# MAGIC This notebook verifies:
# MAGIC - all expected Bronze, Silver, and Gold objects exist;
# MAGIC - the most recent audit record for every source/object succeeded;
# MAGIC - Silver accepted/rejected rows reconcile with Bronze;
# MAGIC - critical Gold tables contain data;
# MAGIC - executive KPI percentages remain within valid ranges.
# MAGIC
# MAGIC The notebook writes one execution result to
# MAGIC `scm_control.pipeline_health_audit` and raises an exception when any
# MAGIC check fails, causing the Lakeflow Job task to fail visibly.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.sql("SELECT current_catalog()").first()[0]

BRONZE_SCHEMA = "scm_bronze"
SILVER_SCHEMA = "scm_silver"
GOLD_SCHEMA = "scm_gold"
CONTROL_SCHEMA = "scm_control"

HEALTH_RUN_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Catalog: {CATALOG}")
print(f"Health run ID: {HEALTH_RUN_ID}")

# COMMAND ----------

EXPECTED_BRONZE_TABLES = [
    "bronze_suppliers",
    "bronze_products",
    "bronze_warehouses",
    "bronze_stores",
    "bronze_orders",
    "bronze_order_items",
    "bronze_shipments",
    "bronze_inventory_snapshots",
]

EXPECTED_SILVER_TABLES = [
    "silver_suppliers",
    "silver_products",
    "silver_warehouses",
    "silver_stores",
    "silver_orders",
    "silver_order_items",
    "silver_shipments",
    "silver_inventory_snapshots",
]

EXPECTED_QUARANTINE_TABLES = [
    "rejected_suppliers",
    "rejected_products",
    "rejected_warehouses",
    "rejected_stores",
    "rejected_orders",
    "rejected_order_items",
    "rejected_shipments",
    "rejected_inventory_snapshots",
]

EXPECTED_GOLD_TABLES = [
    "dim_date",
    "dim_supplier",
    "dim_product",
    "dim_store",
    "dim_warehouse",
    "fact_order_lines",
    "fact_shipments",
    "fact_inventory_daily",
]

EXPECTED_GOLD_VIEWS = [
    "vw_executive_supply_chain_kpis",
    "vw_warehouse_delivery_performance",
    "vw_product_inventory_risk",
    "vw_store_order_performance",
]

EXPECTED_BRONZE_SOURCES = {
    "suppliers",
    "products",
    "warehouses",
    "stores",
    "orders",
    "order_items",
    "shipments",
    "inventory_snapshots",
}

EXPECTED_GOLD_OBJECT_COUNT = (
    len(EXPECTED_GOLD_TABLES)
    + len(EXPECTED_GOLD_VIEWS)
)

# COMMAND ----------

HEALTH_AUDIT_TABLE = (
    f"{CATALOG}.{CONTROL_SCHEMA}.pipeline_health_audit"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {HEALTH_AUDIT_TABLE} (
        health_run_id STRING,
        status STRING,
        total_checks BIGINT,
        passed_checks BIGINT,
        failed_checks BIGINT,
        failed_check_names STRING,
        started_at TIMESTAMP,
        finished_at TIMESTAMP
    )
    COMMENT 'End-to-end FMCG Lakehouse workflow health-check history'
    """
)

# COMMAND ----------

check_results = []


def add_check(
    check_name: str,
    passed: bool,
    observed_value,
    expected_value,
    details: str = "",
):
    check_results.append(
        {
            "check_name": check_name,
            "passed": bool(passed),
            "observed_value": str(observed_value),
            "expected_value": str(expected_value),
            "details": details,
        }
    )


def full_name(schema_name: str, object_name: str) -> str:
    return f"{CATALOG}.{schema_name}.{object_name}"


def check_objects_exist(
    schema_name: str,
    object_names: list[str],
    check_prefix: str,
):
    for object_name in object_names:
        qualified_name = full_name(
            schema_name,
            object_name,
        )

        exists = spark.catalog.tableExists(
            qualified_name
        )

        add_check(
            check_name=f"{check_prefix}_{object_name}_exists",
            passed=exists,
            observed_value=exists,
            expected_value=True,
            details=qualified_name,
        )


def latest_per_partition(
    table_name: str,
    partition_column: str,
    timestamp_column: str,
):
    source_df = spark.table(table_name)

    latest_window = (
        Window
        .partitionBy(partition_column)
        .orderBy(
            F.col(timestamp_column).desc_nulls_last()
        )
    )

    return (
        source_df
        .withColumn(
            "_latest_rank",
            F.row_number().over(latest_window),
        )
        .filter(F.col("_latest_rank") == 1)
        .drop("_latest_rank")
    )

# COMMAND ----------

# 1. Object-existence tests.
check_objects_exist(
    BRONZE_SCHEMA,
    EXPECTED_BRONZE_TABLES,
    "bronze",
)

check_objects_exist(
    SILVER_SCHEMA,
    EXPECTED_SILVER_TABLES,
    "silver",
)

check_objects_exist(
    SILVER_SCHEMA,
    EXPECTED_QUARANTINE_TABLES,
    "quarantine",
)

check_objects_exist(
    GOLD_SCHEMA,
    EXPECTED_GOLD_TABLES,
    "gold_table",
)

check_objects_exist(
    GOLD_SCHEMA,
    EXPECTED_GOLD_VIEWS,
    "gold_view",
)

# COMMAND ----------

# 2. Latest Bronze audit status by source.
BRONZE_AUDIT_TABLE = full_name(
    CONTROL_SCHEMA,
    "bronze_load_audit",
)

if spark.catalog.tableExists(BRONZE_AUDIT_TABLE):
    latest_bronze_audit = latest_per_partition(
        BRONZE_AUDIT_TABLE,
        "source_name",
        "started_at",
    )

    observed_sources = {
        row["source_name"]
        for row in latest_bronze_audit.select(
            "source_name"
        ).collect()
    }

    failed_bronze_sources = [
        row["source_name"]
        for row in (
            latest_bronze_audit
            .filter(F.col("status") != "SUCCESS")
            .select("source_name")
            .collect()
        )
    ]

    add_check(
        "bronze_latest_audit_has_all_sources",
        observed_sources == EXPECTED_BRONZE_SOURCES,
        sorted(observed_sources),
        sorted(EXPECTED_BRONZE_SOURCES),
    )

    add_check(
        "bronze_latest_audit_all_success",
        len(failed_bronze_sources) == 0,
        failed_bronze_sources,
        [],
    )
else:
    add_check(
        "bronze_audit_table_exists",
        False,
        False,
        True,
        BRONZE_AUDIT_TABLE,
    )

# COMMAND ----------

# 3. Latest Silver audit status and reconciliation by source.
SILVER_AUDIT_TABLE = full_name(
    CONTROL_SCHEMA,
    "silver_load_audit",
)

if spark.catalog.tableExists(SILVER_AUDIT_TABLE):
    latest_silver_audit = latest_per_partition(
        SILVER_AUDIT_TABLE,
        "source_name",
        "started_at",
    )

    observed_sources = {
        row["source_name"]
        for row in latest_silver_audit.select(
            "source_name"
        ).collect()
    }

    failed_silver_sources = [
        row["source_name"]
        for row in (
            latest_silver_audit
            .filter(F.col("status") != "SUCCESS")
            .select("source_name")
            .collect()
        )
    ]

    reconciliation_failures = (
        latest_silver_audit
        .filter(
            F.col("bronze_rows")
            != (
                F.col("accepted_rows")
                + F.col("rejected_rows")
            )
        )
        .select(
            "source_name",
            "bronze_rows",
            "accepted_rows",
            "rejected_rows",
        )
        .collect()
    )

    add_check(
        "silver_latest_audit_has_all_sources",
        observed_sources == EXPECTED_BRONZE_SOURCES,
        sorted(observed_sources),
        sorted(EXPECTED_BRONZE_SOURCES),
    )

    add_check(
        "silver_latest_audit_all_success",
        len(failed_silver_sources) == 0,
        failed_silver_sources,
        [],
    )

    add_check(
        "silver_latest_reconciliation_passes",
        len(reconciliation_failures) == 0,
        len(reconciliation_failures),
        0,
        str(reconciliation_failures),
    )
else:
    add_check(
        "silver_audit_table_exists",
        False,
        False,
        True,
        SILVER_AUDIT_TABLE,
    )

# COMMAND ----------

# 4. Latest Gold audit status.
GOLD_AUDIT_TABLE = full_name(
    CONTROL_SCHEMA,
    "gold_load_audit",
)

if spark.catalog.tableExists(GOLD_AUDIT_TABLE):
    latest_gold_audit = latest_per_partition(
        GOLD_AUDIT_TABLE,
        "object_name",
        "started_at",
    )

    latest_gold_count = latest_gold_audit.count()

    failed_gold_objects = [
        row["object_name"]
        for row in (
            latest_gold_audit
            .filter(F.col("status") != "SUCCESS")
            .select("object_name")
            .collect()
        )
    ]

    add_check(
        "gold_latest_audit_has_all_objects",
        latest_gold_count == EXPECTED_GOLD_OBJECT_COUNT,
        latest_gold_count,
        EXPECTED_GOLD_OBJECT_COUNT,
    )

    add_check(
        "gold_latest_audit_all_success",
        len(failed_gold_objects) == 0,
        failed_gold_objects,
        [],
    )
else:
    add_check(
        "gold_audit_table_exists",
        False,
        False,
        True,
        GOLD_AUDIT_TABLE,
    )

# COMMAND ----------

# 5. Critical Gold table row-count tests.
CRITICAL_GOLD_TABLES = [
    "dim_product",
    "dim_store",
    "dim_warehouse",
    "fact_order_lines",
    "fact_shipments",
    "fact_inventory_daily",
]

for table_name in CRITICAL_GOLD_TABLES:
    qualified_name = full_name(
        GOLD_SCHEMA,
        table_name,
    )

    if spark.catalog.tableExists(qualified_name):
        row_count = spark.table(qualified_name).count()

        add_check(
            check_name=f"{table_name}_contains_rows",
            passed=row_count > 0,
            observed_value=row_count,
            expected_value="> 0",
            details=qualified_name,
        )

# COMMAND ----------

# 6. Executive KPI validation.
EXECUTIVE_KPI_VIEW = full_name(
    GOLD_SCHEMA,
    "vw_executive_supply_chain_kpis",
)

if spark.catalog.tableExists(EXECUTIVE_KPI_VIEW):
    executive_kpi_df = spark.table(
        EXECUTIVE_KPI_VIEW
    )

    executive_row_count = executive_kpi_df.count()

    add_check(
        "executive_kpi_view_has_one_row",
        executive_row_count == 1,
        executive_row_count,
        1,
    )

    percentage_columns = [
        "average_discount_pct",
        "fill_rate_pct",
        "on_time_delivery_pct",
        "in_full_pct",
        "otif_pct",
        "stockout_rate_pct",
        "below_reorder_rate_pct",
    ]

    invalid_percentage_condition = None

    for column_name in percentage_columns:
        condition = (
            F.col(column_name).isNotNull()
            & (
                (F.col(column_name) < 0)
                | (F.col(column_name) > 100)
            )
        )

        invalid_percentage_condition = (
            condition
            if invalid_percentage_condition is None
            else invalid_percentage_condition | condition
        )

    invalid_percentage_rows = (
        executive_kpi_df
        .filter(invalid_percentage_condition)
        .count()
    )

    add_check(
        "executive_kpi_percentages_in_range",
        invalid_percentage_rows == 0,
        invalid_percentage_rows,
        0,
    )

# COMMAND ----------

checks_df = spark.createDataFrame(check_results)

display(
    checks_df.orderBy(
        F.col("passed").asc(),
        "check_name",
    )
)

# COMMAND ----------

total_checks = len(check_results)
failed_check_names = [
    check["check_name"]
    for check in check_results
    if not check["passed"]
]
failed_checks = len(failed_check_names)
passed_checks = total_checks - failed_checks

FINAL_STATUS = (
    "SUCCESS"
    if failed_checks == 0
    else "FAILED"
)

FINISHED_AT = datetime.now(timezone.utc).replace(
    tzinfo=None
)

health_audit_schema = """
    health_run_id STRING,
    status STRING,
    total_checks LONG,
    passed_checks LONG,
    failed_checks LONG,
    failed_check_names STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
"""

health_audit_row = [
    (
        HEALTH_RUN_ID,
        FINAL_STATUS,
        total_checks,
        passed_checks,
        failed_checks,
        "; ".join(failed_check_names),
        STARTED_AT,
        FINISHED_AT,
    )
]

(
    spark.createDataFrame(
        health_audit_row,
        schema=health_audit_schema,
    )
    .write
    .mode("append")
    .saveAsTable(HEALTH_AUDIT_TABLE)
)

summary_df = spark.createDataFrame(
    [
        (
            HEALTH_RUN_ID,
            FINAL_STATUS,
            total_checks,
            passed_checks,
            failed_checks,
            "; ".join(failed_check_names),
        )
    ],
    [
        "health_run_id",
        "status",
        "total_checks",
        "passed_checks",
        "failed_checks",
        "failed_check_names",
    ],
)

display(summary_df)

# COMMAND ----------

if failed_checks > 0:
    raise RuntimeError(
        "Pipeline health check failed. Failed checks: "
        + ", ".join(failed_check_names)
    )

print(
    f"PASS: all {total_checks} end-to-end "
    "pipeline health checks succeeded."
)

# COMMAND ----------

display(
    spark.table(HEALTH_AUDIT_TABLE)
    .orderBy(F.col("started_at").desc())
    .limit(20)
)