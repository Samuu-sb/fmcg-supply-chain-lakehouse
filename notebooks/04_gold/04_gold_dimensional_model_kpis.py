# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold dimensional model and business KPIs
# MAGIC
# MAGIC Builds the business-ready presentation layer of the FMCG Supply Chain
# MAGIC Lakehouse from validated Silver tables.
# MAGIC
# MAGIC Gold objects:
# MAGIC - Dimensions: date, supplier, product, store, warehouse
# MAGIC - Facts: order lines, shipments, daily inventory
# MAGIC - Business views: executive KPIs, warehouse performance, inventory risk,
# MAGIC   and store order performance
# MAGIC
# MAGIC Design notes:
# MAGIC - The Gold layer is rebuilt deterministically from the current Silver state.
# MAGIC - Stable BIGINT surrogate keys are generated with xxhash64.
# MAGIC - All writes use managed Delta tables.
# MAGIC - No Spark cache APIs are used, keeping the notebook compatible with
# MAGIC   Databricks Serverless compute.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.sql("SELECT current_catalog()").first()[0]

SILVER_SCHEMA = "scm_silver"
GOLD_SCHEMA = "scm_gold"
CONTROL_SCHEMA = "scm_control"

GOLD_RUN_ID = str(uuid4())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Gold run ID: {GOLD_RUN_ID}")

# COMMAND ----------

REQUIRED_SILVER_TABLES = [
    "silver_suppliers",
    "silver_products",
    "silver_stores",
    "silver_warehouses",
    "silver_orders",
    "silver_order_items",
    "silver_shipments",
    "silver_inventory_snapshots",
]

missing_silver_tables = [
    table_name
    for table_name in REQUIRED_SILVER_TABLES
    if not spark.catalog.tableExists(
        f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    )
]

if missing_silver_tables:
    raise RuntimeError(
        "The following required Silver tables do not exist: "
        + ", ".join(missing_silver_tables)
    )

print("PASS: all required Silver tables exist.")

# COMMAND ----------

GOLD_AUDIT_TABLE = (
    f"{CATALOG}.{CONTROL_SCHEMA}.gold_load_audit"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {GOLD_AUDIT_TABLE} (
        run_id STRING,
        object_name STRING,
        object_type STRING,
        status STRING,
        row_count BIGINT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    )
    COMMENT 'Execution audit for Gold dimensional tables and business views'
    """
)

print(f"Audit table ready: {GOLD_AUDIT_TABLE}")

# COMMAND ----------

def stable_key(entity_name: str, *column_names: str):
    """Generate a deterministic BIGINT surrogate key."""
    return F.xxhash64(
        F.lit(entity_name),
        *[F.col(column_name) for column_name in column_names],
    )


def date_key(column_name: str):
    """Convert a DATE column to an integer YYYYMMDD key."""
    return F.date_format(
        F.col(column_name),
        "yyyyMMdd",
    ).cast("int")


def append_gold_audit(
    object_name: str,
    object_type: str,
    status: str,
    row_count: int,
    started_at: datetime,
    finished_at: datetime,
    error_message=None,
):
    audit_schema = """
        run_id STRING,
        object_name STRING,
        object_type STRING,
        status STRING,
        row_count LONG,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        error_message STRING
    """

    audit_rows = [
        (
            GOLD_RUN_ID,
            object_name,
            object_type,
            status,
            int(row_count),
            started_at,
            finished_at,
            error_message,
        )
    ]

    (
        spark.createDataFrame(
            audit_rows,
            schema=audit_schema,
        )
        .write
        .mode("append")
        .saveAsTable(GOLD_AUDIT_TABLE)
    )


def write_gold_table(
    df: DataFrame,
    table_name: str,
) -> dict:
    """
    Atomically rebuild one managed Delta table from the current Silver state.

    A full refresh is appropriate for this portfolio-sized dataset and keeps
    the business layer deterministic. Bronze remains incremental and Silver
    remains idempotent, while Gold represents the current trusted state.
    """
    full_table_name = (
        f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    )

    started_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )

        row_count = spark.table(full_table_name).count()

        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        append_gold_audit(
            object_name=full_table_name,
            object_type="TABLE",
            status="SUCCESS",
            row_count=row_count,
            started_at=started_at,
            finished_at=finished_at,
        )

        result = {
            "object_name": full_table_name,
            "object_type": "TABLE",
            "status": "SUCCESS",
            "row_count": row_count,
        }

        print(result)
        return result

    except Exception as error:
        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        error_message = str(error)[:4000]

        append_gold_audit(
            object_name=full_table_name,
            object_type="TABLE",
            status="FAILED",
            row_count=0,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
        )

        print(f"FAILED: {full_table_name}")
        print(error_message)
        raise


def create_business_view(
    view_name: str,
    query: str,
) -> dict:
    full_view_name = (
        f"{CATALOG}.{GOLD_SCHEMA}.{view_name}"
    )

    started_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    try:
        spark.sql(
            f"""
            CREATE OR REPLACE VIEW {full_view_name}
            AS
            {query}
            """
        )

        row_count = spark.table(full_view_name).count()

        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        append_gold_audit(
            object_name=full_view_name,
            object_type="VIEW",
            status="SUCCESS",
            row_count=row_count,
            started_at=started_at,
            finished_at=finished_at,
        )

        result = {
            "object_name": full_view_name,
            "object_type": "VIEW",
            "status": "SUCCESS",
            "row_count": row_count,
        }

        print(result)
        return result

    except Exception as error:
        finished_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        error_message = str(error)[:4000]

        append_gold_audit(
            object_name=full_view_name,
            object_type="VIEW",
            status="FAILED",
            row_count=0,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
        )

        print(f"FAILED: {full_view_name}")
        print(error_message)
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dimensions

# COMMAND ----------

silver_suppliers = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_suppliers"
)

dim_supplier_df = (
    silver_suppliers
    .select(
        stable_key(
            "SUPPLIER",
            "supplier_id",
        ).alias("supplier_key"),
        "supplier_id",
        "supplier_name",
        "country",
        "lead_time_days",
        "reliability_score",
        "active_flag",
        "updated_at",
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

silver_products = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_products"
)

dim_product_df = (
    silver_products
    .select(
        stable_key(
            "PRODUCT",
            "product_id",
        ).alias("product_key"),
        stable_key(
            "SUPPLIER",
            "supplier_id",
        ).alias("supplier_key"),
        "product_id",
        "sku",
        "product_name",
        "category",
        "brand",
        "package_size",
        "supplier_id",
        "unit_price",
        "unit_cost",
        (
            F.col("unit_price") - F.col("unit_cost")
        ).cast("decimal(12,2)").alias(
            "unit_margin"
        ),
        (
            (
                F.col("unit_price") - F.col("unit_cost")
            )
            / F.col("unit_price")
        ).cast("decimal(12,4)").alias(
            "margin_rate"
        ),
        "weight_kg",
        "active_flag",
        "updated_at",
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

silver_stores = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_stores"
)

dim_store_df = (
    silver_stores
    .select(
        stable_key(
            "STORE",
            "store_id",
        ).alias("store_key"),
        "store_id",
        "store_name",
        "channel",
        "province",
        "city",
        "priority_level",
        "active_flag",
        "updated_at",
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

silver_warehouses = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_warehouses"
)

dim_warehouse_df = (
    silver_warehouses
    .select(
        stable_key(
            "WAREHOUSE",
            "warehouse_id",
        ).alias("warehouse_key"),
        "warehouse_id",
        "warehouse_name",
        "province",
        "city",
        "capacity_units",
        "opened_date",
        "status",
        "updated_at",
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

# Determine the complete business date range from all relevant Silver dates.
date_bounds = spark.sql(
    f"""
    SELECT
        MIN(business_date) AS minimum_date,
        MAX(business_date) AS maximum_date
    FROM (
        SELECT order_date AS business_date
        FROM {CATALOG}.{SILVER_SCHEMA}.silver_orders

        UNION ALL

        SELECT requested_delivery_date AS business_date
        FROM {CATALOG}.{SILVER_SCHEMA}.silver_orders

        UNION ALL

        SELECT shipment_date AS business_date
        FROM {CATALOG}.{SILVER_SCHEMA}.silver_shipments

        UNION ALL

        SELECT actual_delivery_date AS business_date
        FROM {CATALOG}.{SILVER_SCHEMA}.silver_shipments

        UNION ALL

        SELECT snapshot_date AS business_date
        FROM {CATALOG}.{SILVER_SCHEMA}.silver_inventory_snapshots
    )
    WHERE business_date IS NOT NULL
    """
).first()

minimum_date = date_bounds["minimum_date"]
maximum_date = date_bounds["maximum_date"]

if minimum_date is None or maximum_date is None:
    raise RuntimeError(
        "The Silver tables do not contain any valid business dates."
    )

print(f"Date dimension range: {minimum_date} to {maximum_date}")

date_sequence_df = spark.sql(
    f"""
    SELECT EXPLODE(
        SEQUENCE(
            TO_DATE('{minimum_date}'),
            TO_DATE('{maximum_date}'),
            INTERVAL 1 DAY
        )
    ) AS full_date
    """
)

dim_date_df = (
    date_sequence_df
    .select(
        date_key("full_date").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month_number"),
        F.date_format(
            "full_date",
            "MMMM",
        ).alias("month_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week_number"),
        F.date_format(
            "full_date",
            "EEEE",
        ).alias("day_name"),
        (
            F.dayofweek("full_date").isin(1, 7)
        ).alias("is_weekend"),
        F.date_trunc(
            "month",
            "full_date",
        ).cast("date").alias("month_start_date"),
        F.last_day("full_date").alias("month_end_date"),
        F.concat(
            F.year("full_date"),
            F.lit("-Q"),
            F.quarter("full_date"),
        ).alias("year_quarter"),
        F.date_format(
            "full_date",
            "yyyy-MM",
        ).alias("year_month"),
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

gold_results = []

gold_results.append(
    write_gold_table(
        dim_date_df,
        "dim_date",
    )
)

gold_results.append(
    write_gold_table(
        dim_supplier_df,
        "dim_supplier",
    )
)

gold_results.append(
    write_gold_table(
        dim_product_df,
        "dim_product",
    )
)

gold_results.append(
    write_gold_table(
        dim_store_df,
        "dim_store",
    )
)

gold_results.append(
    write_gold_table(
        dim_warehouse_df,
        "dim_warehouse",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact tables

# COMMAND ----------

silver_orders = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_orders"
).alias("orders")

silver_order_items = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_order_items"
).alias("items")

silver_product_lookup = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_products"
    )
    .select(
        "product_id",
        "supplier_id",
    )
    .alias("products")
)

order_line_source_df = (
    silver_order_items
    .join(
        silver_orders,
        F.col("items.order_id")
        == F.col("orders.order_id"),
        "inner",
    )
    .join(
        silver_product_lookup,
        F.col("items.product_id")
        == F.col("products.product_id"),
        "inner",
    )
)

gross_amount = (
    F.col("items.quantity_ordered").cast("decimal(18,2)")
    * F.col("items.unit_price").cast("decimal(18,2)")
)

fact_order_lines_df = (
    order_line_source_df
    .select(
        stable_key(
            "ORDER_LINE",
            "items.order_item_id",
        ).alias("order_line_key"),
        F.col("items.order_item_id").alias("order_item_id"),
        F.col("items.order_id").alias("order_id"),
        F.col("items.product_id").alias("product_id"),
        F.col("orders.store_id").alias("store_id"),
        F.col("products.supplier_id").alias("supplier_id"),
        stable_key(
            "PRODUCT",
            "items.product_id",
        ).alias("product_key"),
        stable_key(
            "STORE",
            "orders.store_id",
        ).alias("store_key"),
        stable_key(
            "SUPPLIER",
            "products.supplier_id",
        ).alias("supplier_key"),
        date_key(
            "orders.order_date"
        ).alias("order_date_key"),
        date_key(
            "orders.requested_delivery_date"
        ).alias("requested_delivery_date_key"),
        F.col("orders.order_date").alias("order_date"),
        F.col(
            "orders.requested_delivery_date"
        ).alias("requested_delivery_date"),
        F.col("orders.order_status").alias("order_status"),
        F.col("orders.currency").alias("currency"),
        F.col("orders.source_system").alias("source_system"),
        F.col(
            "items.quantity_ordered"
        ).alias("quantity_ordered"),
        F.col("items.unit_price").alias("unit_price"),
        F.col("items.discount_pct").alias("discount_pct"),
        gross_amount.cast("decimal(18,2)").alias(
            "gross_amount"
        ),
        (
            gross_amount
            - F.col("items.line_amount")
        ).cast("decimal(18,2)").alias(
            "discount_amount"
        ),
        F.col("items.line_amount").cast(
            "decimal(18,2)"
        ).alias("net_amount"),
        F.greatest(
            F.col("items.updated_at"),
            F.col("orders.updated_at"),
        ).alias("source_updated_at"),
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

silver_shipments = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_shipments"
).alias("shipments")

shipment_source_df = (
    silver_shipments
    .join(
        silver_orders,
        F.col("shipments.order_id")
        == F.col("orders.order_id"),
        "inner",
    )
)

actual_date_exists = (
    F.col("shipments.actual_delivery_date").isNotNull()
)

on_time_flag = (
    actual_date_exists
    & (
        F.col("shipments.actual_delivery_date")
        <= F.col("orders.requested_delivery_date")
    )
)

in_full_flag = (
    F.col("shipments.delivered_quantity")
    >= F.col("shipments.shipped_quantity")
)

fact_shipments_df = (
    shipment_source_df
    .select(
        stable_key(
            "SHIPMENT",
            "shipments.shipment_id",
        ).alias("shipment_key"),
        F.col("shipments.shipment_id").alias("shipment_id"),
        F.col("shipments.order_id").alias("order_id"),
        F.col("orders.store_id").alias("store_id"),
        F.col(
            "shipments.warehouse_id"
        ).alias("warehouse_id"),
        stable_key(
            "STORE",
            "orders.store_id",
        ).alias("store_key"),
        stable_key(
            "WAREHOUSE",
            "shipments.warehouse_id",
        ).alias("warehouse_key"),
        date_key(
            "shipments.shipment_date"
        ).alias("shipment_date_key"),
        date_key(
            "orders.requested_delivery_date"
        ).alias("requested_delivery_date_key"),
        date_key(
            "shipments.actual_delivery_date"
        ).alias("actual_delivery_date_key"),
        F.col(
            "shipments.shipment_date"
        ).alias("shipment_date"),
        F.col(
            "orders.requested_delivery_date"
        ).alias("requested_delivery_date"),
        F.col(
            "shipments.actual_delivery_date"
        ).alias("actual_delivery_date"),
        F.col("shipments.carrier").alias("carrier"),
        F.col(
            "shipments.shipment_status"
        ).alias("shipment_status"),
        F.col(
            "shipments.shipped_quantity"
        ).alias("shipped_quantity"),
        F.col(
            "shipments.delivered_quantity"
        ).alias("delivered_quantity"),
        (
            F.col("shipments.shipped_quantity")
            - F.col("shipments.delivered_quantity")
        ).alias("undelivered_quantity"),
        F.col("shipments.shipping_cost").alias(
            "shipping_cost"
        ),
        (
            F.col("shipments.delivered_quantity").cast("double")
            / F.col("shipments.shipped_quantity").cast("double")
        ).alias("fill_rate"),
        F.when(
            actual_date_exists,
            F.datediff(
                F.col("shipments.actual_delivery_date"),
                F.col("shipments.shipment_date"),
            ),
        ).alias("delivery_lead_time_days"),
        F.when(
            actual_date_exists,
            F.greatest(
                F.datediff(
                    F.col("shipments.actual_delivery_date"),
                    F.col("orders.requested_delivery_date"),
                ),
                F.lit(0),
            ),
        ).alias("delay_days"),
        actual_date_exists.alias("delivery_completed_flag"),
        on_time_flag.alias("on_time_flag"),
        in_full_flag.alias("in_full_flag"),
        (
            on_time_flag & in_full_flag
        ).alias("otif_flag"),
        F.greatest(
            F.col("shipments.updated_at"),
            F.col("orders.updated_at"),
        ).alias("source_updated_at"),
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

silver_inventory = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_inventory_snapshots"
).alias("inventory")

inventory_source_df = (
    silver_inventory
    .join(
        silver_product_lookup,
        F.col("inventory.product_id")
        == F.col("products.product_id"),
        "inner",
    )
)

fact_inventory_daily_df = (
    inventory_source_df
    .select(
        stable_key(
            "INVENTORY",
            "inventory.snapshot_date",
            "inventory.warehouse_id",
            "inventory.product_id",
        ).alias("inventory_key"),
        date_key(
            "inventory.snapshot_date"
        ).alias("snapshot_date_key"),
        F.col(
            "inventory.snapshot_date"
        ).alias("snapshot_date"),
        F.col(
            "inventory.warehouse_id"
        ).alias("warehouse_id"),
        F.col("inventory.product_id").alias("product_id"),
        F.col("products.supplier_id").alias("supplier_id"),
        stable_key(
            "WAREHOUSE",
            "inventory.warehouse_id",
        ).alias("warehouse_key"),
        stable_key(
            "PRODUCT",
            "inventory.product_id",
        ).alias("product_key"),
        stable_key(
            "SUPPLIER",
            "products.supplier_id",
        ).alias("supplier_key"),
        F.col("inventory.on_hand_qty").alias("on_hand_qty"),
        F.col("inventory.reserved_qty").alias("reserved_qty"),
        F.col("inventory.available_qty").alias("available_qty"),
        F.col("inventory.reorder_point").alias("reorder_point"),
        (
            F.col("inventory.available_qty") == 0
        ).alias("stockout_flag"),
        (
            F.col("inventory.available_qty")
            < F.col("inventory.reorder_point")
        ).alias("below_reorder_point_flag"),
        F.when(
            F.col("inventory.available_qty") == 0,
            F.lit("STOCKOUT"),
        )
        .when(
            F.col("inventory.available_qty")
            < F.col("inventory.reorder_point"),
            F.lit("LOW_STOCK"),
        )
        .otherwise(F.lit("HEALTHY"))
        .alias("inventory_status"),
        (
            F.col("inventory.available_qty").cast("double")
            / F.when(
                F.col("inventory.reorder_point") == 0,
                F.lit(None),
            ).otherwise(
                F.col("inventory.reorder_point").cast("double")
            )
        ).alias("available_to_reorder_ratio"),
        F.col("inventory.updated_at").alias("source_updated_at"),
        F.lit(GOLD_RUN_ID).alias("_gold_run_id"),
        F.current_timestamp().alias("_gold_processed_at"),
    )
)

# COMMAND ----------

gold_results.append(
    write_gold_table(
        fact_order_lines_df,
        "fact_order_lines",
    )
)

gold_results.append(
    write_gold_table(
        fact_shipments_df,
        "fact_shipments",
    )
)

gold_results.append(
    write_gold_table(
        fact_inventory_daily_df,
        "fact_inventory_daily",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create business-facing views

# COMMAND ----------

executive_kpi_query = f"""
WITH order_metrics AS (
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity_ordered) AS units_ordered,
        SUM(gross_amount) AS gross_sales,
        SUM(discount_amount) AS total_discounts,
        SUM(net_amount) AS net_sales,
        AVG(discount_pct) AS average_discount_rate
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines
),
shipment_metrics AS (
    SELECT
        COUNT(*) AS total_shipments,
        SUM(shipped_quantity) AS units_shipped,
        SUM(delivered_quantity) AS units_delivered,
        SUM(shipping_cost) AS total_shipping_cost,
        AVG(fill_rate) AS average_fill_rate,
        AVG(
            CASE
                WHEN delivery_completed_flag THEN CAST(on_time_flag AS INT)
            END
        ) AS on_time_delivery_rate,
        AVG(CAST(in_full_flag AS INT)) AS in_full_rate,
        AVG(
            CASE
                WHEN delivery_completed_flag THEN CAST(otif_flag AS INT)
            END
        ) AS otif_rate,
        AVG(delay_days) AS average_delay_days
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments
),
inventory_metrics AS (
    SELECT
        SUM(on_hand_qty) AS total_on_hand_units,
        SUM(available_qty) AS total_available_units,
        SUM(CASE WHEN stockout_flag THEN 1 ELSE 0 END) AS stockout_positions,
        SUM(
            CASE
                WHEN below_reorder_point_flag THEN 1 ELSE 0
            END
        ) AS below_reorder_positions,
        COUNT(*) AS inventory_positions
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily
)
SELECT
    o.total_orders,
    o.units_ordered,
    ROUND(o.gross_sales, 2) AS gross_sales,
    ROUND(o.total_discounts, 2) AS total_discounts,
    ROUND(o.net_sales, 2) AS net_sales,
    ROUND(100 * o.average_discount_rate, 2) AS average_discount_pct,
    s.total_shipments,
    s.units_shipped,
    s.units_delivered,
    ROUND(s.total_shipping_cost, 2) AS total_shipping_cost,
    ROUND(100 * s.average_fill_rate, 2) AS fill_rate_pct,
    ROUND(100 * s.on_time_delivery_rate, 2) AS on_time_delivery_pct,
    ROUND(100 * s.in_full_rate, 2) AS in_full_pct,
    ROUND(100 * s.otif_rate, 2) AS otif_pct,
    ROUND(s.average_delay_days, 2) AS average_delay_days,
    i.total_on_hand_units,
    i.total_available_units,
    i.stockout_positions,
    i.below_reorder_positions,
    i.inventory_positions,
    ROUND(
        100 * i.stockout_positions / NULLIF(i.inventory_positions, 0),
        2
    ) AS stockout_rate_pct,
    ROUND(
        100 * i.below_reorder_positions
        / NULLIF(i.inventory_positions, 0),
        2
    ) AS below_reorder_rate_pct
FROM order_metrics o
CROSS JOIN shipment_metrics s
CROSS JOIN inventory_metrics i
"""

# COMMAND ----------

warehouse_performance_query = f"""
SELECT
    w.warehouse_key,
    w.warehouse_id,
    w.warehouse_name,
    w.province,
    w.city,
    COUNT(f.shipment_id) AS total_shipments,
    SUM(f.shipped_quantity) AS units_shipped,
    SUM(f.delivered_quantity) AS units_delivered,
    ROUND(SUM(f.shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(f.shipping_cost), 2) AS average_shipping_cost,
    ROUND(100 * AVG(f.fill_rate), 2) AS fill_rate_pct,
    ROUND(
        100 * AVG(
            CASE
                WHEN f.delivery_completed_flag
                THEN CAST(f.on_time_flag AS INT)
            END
        ),
        2
    ) AS on_time_delivery_pct,
    ROUND(
        100 * AVG(
            CASE
                WHEN f.delivery_completed_flag
                THEN CAST(f.otif_flag AS INT)
            END
        ),
        2
    ) AS otif_pct,
    ROUND(AVG(f.delivery_lead_time_days), 2) AS average_lead_time_days,
    ROUND(AVG(f.delay_days), 2) AS average_delay_days
FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments f
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_warehouse w
    ON f.warehouse_key = w.warehouse_key
GROUP BY
    w.warehouse_key,
    w.warehouse_id,
    w.warehouse_name,
    w.province,
    w.city
"""

# COMMAND ----------

inventory_risk_query = f"""
SELECT
    i.snapshot_date,
    w.warehouse_id,
    w.warehouse_name,
    w.province AS warehouse_province,
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.brand,
    s.supplier_id,
    s.supplier_name,
    i.on_hand_qty,
    i.reserved_qty,
    i.available_qty,
    i.reorder_point,
    i.inventory_status,
    i.stockout_flag,
    i.below_reorder_point_flag,
    ROUND(i.available_to_reorder_ratio, 2) AS available_to_reorder_ratio
FROM {CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily i
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_warehouse w
    ON i.warehouse_key = w.warehouse_key
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product p
    ON i.product_key = p.product_key
INNER JOIN {CATALOG}.{GOLD_SCHEMA}.dim_supplier s
    ON i.supplier_key = s.supplier_key
WHERE i.below_reorder_point_flag = TRUE
"""

# COMMAND ----------

store_performance_query = f"""
WITH order_performance AS (
    SELECT
        store_key,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity_ordered) AS units_ordered,
        SUM(gross_amount) AS gross_sales,
        SUM(discount_amount) AS total_discounts,
        SUM(net_amount) AS net_sales
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines
    GROUP BY store_key
),
shipment_performance AS (
    SELECT
        store_key,
        COUNT(*) AS total_shipments,
        ROUND(100 * AVG(fill_rate), 2) AS fill_rate_pct,
        ROUND(
            100 * AVG(
                CASE
                    WHEN delivery_completed_flag
                    THEN CAST(on_time_flag AS INT)
                END
            ),
            2
        ) AS on_time_delivery_pct,
        ROUND(
            100 * AVG(
                CASE
                    WHEN delivery_completed_flag
                    THEN CAST(otif_flag AS INT)
                END
            ),
            2
        ) AS otif_pct,
        ROUND(AVG(delay_days), 2) AS average_delay_days
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments
    GROUP BY store_key
)
SELECT
    s.store_key,
    s.store_id,
    s.store_name,
    s.channel,
    s.province,
    s.city,
    s.priority_level,
    o.total_orders,
    o.units_ordered,
    ROUND(o.gross_sales, 2) AS gross_sales,
    ROUND(o.total_discounts, 2) AS total_discounts,
    ROUND(o.net_sales, 2) AS net_sales,
    sh.total_shipments,
    sh.fill_rate_pct,
    sh.on_time_delivery_pct,
    sh.otif_pct,
    sh.average_delay_days
FROM {CATALOG}.{GOLD_SCHEMA}.dim_store s
LEFT JOIN order_performance o
    ON s.store_key = o.store_key
LEFT JOIN shipment_performance sh
    ON s.store_key = sh.store_key
"""

# COMMAND ----------

gold_results.append(
    create_business_view(
        "vw_executive_supply_chain_kpis",
        executive_kpi_query,
    )
)

gold_results.append(
    create_business_view(
        "vw_warehouse_delivery_performance",
        warehouse_performance_query,
    )
)

gold_results.append(
    create_business_view(
        "vw_product_inventory_risk",
        inventory_risk_query,
    )
)

gold_results.append(
    create_business_view(
        "vw_store_order_performance",
        store_performance_query,
    )
)

# COMMAND ----------

gold_results_df = spark.createDataFrame(gold_results)

display(
    gold_results_df.orderBy(
        "object_type",
        "object_name",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold data-quality and reconciliation tests

# COMMAND ----------

# Reconcile fact grains against their validated Silver sources.
silver_order_item_count = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_order_items"
).count()

gold_order_line_count = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.fact_order_lines"
).count()

silver_shipment_count = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_shipments"
).count()

gold_shipment_count = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.fact_shipments"
).count()

silver_inventory_count = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.silver_inventory_snapshots"
).count()

gold_inventory_count = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily"
).count()

reconciliation_tests = [
    (
        "order_lines_match_silver_order_items",
        silver_order_item_count,
        gold_order_line_count,
        silver_order_item_count == gold_order_line_count,
    ),
    (
        "shipments_match_silver_shipments",
        silver_shipment_count,
        gold_shipment_count,
        silver_shipment_count == gold_shipment_count,
    ),
    (
        "inventory_matches_silver_inventory",
        silver_inventory_count,
        gold_inventory_count,
        silver_inventory_count == gold_inventory_count,
    ),
]

reconciliation_tests_df = spark.createDataFrame(
    reconciliation_tests,
    [
        "test_name",
        "expected_rows",
        "actual_rows",
        "passed",
    ],
)

display(reconciliation_tests_df)

failed_reconciliations = (
    reconciliation_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_reconciliations == 0, (
    f"{failed_reconciliations} Gold reconciliation tests failed."
)

print("PASS: Gold fact grains reconcile with Silver.")

# COMMAND ----------

# Validate dimension surrogate-key uniqueness.
dimension_key_tests = [
    ("dim_date", "date_key"),
    ("dim_supplier", "supplier_key"),
    ("dim_product", "product_key"),
    ("dim_store", "store_key"),
    ("dim_warehouse", "warehouse_key"),
]

dimension_test_rows = []

for table_name, key_column in dimension_key_tests:
    table_df = spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    )

    duplicate_key_groups = (
        table_df
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_key_rows = table_df.filter(
        F.col(key_column).isNull()
    ).count()

    dimension_test_rows.append(
        (
            table_name,
            key_column,
            duplicate_key_groups,
            null_key_rows,
            duplicate_key_groups == 0 and null_key_rows == 0,
        )
    )

dimension_tests_df = spark.createDataFrame(
    dimension_test_rows,
    [
        "table_name",
        "key_column",
        "duplicate_key_groups",
        "null_key_rows",
        "passed",
    ],
)

display(dimension_tests_df)

failed_dimension_tests = (
    dimension_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_dimension_tests == 0, (
    f"{failed_dimension_tests} dimension key tests failed."
)

print("PASS: all Gold dimension keys are complete and unique.")

# COMMAND ----------

# Validate fact-to-dimension relationships with LEFT ANTI joins.
foreign_key_tests = [
    (
        "order_lines_to_products",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product d
            ON f.product_key = d.product_key
        """,
    ),
    (
        "order_lines_to_stores",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store d
            ON f.store_key = d.store_key
        """,
    ),
    (
        "order_lines_to_suppliers",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_supplier d
            ON f.supplier_key = d.supplier_key
        """,
    ),
    (
        "shipments_to_warehouses",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_warehouse d
            ON f.warehouse_key = d.warehouse_key
        """,
    ),
    (
        "shipments_to_stores",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_store d
            ON f.store_key = d.store_key
        """,
    ),
    (
        "inventory_to_warehouses",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_warehouse d
            ON f.warehouse_key = d.warehouse_key
        """,
    ),
    (
        "inventory_to_products",
        f"""
        SELECT COUNT(*) AS failures
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily f
        LEFT ANTI JOIN {CATALOG}.{GOLD_SCHEMA}.dim_product d
            ON f.product_key = d.product_key
        """,
    ),
]

foreign_key_test_rows = []

for test_name, query in foreign_key_tests:
    failure_count = int(
        spark.sql(query).first()["failures"]
    )

    foreign_key_test_rows.append(
        (
            test_name,
            failure_count,
            failure_count == 0,
        )
    )

foreign_key_tests_df = spark.createDataFrame(
    foreign_key_test_rows,
    [
        "test_name",
        "failure_count",
        "passed",
    ],
)

display(foreign_key_tests_df)

failed_foreign_key_tests = (
    foreign_key_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_foreign_key_tests == 0, (
    f"{failed_foreign_key_tests} Gold foreign-key tests failed."
)

print("PASS: all Gold fact-to-dimension relationships are valid.")

# COMMAND ----------

# Validate critical business metrics and numeric ranges.
business_rule_tests = [
    (
        "negative_order_amounts",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{GOLD_SCHEMA}.fact_order_lines
            WHERE gross_amount < 0
               OR discount_amount < 0
               OR net_amount < 0
            """
        ).first()["failures"],
    ),
    (
        "invalid_shipment_fill_rate",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{GOLD_SCHEMA}.fact_shipments
            WHERE fill_rate < 0 OR fill_rate > 1
            """
        ).first()["failures"],
    ),
    (
        "negative_inventory_quantities",
        spark.sql(
            f"""
            SELECT COUNT(*) AS failures
            FROM {CATALOG}.{GOLD_SCHEMA}.fact_inventory_daily
            WHERE on_hand_qty < 0
               OR reserved_qty < 0
               OR available_qty < 0
            """
        ).first()["failures"],
    ),
]

business_rule_test_rows = [
    (
        test_name,
        int(failure_count),
        int(failure_count) == 0,
    )
    for test_name, failure_count in business_rule_tests
]

business_rule_tests_df = spark.createDataFrame(
    business_rule_test_rows,
    [
        "test_name",
        "failure_count",
        "passed",
    ],
)

display(business_rule_tests_df)

failed_business_rule_tests = (
    business_rule_tests_df
    .filter(~F.col("passed"))
    .count()
)

assert failed_business_rule_tests == 0, (
    f"{failed_business_rule_tests} Gold business-rule tests failed."
)

print("PASS: Gold business metrics satisfy expected ranges.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview business outputs

# COMMAND ----------

display(
    spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}."
        "vw_executive_supply_chain_kpis"
    )
)

# COMMAND ----------

display(
    spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}."
        "vw_warehouse_delivery_performance"
    )
    .orderBy(
        F.col("otif_pct").asc_nulls_last()
    )
)

# COMMAND ----------

display(
    spark.table(
        f"{CATALOG}.{GOLD_SCHEMA}."
        "vw_product_inventory_risk"
    )
    .orderBy(
        "inventory_status",
        "available_qty",
    )
    .limit(30)
)

# COMMAND ----------

# Review audit records generated by this run.
display(
    spark.table(GOLD_AUDIT_TABLE)
    .filter(F.col("run_id") == GOLD_RUN_ID)
    .orderBy(
        "object_type",
        "object_name",
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"SHOW TABLES IN {CATALOG}.{GOLD_SCHEMA}"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency test
# MAGIC
# MAGIC Run this notebook a second time without changing Silver.
# MAGIC
# MAGIC Expected behavior:
# MAGIC - all Gold row counts remain unchanged;
# MAGIC - no duplicate surrogate keys appear;
# MAGIC - all reconciliation and relationship tests pass;
# MAGIC - business views return the same KPI values;
# MAGIC - a new audit record is added for each rebuilt object.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_gold.vw_executive_supply_chain_kpis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_gold.vw_warehouse_delivery_performance
# MAGIC ORDER BY otif_pct ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_gold.vw_product_inventory_risk
# MAGIC WHERE inventory_status = 'STOCKOUT'
# MAGIC ORDER BY warehouse_name, product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM scm_gold.vw_store_order_performance
# MAGIC ORDER BY net_sales DESC;