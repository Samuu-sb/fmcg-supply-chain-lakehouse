# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate synthetic supply chain source data
# MAGIC
# MAGIC Generates master and transactional source files for the FMCG Supply Chain Lakehouse.
# MAGIC The data intentionally contains a small number of quality issues for later validation.

# COMMAND ----------

import csv
import json
import os
import random
from datetime import date, datetime, timedelta

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
LANDING_PATH = f"/Volumes/{CATALOG}/scm_bronze/landing"

print(f"Catalog: {CATALOG}")
print(f"Landing path: {LANDING_PATH}")

# Set this to True only when you intentionally want to regenerate all source files.
RESET_SOURCE_FILES = True

SOURCE_DIRECTORIES = [
    "suppliers",
    "products",
    "warehouses",
    "stores",
    "orders",
    "order_items",
    "shipments",
    "inventory_snapshots",
    "manifests",
]

if RESET_SOURCE_FILES:
    for source_directory in SOURCE_DIRECTORIES:
        dbutils.fs.rm(
            f"{LANDING_PATH}/{source_directory}",
            recurse=True,
        )
    print("Previous source files removed.")
else:
    print("Existing files preserved.")

# COMMAND ----------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def write_csv(rows, path):
    if not rows:
        raise ValueError(f"No rows supplied for {path}")

    ensure_dir(os.path.dirname(path))

    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json_lines(rows, path):
    if not rows:
        raise ValueError(f"No rows supplied for {path}")

    ensure_dir(os.path.dirname(path))

    with open(path, "w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def iso_timestamp(value=None):
    value = value or datetime.now()
    return value.replace(microsecond=0).isoformat()

# COMMAND ----------

# Suppliers
supplier_countries = [
    "Panama",
    "Mexico",
    "Colombia",
    "Costa Rica",
    "Guatemala",
    "Brazil",
]

suppliers = []

for supplier_number in range(1, 16):
    suppliers.append(
        {
            "supplier_id": f"SUP{supplier_number:03d}",
            "supplier_name": f"Supplier {supplier_number:02d}",
            "country": random.choice(supplier_countries),
            "lead_time_days": random.randint(3, 21),
            "reliability_score": round(random.uniform(0.82, 0.99), 3),
            "active_flag": True,
            "updated_at": iso_timestamp(
                datetime(2026, 7, 1, 8, 0)
                + timedelta(minutes=supplier_number)
            ),
        }
    )

# Products
categories = {
    "Laundry": ["Detergent", "Fabric Softener", "Stain Remover"],
    "Baby Care": ["Diapers", "Baby Wipes"],
    "Personal Care": ["Shampoo", "Conditioner", "Deodorant"],
    "Oral Care": ["Toothpaste", "Toothbrush", "Mouthwash"],
    "Home Care": ["Surface Cleaner", "Dish Soap", "Air Freshener"],
}

package_sizes = ["Travel", "Small", "Medium", "Large", "Family"]
brands = ["Nova", "PureLife", "Bright", "CarePlus", "FreshHome", "DailyPro"]

products = []
product_number = 1

for category, product_types in categories.items():
    for product_type in product_types:
        for package_size in package_sizes:
            supplier = random.choice(suppliers)
            unit_cost = round(random.uniform(1.50, 18.00), 2)
            unit_price = round(unit_cost * random.uniform(1.25, 1.75), 2)
            brand = random.choice(brands)

            products.append(
                {
                    "product_id": f"PRD{product_number:04d}",
                    "sku": f"{category[:2].upper()}-{product_number:05d}",
                    "product_name": (
                        f"{brand} {product_type} {package_size}"
                    ),
                    "category": category,
                    "brand": brand,
                    "package_size": package_size,
                    "unit_price": unit_price,
                    "unit_cost": unit_cost,
                    "supplier_id": supplier["supplier_id"],
                    "weight_kg": round(random.uniform(0.10, 8.00), 3),
                    "active_flag": True,
                    "updated_at": iso_timestamp(
                        datetime(2026, 7, 1, 9, 0)
                        + timedelta(minutes=product_number)
                    ),
                }
            )

            product_number += 1

# Warehouses
warehouses = [
    {
        "warehouse_id": "WH001",
        "warehouse_name": "Panama Pacific Distribution Center",
        "province": "Panama Oeste",
        "city": "Arraijan",
        "capacity_units": 250000,
        "opened_date": "2018-03-15",
        "status": "ACTIVE",
    },
    {
        "warehouse_id": "WH002",
        "warehouse_name": "Tocumen Distribution Center",
        "province": "Panama",
        "city": "Tocumen",
        "capacity_units": 300000,
        "opened_date": "2015-07-20",
        "status": "ACTIVE",
    },
    {
        "warehouse_id": "WH003",
        "warehouse_name": "Colon Regional Warehouse",
        "province": "Colon",
        "city": "Colon",
        "capacity_units": 180000,
        "opened_date": "2017-11-10",
        "status": "ACTIVE",
    },
    {
        "warehouse_id": "WH004",
        "warehouse_name": "Chiriqui Regional Warehouse",
        "province": "Chiriqui",
        "city": "David",
        "capacity_units": 150000,
        "opened_date": "2020-02-01",
        "status": "ACTIVE",
    },
    {
        "warehouse_id": "WH005",
        "warehouse_name": "Central Provinces Warehouse",
        "province": "Cocle",
        "city": "Penonome",
        "capacity_units": 120000,
        "opened_date": "2021-05-12",
        "status": "ACTIVE",
    },
]

for index, warehouse in enumerate(warehouses, start=1):
    warehouse["updated_at"] = iso_timestamp(
        datetime(2026, 7, 1, 10, 0) + timedelta(minutes=index)
    )

# Stores
province_cities = {
    "Panama": ["Panama City", "San Miguelito", "Tocumen"],
    "Panama Oeste": ["La Chorrera", "Arraijan", "Chame"],
    "Colon": ["Colon", "Sabanitas"],
    "Chiriqui": ["David", "Boquete", "Bugaba"],
    "Cocle": ["Penonome", "Aguadulce"],
    "Veraguas": ["Santiago", "Atalaya"],
    "Herrera": ["Chitre"],
    "Los Santos": ["Las Tablas"],
}

channels = [
    "SUPERMARKET",
    "PHARMACY",
    "WHOLESALE",
    "CONVENIENCE",
    "E_COMMERCE",
]

stores = []
store_number = 1

for province, cities in province_cities.items():
    store_count = 12 if province == "Panama" else 7

    for _ in range(store_count):
        stores.append(
            {
                "store_id": f"STR{store_number:04d}",
                "store_name": (
                    f"{random.choice(['Market', 'Retail', 'Commerce', 'Store'])} "
                    f"{store_number:03d}"
                ),
                "channel": random.choices(
                    channels,
                    weights=[35, 20, 15, 20, 10],
                    k=1,
                )[0],
                "province": province,
                "city": random.choice(cities),
                "priority_level": random.choices(
                    ["HIGH", "MEDIUM", "LOW"],
                    weights=[20, 55, 25],
                    k=1,
                )[0],
                "active_flag": True,
                "updated_at": iso_timestamp(
                    datetime(2026, 7, 1, 11, 0)
                    + timedelta(minutes=store_number)
                ),
            }
        )

        store_number += 1

print(
    {
        "suppliers": len(suppliers),
        "products": len(products),
        "warehouses": len(warehouses),
        "stores": len(stores),
    }
)

# COMMAND ----------

def generate_batch(
    batch_id,
    start_date,
    number_of_orders=500,
    inject_errors=True,
):
    orders = []
    order_items = []
    shipments = []
    inventory_snapshots = []

    carriers = [
        "DHL",
        "FedEx",
        "LocalExpress",
        "CargoPanama",
        "RapidShip",
    ]

    order_statuses = [
        "CREATED",
        "PROCESSING",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
    ]

    base_order_number = (batch_id - 1) * 100000

    for order_index in range(1, number_of_orders + 1):
        order_id = f"ORD{base_order_number + order_index:07d}"
        store = random.choice(stores)

        order_date = start_date + timedelta(days=random.randint(0, 6))
        requested_delivery_date = order_date + timedelta(
            days=random.randint(2, 7)
        )

        order_status = random.choices(
            order_statuses,
            weights=[5, 10, 20, 60, 5],
            k=1,
        )[0]

        order = {
            "order_id": order_id,
            "store_id": store["store_id"],
            "order_date": order_date.isoformat(),
            "requested_delivery_date": requested_delivery_date.isoformat(),
            "order_status": order_status,
            "currency": "USD",
            "source_system": random.choice(
                ["ERP", "B2B_PORTAL", "MOBILE_SALES"]
            ),
            "updated_at": iso_timestamp(
                datetime.combine(order_date, datetime.min.time())
                + timedelta(hours=18)
            ),
        }

        orders.append(order)

        number_of_items = random.randint(1, 6)
        selected_products = random.sample(
            products,
            k=number_of_items,
        )

        total_quantity = 0

        for line_number, product in enumerate(
            selected_products,
            start=1,
        ):
            quantity = random.randint(5, 80)
            discount_pct = random.choice(
                [0, 0, 0.05, 0.10, 0.15]
            )

            line_amount = round(
                quantity
                * product["unit_price"]
                * (1 - discount_pct),
                2,
            )

            total_quantity += quantity

            order_items.append(
                {
                    "order_item_id": (
                        f"{order_id}-{line_number:02d}"
                    ),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity_ordered": quantity,
                    "unit_price": product["unit_price"],
                    "discount_pct": discount_pct,
                    "line_amount": line_amount,
                    "updated_at": order["updated_at"],
                }
            )

        warehouse = random.choice(warehouses)
        shipment_date = order_date + timedelta(
            days=random.randint(0, 3)
        )

        is_late = random.random() < 0.18

        transit_days = random.randint(1, 4)

        if is_late:
            transit_days += random.randint(2, 5)

        actual_delivery_date = shipment_date + timedelta(
            days=transit_days
        )

        delivered_quantity = max(
            0,
            total_quantity
            - random.choices(
                [0, 1, 2, 5],
                weights=[85, 8, 5, 2],
                k=1,
            )[0],
        )

        if order_status in {
            "CREATED",
            "PROCESSING",
            "CANCELLED",
        }:
            actual_delivery_value = ""
            shipment_status = order_status
        else:
            actual_delivery_value = (
                actual_delivery_date.isoformat()
            )
            shipment_status = (
                "DELIVERED"
                if order_status == "DELIVERED"
                else "IN_TRANSIT"
            )

        shipments.append(
            {
                "shipment_id": (
                    f"SHP{base_order_number + order_index:07d}"
                ),
                "order_id": order_id,
                "warehouse_id": warehouse["warehouse_id"],
                "shipment_date": shipment_date.isoformat(),
                "actual_delivery_date": actual_delivery_value,
                "carrier": random.choice(carriers),
                "shipment_status": shipment_status,
                "shipped_quantity": total_quantity,
                "delivered_quantity": delivered_quantity,
                "shipping_cost": round(
                    random.uniform(15.00, 240.00),
                    2,
                ),
                "updated_at": iso_timestamp(
                    datetime.combine(
                        shipment_date,
                        datetime.min.time(),
                    )
                    + timedelta(hours=20)
                ),
            }
        )

    snapshot_date = start_date + timedelta(days=6)

    for warehouse in warehouses:
        for product in products:
            on_hand_quantity = random.randint(0, 500)
            reserved_quantity = random.randint(
                0,
                min(100, on_hand_quantity),
            )

            inventory_snapshots.append(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "warehouse_id": warehouse["warehouse_id"],
                    "product_id": product["product_id"],
                    "on_hand_qty": on_hand_quantity,
                    "reserved_qty": reserved_quantity,
                    "available_qty": (
                        on_hand_quantity - reserved_quantity
                    ),
                    "reorder_point": random.randint(40, 120),
                    "updated_at": iso_timestamp(
                        datetime.combine(
                            snapshot_date,
                            datetime.min.time(),
                        )
                        + timedelta(hours=23)
                    ),
                }
            )

    if inject_errors:
        # Orders
        duplicate_order = orders[10].copy()
        duplicate_order["updated_at"] = iso_timestamp(
            datetime.fromisoformat(
                duplicate_order["updated_at"]
            )
            + timedelta(hours=1)
        )
        orders.append(duplicate_order)

        orders[20]["store_id"] = ""
        orders[30]["order_date"] = "2026-13-40"
        orders[40]["store_id"] = "STR9999"

        # Order items
        order_items[15]["quantity_ordered"] = -10
        order_items[25]["product_id"] = "PRD9999"

        duplicate_item = order_items[35].copy()
        order_items.append(duplicate_item)

        order_items[45]["line_amount"] = -999.99

        # Shipments
        shipments[12]["actual_delivery_date"] = (
            date.fromisoformat(
                shipments[12]["shipment_date"]
            )
            - timedelta(days=2)
        ).isoformat()

        shipments[22]["order_id"] = "ORD9999999"
        shipments[32]["shipping_cost"] = -50.00

        shipments[42]["delivered_quantity"] = (
            shipments[42]["shipped_quantity"] + 25
        )

        # Inventory
        inventory_snapshots[5]["on_hand_qty"] = -20
        inventory_snapshots[15]["warehouse_id"] = "WH999"

        inventory_snapshots[25]["available_qty"] = (
            inventory_snapshots[25]["on_hand_qty"] + 50
        )

    return (
        orders,
        order_items,
        shipments,
        inventory_snapshots,
    )

# COMMAND ----------

BATCH_ID = 1
BATCH_LABEL = f"batch_{BATCH_ID:03d}"
BATCH_START_DATE = date(2026, 7, 1)

(
    orders,
    order_items,
    shipments,
    inventory_snapshots,
) = generate_batch(
    batch_id=BATCH_ID,
    start_date=BATCH_START_DATE,
    number_of_orders=500,
    inject_errors=True,
)

# Master data
write_csv(
    suppliers,
    f"{LANDING_PATH}/suppliers/suppliers_master.csv",
)
write_csv(
    products,
    f"{LANDING_PATH}/products/products_master.csv",
)
write_csv(
    warehouses,
    f"{LANDING_PATH}/warehouses/warehouses_master.csv",
)
write_csv(
    stores,
    f"{LANDING_PATH}/stores/stores_master.csv",
)

# Transactional data
write_csv(
    orders,
    f"{LANDING_PATH}/orders/orders_{BATCH_LABEL}.csv",
)
write_csv(
    order_items,
    (
        f"{LANDING_PATH}/order_items/"
        f"order_items_{BATCH_LABEL}.csv"
    ),
)
write_csv(
    shipments,
    (
        f"{LANDING_PATH}/shipments/"
        f"shipments_{BATCH_LABEL}.csv"
    ),
)
write_json_lines(
    inventory_snapshots,
    (
        f"{LANDING_PATH}/inventory_snapshots/"
        f"inventory_{BATCH_LABEL}.json"
    ),
)

manifest = {
    "batch_id": BATCH_LABEL,
    "generated_at": iso_timestamp(),
    "random_seed": RANDOM_SEED,
    "source_period_start": BATCH_START_DATE.isoformat(),
    "source_period_end": (
        BATCH_START_DATE + timedelta(days=6)
    ).isoformat(),
    "files": {
        "orders": len(orders),
        "order_items": len(order_items),
        "shipments": len(shipments),
        "inventory_snapshots": len(inventory_snapshots),
    },
    "contains_intentional_quality_issues": True,
}

write_json_lines(
    [manifest],
    (
        f"{LANDING_PATH}/manifests/"
        f"{BATCH_LABEL}_manifest.json"
    ),
)

generation_summary = {
    "suppliers": len(suppliers),
    "products": len(products),
    "warehouses": len(warehouses),
    "stores": len(stores),
    "orders": len(orders),
    "order_items": len(order_items),
    "shipments": len(shipments),
    "inventory_snapshots": len(inventory_snapshots),
}

print("Generation completed.")
print(generation_summary)

# COMMAND ----------

# Display the files created in every source directory.
for source_directory in SOURCE_DIRECTORIES:
    print(f"\n{source_directory.upper()}")
    display(
        dbutils.fs.ls(
            f"{LANDING_PATH}/{source_directory}"
        )
    )

# COMMAND ----------

# Validate that Spark can read every source.
validation_results = [
    (
        "suppliers",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/suppliers")
        .count(),
    ),
    (
        "products",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/products")
        .count(),
    ),
    (
        "warehouses",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/warehouses")
        .count(),
    ),
    (
        "stores",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/stores")
        .count(),
    ),
    (
        "orders",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/orders")
        .count(),
    ),
    (
        "order_items",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/order_items")
        .count(),
    ),
    (
        "shipments",
        spark.read.option("header", True)
        .csv(f"{LANDING_PATH}/shipments")
        .count(),
    ),
    (
        "inventory_snapshots",
        spark.read.json(
            f"{LANDING_PATH}/inventory_snapshots"
        ).count(),
    ),
]

validation_df = spark.createDataFrame(
    validation_results,
    ["source_name", "row_count"],
)

display(validation_df.orderBy("source_name"))

# COMMAND ----------

# Preview source records.
display(
    spark.read.option("header", True)
    .csv(f"{LANDING_PATH}/orders")
    .limit(10)
)

display(
    spark.read.json(
        f"{LANDING_PATH}/inventory_snapshots"
    )
    .limit(10)
)

# COMMAND ----------

orders_path = (
    f"/Volumes/{CATALOG}/"
    "scm_bronze/landing/orders"
)

orders_df = (
    spark.read
    .option("header", True)
    .csv(orders_path)
)

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

display(
    orders_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)