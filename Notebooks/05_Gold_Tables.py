# Databricks notebook source
# DBTITLE 1,Title and Description
# MAGIC %md
# MAGIC # Gold Layer — Supply Chain (DataCo)
# MAGIC
# MAGIC Builds the **Gold** Star Schema tables from the Silver Delta table:
# MAGIC
# MAGIC * `dim_customers` — Customer dimension (1 row per `customer_id`, excluding sensitive password/street fields)
# MAGIC * `dim_products` — Product dimension with denormalized category and department (1 row per `product_card_id`)
# MAGIC * `dim_markets` — Geographic market hierarchy (`market > region > country > state > city`)
# MAGIC * `fact_sales` — Order-item grain fact table (`order_item_id`) with Liquid Clustering by `order_date`

# COMMAND ----------

# DBTITLE 1,1. Parameters, Logging & Gold Schema Setup
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("supplychain_gold_tables")


def get_param(name: str, default: str) -> str:
    """Read parameter from Databricks widgets with fallback to env vars/default."""
    try:
        dbutils.widgets.text(name, default)  # type: ignore[name-defined]
        value = dbutils.widgets.get(name).strip()
        return value or default
    except NameError:
        return os.getenv(name.upper(), default)


CATALOG = get_param("catalog", "supplychain")
SILVER_SCHEMA = get_param("silver_schema", "silver")
GOLD_SCHEMA = get_param("gold_schema", "gold")

SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dataco_supply_chain"
GOLD_PREFIX = f"{CATALOG}.{GOLD_SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_PREFIX}")

silver_df = spark.table(SILVER_TABLE)
silver_count = silver_df.count()
logger.info("Silver source %s loaded with %d rows", SILVER_TABLE, silver_count)
display(
    silver_df.select(
        "order_item_id",
        "order_id",
        "customer_id",
        "product_card_id",
        "order_date",
        "sales",
        "order_item_quantity",
    ).limit(5)
)

# COMMAND ----------

# DBTITLE 1,2. dim_customers
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_PREFIX}.dim_customers
    COMMENT 'Customer dimension (1 row per customer_id). Source: {SILVER_TABLE}. Password and full street address excluded for security.'
    AS
    SELECT
        customer_id,
        first(customer_fname IGNORE NULLS)   AS first_name,
        first(customer_lname IGNORE NULLS)   AS last_name,
        first(customer_email IGNORE NULLS)   AS email,
        first(customer_segment IGNORE NULLS) AS segment,
        first(customer_city IGNORE NULLS)    AS city,
        first(customer_state IGNORE NULLS)   AS state,
        first(customer_country IGNORE NULLS) AS country,
        first(customer_zipcode IGNORE NULLS) AS zipcode,
        first(latitude IGNORE NULLS)         AS latitude,
        first(longitude IGNORE NULLS)        AS longitude,
        min(order_date)                      AS first_order_date,
        max(order_date)                      AS last_order_date
    FROM {SILVER_TABLE}
    GROUP BY customer_id
""")

display(spark.sql(f"SELECT COUNT(*) AS total_customers FROM {GOLD_PREFIX}.dim_customers"))

# COMMAND ----------

# DBTITLE 1,3. dim_products
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_PREFIX}.dim_products
    COMMENT 'Product dimension (1 row per product_card_id) with denormalized category and department. Source: {SILVER_TABLE}.'
    AS
    SELECT
        product_card_id,
        first(product_name IGNORE NULLS)        AS product_name,
        first(product_description IGNORE NULLS) AS product_description,
        first(product_image IGNORE NULLS)       AS product_image,
        first(product_price IGNORE NULLS)       AS list_price,
        first(category_id IGNORE NULLS)         AS category_id,
        first(category_name IGNORE NULLS)       AS category_name,
        first(department_id IGNORE NULLS)       AS department_id,
        first(department_name IGNORE NULLS)     AS department_name
    FROM {SILVER_TABLE}
    GROUP BY product_card_id
""")

display(spark.sql(f"SELECT COUNT(*) AS total_products FROM {GOLD_PREFIX}.dim_products"))

# COMMAND ----------

# DBTITLE 1,4. dim_markets
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_PREFIX}.dim_markets
    COMMENT 'Market/destination hierarchy dimension (market > region > country > state > city). Source: {SILVER_TABLE}.'
    AS
    SELECT DISTINCT
        md5(concat_ws('|', market, order_region, order_country, order_state, order_city)) AS market_key,
        market,
        order_region  AS region,
        order_country AS country,
        order_state   AS state,
        order_city    AS city
    FROM {SILVER_TABLE}
""")

display(spark.sql(f"SELECT COUNT(*) AS total_markets FROM {GOLD_PREFIX}.dim_markets"))

# COMMAND ----------

# DBTITLE 1,5. fact_sales
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_PREFIX}.fact_sales
    COMMENT 'Sales fact table at order-item grain (order_item_id) with commercial and logistics metrics. Source: {SILVER_TABLE}.'
    CLUSTER BY (order_date)
    AS
    SELECT
        -- keys
        s.order_item_id,
        s.order_id,
        s.customer_id,
        s.product_card_id,
        md5(concat_ws('|', s.market, s.order_region, s.order_country, s.order_state, s.order_city)) AS market_key,
        -- dates
        s.order_date,
        s.order_time,
        s.shipping_date,
        s.shipping_time,
        -- payment and status
        s.type                     AS payment_type,
        s.order_status,
        s.delivery_status,
        s.shipping_mode,
        s.late_delivery_risk,
        -- commercial metrics
        s.order_item_quantity,
        s.order_item_discount,
        s.order_item_discount_rate,
        s.order_item_product_price,
        s.order_item_total,
        s.sales,
        s.order_profit_per_order,
        s.order_item_profit_ratio,
        s.benefit_per_order,
        s.sales_per_customer,
        -- logistics
        s.days_for_shipping_real,
        s.days_for_shipment_scheduled,
        s.shipping_delay_days,
        -- audit
        current_timestamp() AS _loaded_at
    FROM {SILVER_TABLE} AS s
""")

logger.info("Table %s.fact_sales created.", GOLD_PREFIX)

# COMMAND ----------

# DBTITLE 1,6. Gold Validation & Referential Integrity Gate
gold_tables = ["dim_customers", "dim_products", "dim_markets", "fact_sales"]
for table in gold_tables:
    row_count = spark.table(f"{GOLD_PREFIX}.{table}").count()
    logger.info("%s.%s: %d rows", GOLD_PREFIX, table, row_count)

integrity_df = spark.sql(f"""
    SELECT
        COUNT(*) AS total_facts,
        SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_customers,
        SUM(CASE WHEN p.product_card_id IS NULL THEN 1 ELSE 0 END) AS orphan_products,
        SUM(CASE WHEN m.market_key IS NULL THEN 1 ELSE 0 END) AS orphan_markets
    FROM {GOLD_PREFIX}.fact_sales AS f
    LEFT JOIN {GOLD_PREFIX}.dim_customers AS c ON f.customer_id = c.customer_id
    LEFT JOIN {GOLD_PREFIX}.dim_products AS p ON f.product_card_id = p.product_card_id
    LEFT JOIN {GOLD_PREFIX}.dim_markets AS m ON f.market_key = m.market_key
""")
display(integrity_df)

integrity_row = integrity_df.collect()[0]
assert integrity_row["total_facts"] == silver_count, (
    f"Volume mismatch: fact_sales={integrity_row['total_facts']} vs silver={silver_count}"
)
assert integrity_row["orphan_customers"] == 0, f"Orphan customers found: {integrity_row['orphan_customers']}"
assert integrity_row["orphan_products"] == 0, f"Orphan products found: {integrity_row['orphan_products']}"
assert integrity_row["orphan_markets"] == 0, f"Orphan markets found: {integrity_row['orphan_markets']}"
logger.info("Referential integrity and Silver-to-Gold reconciliation gates passed.")

display(spark.sql(f"""
    SELECT
        f.order_date,
        c.segment                AS customer_segment,
        c.country                AS customer_country,
        p.category_name          AS category,
        p.product_name           AS product,
        m.country                AS destination_country,
        f.order_item_quantity    AS quantity,
        f.sales                  AS sales,
        f.order_profit_per_order AS profit
    FROM {GOLD_PREFIX}.fact_sales AS f
    JOIN {GOLD_PREFIX}.dim_customers AS c ON f.customer_id = c.customer_id
    JOIN {GOLD_PREFIX}.dim_products AS p ON f.product_card_id = p.product_card_id
    JOIN {GOLD_PREFIX}.dim_markets AS m ON f.market_key = m.market_key
    LIMIT 10
"""))
