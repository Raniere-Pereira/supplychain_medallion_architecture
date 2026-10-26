# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE supplychain.silver.dataco_supply_chain
# MAGIC AS SELECT * FROM supplychain.bronze.dataco_supply_chain

# COMMAND ----------

display(spark.sql("SELECT * FROM supplychain.silver.dataco_supply_chain LIMIT 10"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE supplychain.silver.dataco_supply_chain
# MAGIC AS SELECT
# MAGIC   -- Strings (identificadores textuais, categorias, nomes)
# MAGIC   CAST(type                     AS STRING)               AS type,
# MAGIC   -- Inteiros (dias, contagens, IDs numéricos, flags)
# MAGIC   CAST(days_for_shipping_real   AS INT)                  AS days_for_shipping_real,
# MAGIC   CAST(days_for_shipment_scheduled AS INT)               AS days_for_shipment_scheduled,
# MAGIC   CAST(late_delivery_risk       AS INT)                  AS late_delivery_risk,
# MAGIC   CAST(category_id              AS INT)                  AS category_id,
# MAGIC   CAST(customer_id              AS INT)                  AS customer_id,
# MAGIC   CAST(order_customer_id        AS INT)                  AS order_customer_id,
# MAGIC   CAST(department_id            AS INT)                  AS department_id,
# MAGIC   CAST(order_id                 AS BIGINT)               AS order_id,
# MAGIC   CAST(order_item_cardprod_id   AS INT)                  AS order_item_cardprod_id,
# MAGIC   CAST(order_item_id            AS BIGINT)               AS order_item_id,
# MAGIC   CAST(order_item_quantity      AS INT)                  AS order_item_quantity,
# MAGIC   CAST(product_card_id           AS INT)                  AS product_card_id,
# MAGIC   CAST(product_category_id       AS INT)                  AS product_category_id,
# MAGIC   CAST(product_status           AS INT)                  AS product_status,
# MAGIC   -- Decimais / duplos (valores monetários, ratios, coordenadas)
# MAGIC   CAST(benefit_per_order         AS DOUBLE)               AS benefit_per_order,
# MAGIC   CAST(sales_per_customer        AS DOUBLE)               AS sales_per_customer,
# MAGIC   CAST(latitude                  AS DOUBLE)               AS latitude,
# MAGIC   CAST(longitude                 AS DOUBLE)               AS longitude,
# MAGIC   CAST(order_item_discount       AS DOUBLE)               AS order_item_discount,
# MAGIC   CAST(order_item_discount_rate  AS DOUBLE)               AS order_item_discount_rate,
# MAGIC   CAST(order_item_product_price  AS DOUBLE)               AS order_item_product_price,
# MAGIC   CAST(order_item_profit_ratio   AS DOUBLE)               AS order_item_profit_ratio,
# MAGIC   CAST(sales                     AS DOUBLE)               AS sales,
# MAGIC   CAST(order_item_total          AS DOUBLE)               AS order_item_total,
# MAGIC   CAST(order_profit_per_order    AS DOUBLE)               AS order_profit_per_order,
# MAGIC   CAST(product_price             AS DOUBLE)               AS product_price,
# MAGIC   -- Timestamps (datas de pedido e envio no formato M/D/yyyy H:mm)
# MAGIC   to_timestamp(order_date_dateorders,    'M/d/yyyy H:mm') AS order_date_dateorders,
# MAGIC   to_timestamp(shipping_date_dateorders, 'M/d/yyyy H:mm') AS shipping_date_dateorders,
# MAGIC   -- Strings restantes (textos, categorias, endereços, zipcodes)
# MAGIC   CAST(delivery_status           AS STRING)               AS delivery_status,
# MAGIC   CAST(category_name             AS STRING)               AS category_name,
# MAGIC   CAST(customer_city             AS STRING)               AS customer_city,
# MAGIC   CAST(customer_country          AS STRING)               AS customer_country,
# MAGIC   CAST(customer_email            AS STRING)               AS customer_email,
# MAGIC   CAST(customer_fname            AS STRING)               AS customer_fname,
# MAGIC   CAST(customer_lname            AS STRING)               AS customer_lname,
# MAGIC   CAST(customer_password         AS STRING)               AS customer_password,
# MAGIC   CAST(customer_segment          AS STRING)               AS customer_segment,
# MAGIC   CAST(customer_state            AS STRING)               AS customer_state,
# MAGIC   CAST(customer_street           AS STRING)               AS customer_street,
# MAGIC   CAST(customer_zipcode          AS STRING)               AS customer_zipcode,
# MAGIC   CAST(department_name           AS STRING)               AS department_name,
# MAGIC   CAST(market                    AS STRING)               AS market,
# MAGIC   CAST(order_city                AS STRING)               AS order_city,
# MAGIC   CAST(order_country             AS STRING)               AS order_country,
# MAGIC   CAST(order_region              AS STRING)               AS order_region,
# MAGIC   CAST(order_state               AS STRING)               AS order_state,
# MAGIC   CAST(order_status              AS STRING)               AS order_status,
# MAGIC   CAST(order_zipcode             AS STRING)               AS order_zipcode,
# MAGIC   CAST(product_description       AS STRING)               AS product_description,
# MAGIC   CAST(product_image             AS STRING)               AS product_image,
# MAGIC   CAST(product_name              AS STRING)               AS product_name,
# MAGIC   CAST(shipping_mode             AS STRING)               AS shipping_mode,
# MAGIC   -- Metadados de ingestão (preservados)
# MAGIC   _ingestion_timestamp,
# MAGIC   _source_file_name,
# MAGIC   _source_file_path
# MAGIC FROM supplychain.silver.dataco_supply_chain

# COMMAND ----------

# DBTITLE 1,Verificar schema tipado
display(spark.sql("DESCRIBE supplychain.silver.dataco_supply_chain"))

# COMMAND ----------

spark.sql("""
ALTER TABLE supplychain.silver.dataco_supply_chain
ADD COLUMNS (
  order_date    DATE,
  order_time    STRING,
  shipping_date DATE,
  shipping_time STRING
)
""")

spark.sql("""
UPDATE supplychain.silver.dataco_supply_chain
SET 
  order_date    = DATE(order_date_dateorders),
  order_time    = DATE_FORMAT(order_date_dateorders, 'HH:mm:ss'),
  shipping_date = DATE(shipping_date_dateorders),
  shipping_time = DATE_FORMAT(shipping_date_dateorders, 'HH:mm:ss')
""")

# COMMAND ----------

display(spark.sql("DESCRIBE supplychain.silver.dataco_supply_chain"))

# COMMAND ----------

display(spark.sql("SELECT * FROM supplychain.silver.dataco_supply_chain LIMIT 10"))

# COMMAND ----------

spark.sql("ALTER TABLE supplychain.silver.dataco_supply_chain ALTER COLUMN order_date AFTER order_date_dateorders")
spark.sql("ALTER TABLE supplychain.silver.dataco_supply_chain ALTER COLUMN order_time AFTER order_date")
spark.sql("ALTER TABLE supplychain.silver.dataco_supply_chain ALTER COLUMN shipping_date AFTER shipping_date_dateorders")
spark.sql("ALTER TABLE supplychain.silver.dataco_supply_chain ALTER COLUMN shipping_time AFTER shipping_date")

display(spark.sql("DESCRIBE supplychain.silver.dataco_supply_chain"))

# COMMAND ----------

spark.sql("""
ALTER TABLE supplychain.silver.dataco_supply_chain
ADD COLUMN shipping_delay_days INT
""")

spark.sql("""
UPDATE supplychain.silver.dataco_supply_chain
SET shipping_delay_days = days_for_shipment_scheduled - days_for_shipping_real
""")

display(spark.sql("SELECT order_id, days_for_shipping_real, days_for_shipment_scheduled, shipping_delay_days FROM supplychain.silver.dataco_supply_chain LIMIT 10"))

# COMMAND ----------

display(spark.sql("DESCRIBE supplychain.silver.dataco_supply_chain"))