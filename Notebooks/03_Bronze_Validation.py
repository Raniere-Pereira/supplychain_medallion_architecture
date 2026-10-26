# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM supplychain.bronze.dataco_supply_chain
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE supplychain.bronze.dataco_supply_chain;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM supplychain.bronze.dataco_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(_ingestion_timestamp) AS first_ingestion,
# MAGIC     MAX(_ingestion_timestamp) AS last_ingestion
# MAGIC FROM supplychain.bronze.dataco_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM supplychain.bronze.dataco_supply_chain;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT `Order Id`) AS distinct_orders
# MAGIC FROM supplychain.bronze.dataco_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     `_source_file_path` AS _source_file,
# MAGIC     COUNT(*) AS records
# MAGIC FROM supplychain.bronze.dataco_supply_chain
# MAGIC GROUP BY _source_file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(_ingestion_timestamp) AS min_ingestion,
# MAGIC     MAX(_ingestion_timestamp) AS max_ingestion
# MAGIC FROM supplychain.bronze.dataco_supply_chain