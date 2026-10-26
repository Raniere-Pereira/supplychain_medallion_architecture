# Databricks notebook source
# Leitura do CSV mantendo todas as colunas como string (sem inferência de tipo)
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/supplychain/bronze/rawdata/DataCoSupplyChainDataset.csv")
)

df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# Adiciona metadados técnicos ao DataFrame lido na célula anterior
df_with_meta = (
    df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file_name", col("_metadata.file_name"))
    .withColumn("_source_file_path", col("_metadata.file_path"))
)

df_with_meta.display()

# COMMAND ----------

from pyspark.sql.functions import trim, col

# Normalização dos nomes das colunas: remove espaços, hífens e caracteres especiais,
# substituindo por underscore e convertendo para minúsculas
normalized_cols = []
for c in df_with_meta.columns:
    if c.startswith("_"):
        # Preserva colunas de metadados técnicas
        normalized_cols.append(col(c))
    else:
        new_name = (
            c.strip()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
            .replace("\"", "")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .replace(".", "_")
            .lower()
        )
        normalized_cols.append(trim(col(c)).alias(new_name))

df_normalized = df_with_meta.select(*normalized_cols)

df_normalized.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# Salva o DataFrame com metadados como tabela Delta no schema bronze
df_normalized.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable("supplychain.bronze.dataco_supply_chain")

# Lê e exibe a tabela recém-criada
spark.table("supplychain.bronze.dataco_supply_chain").display()

# COMMAND ----------

spark.sql("""
COMMENT ON TABLE supplychain.bronze.dataco_supply_chain
IS 'Camada Bronze da arquitetura Medalhão, contendo os dados brutos do dataset DataCo Supply Chain preservados conforme a origem, sem transformações de negócio, com metadados técnicos de ingestão para rastreabilidade. Os dados abrangem informações de pedidos, clientes, produtos, categorias, departamentos, vendas, descontos, lucro, métodos e custos de envio, datas de pedido e entrega, além de informações geográficas e operacionais relacionadas à cadeia de suprimentos.'
""")

# COMMAND ----------

spark.sql(""" DESCRIBE TABLE EXTENDED supplychain.bronze.dataco_supply_chain""").display()

# COMMAND ----------

# Verificação de preservação dos dados: compara o CSV original com a tabela Delta bronze

# 1. Lê o CSV original (todas as colunas como string, igual à ingestão)
df_csv = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/supplychain/bronze/rawdata/DataCoSupplyChainDataset.csv")
)

# 2. Lê a tabela Delta bronze
df_delta = spark.table("supplychain.bronze.dataco_supply_chain")

# 3. Compara contagem de linhas
csv_count = df_csv.count()
delta_count = df_delta.count()
print(f"Linhas no CSV original:     {csv_count}")
print(f"Linhas na tabela Delta:    {delta_count}")
print(f"Contagem de linhas confere? {csv_count == delta_count}")
print()

# 4. Compara contagem de colunas (excluindo metadados técnicos da tabela Delta)
csv_cols = set(df_csv.columns)
delta_cols = set([c for c in df_delta.columns if not c.startswith("_")])
print(f"Colunas no CSV original:    {len(csv_cols)}")
print(f"Colunas na tabela Delta:    {len(delta_cols)}")
print()

# 5. Verifica se os nomes das colunas correspondem (após normalização)
# Normaliza os nomes do CSV da mesma forma que a célula de ingestão
def normalize_name(c):
    return (
        c.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("'", "")
        .replace('"', "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
        .replace(".", "_")
        .lower()
    )

csv_normalized = set([normalize_name(c) for c in csv_cols])
print("Colunas do CSV ausentes na tabela Delta:", csv_normalized - delta_cols)
print("Colunas da tabela Delta ausentes no CSV:", delta_cols - csv_normalized)
print()

# 6. Verificação de integridade linha a linha (sample)
# Ordena ambos pelo mesmo conjunto de colunas para comparação determinística
# Usa apenas as colunas de negócio (sem metadados)
sort_cols = sorted(csv_normalized)

from pyspark.sql.functions import col, trim as sql_trim

# Normaliza nomes no CSV para casar com a tabela Delta
df_csv_norm = df_csv.select(*[sql_trim(col(c)).alias(normalize_name(c)) for c in df_csv.columns])

# Seleciona as mesmas colunas da tabela Delta (sem metadados)
df_delta_clean = df_delta.select(*sort_cols)
df_csv_clean = df_csv_norm.select(*sort_cols)

# Compara usando exceptAll (dados que estão em um mas não no outro)
only_in_csv = df_csv_clean.exceptAll(df_delta_clean).count()
only_in_delta = df_delta_clean.exceptAll(df_csv_clean).count()

print(f"Registros presentes no CSV mas não na tabela Delta: {only_in_csv}")
print(f"Registros presentes na tabela Delta mas não no CSV:  {only_in_delta}")
print()

if csv_count == delta_count and only_in_csv == 0 and only_in_delta == 0:
    print("✅ Verificação concluída: os dados foram preservados integralmente.")
else:
    print("⚠️ Verificação concluída: foram encontradas divergências entre o CSV e a tabela Delta.")