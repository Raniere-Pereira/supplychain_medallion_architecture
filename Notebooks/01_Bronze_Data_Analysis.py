# Databricks notebook source
# Leitura e Análise Exploratória do DataCo Supply Chain Dataset
# Caminho: /Volumes/supplychain/bronze/rawdata/DataCoSupplyChainDataset.csv

# 1. Leitura do arquivo CSV
file_path = "/Volumes/supplychain/bronze/rawdata/DataCoSupplyChainDataset.csv"

df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(file_path))

print("Arquivo lido com sucesso!")
print(f"Número de linhas: {df.count()}")
print(f"Número de colunas: {len(df.columns)}")

# 2. Visualização do schema (tipos de dados inferidos)
print("\n" + "=" * 60)
print("SCHEMA DOS DADOS")
print("=" * 60)
df.printSchema()

# 3. Amostra dos primeiros registros
print("\n" + "=" * 60)
print("AMOSTRA DOS DADOS (5 primeiras linhas)")
print("=" * 60)
display(df.limit(5))

# 4. Resumo estatístico das colunas numéricas
print("\n" + "=" * 60)
print("RESUMO ESTATÍSTICO (colunas numéricas)")
print("=" * 60)
df_summary = df.describe()
display(df_summary)

# 5. Verificação de valores nulos por coluna
print("\n" + "=" * 60)
print("VERIFICAÇÃO DE VALORES NULOS")
print("=" * 60)

from pyspark.sql.functions import col, count, when, isnan, isnull

null_counts = df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    if dict(df.dtypes)[c] in ("int", "double", "float", "long")
    else count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])
display(null_counts)

# 6. Cardinalidade de colunas categóricas
print("\n" + "=" * 60)
print("CARDINALIDADE DE COLUNAS CATEGÓRICAS (string)")
print("=" * 60)

from pyspark.sql.functions import countDistinct

cat_cols = [c for c in df.columns if dict(df.dtypes)[c] == "string"]
cardinality = df.select([countDistinct(col(c)).alias(c) for c in cat_cols])
display(cardinality)

# 7. Valores distintos das principais colunas categóricas (até 20 valores)
print("\n" + "=" * 60)
print("VALORES DISTINTOS DE COLUNAS CATEGÓRICAS RELEVANTES")
print("=" * 60)

for c in cat_cols:
    distinct_vals = df.select(c).distinct().limit(21)
    collected = distinct_vals.collect()
    if len(collected) <= 20:
        print(f"\n{c} ({len(collected)} valores distintos):")
        for row in collected:
            print(f"  - {row[c]}")
    else:
        print(f"\n{c}: mais de 20 valores distintos (alta cardinalidade)")

# 8. Distribuição de tipos de envio e status de pedido
print("\n" + "=" * 60)
print("DISTRIBUIÇÕES RELEVANTES")
print("=" * 60)

# Tenta identificar colunas comuns do dataset DataCo Supply Chain
possible_dist_cols = ["Type", "Delivery Status", "Shipping Mode", "Customer Segment", "Order Status", "Market", "Department Name", "Category Name"]
available_dist_cols = [c for c in possible_dist_cols if c in df.columns]

for c in available_dist_cols:
    print(f"\n--- Distribuição de: {c} ---")
    display(df.groupBy(c).count().orderBy("count", ascending=False))