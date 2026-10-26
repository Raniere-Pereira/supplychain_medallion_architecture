# Databricks notebook source
# Verifica valores nulos em cada coluna de supplychain.silver.dataco_supply_chain
# Mostra apenas as colunas que possuem nulos e compara com o total de linhas da tabela
from pyspark.sql.functions import col, sum as _sum, lit

df = spark.table("supplychain.silver.dataco_supply_chain")

total_rows = df.count()

null_counts = df.select([
    _sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
]).collect()[0].asDict()

# Filtra apenas as colunas que possuem pelo menos um valor nulo
columns_with_nulls = [
    (c, null_counts[c]) for c in df.columns if null_counts[c] > 0
]

# Exibe o total de linhas e as colunas com nulos
print(f"Total de linhas na tabela: {total_rows}")
print(f"Colunas com valores nulos: {len(columns_with_nulls)}")
print("-" * 80)
print(f"{'Coluna':<40} {'Nulos':>10} {'% Nulos':>10}")
print("-" * 80)
for col_name, null_count in sorted(columns_with_nulls, key=lambda x: -x[1]):
    pct = (null_count / total_rows) * 100 if total_rows else 0
    print(f"{col_name:<40} {null_count:>10} {pct:>9.2f}%")

# Também exibe como DataFrame para visualização tabular
from pyspark.sql.types import StructType, StructField, StringType, LongType

if columns_with_nulls:
    result_df = spark.createDataFrame(
        [(c, n, (n / total_rows) * 100 if total_rows else 0.0) for c, n in columns_with_nulls],
        schema=StructType([
            StructField("coluna", StringType(), False),
            StructField("valores_nulos", LongType(), False),
            StructField("percentual_nulos", StringType(), False),
        ])
    ).withColumn("total_linhas", lit(total_rows))
    display(result_df)
else:
    print("\nNenhuma coluna possui valores nulos.")

# COMMAND ----------

# Verifica colunas com dados mascarados em supplychain.silver.dataco_supply_chain
# Compara com o total de linhas, seguindo o mesmo formato da análise de nulos
from pyspark.sql.functions import col, sum as _sum, lit, regexp_like

df = spark.table("supplychain.silver.dataco_supply_chain")
total_rows = df.count()

# 1. Verifica se há políticas de mascaramento aplicadas via Unity Catalog (column masking policies)
print("=" * 80)
print("VERIFICAÇÃO DE POLÍTICAS DE MASCARAMENTO (Unity Catalog)")
print("=" * 80)

try:
    describe_df = spark.sql("DESCRIBE TABLE EXTENDED supplychain.silver.dataco_supply_chain")
    # Filtra linhas que mencionam mascaramento/masking na coluna de metadados
    describe_rows = describe_df.collect()
    masking_found = False
    for row in describe_rows:
        info = str(row.asDict()) if hasattr(row, 'asDict') else str(row)
        if any(kw in info.lower() for kw in ['mask', 'redact', 'masking']):
            print(f"  -> {info}")
            masking_found = True
    if not masking_found:
        print("  Nenhuma política de mascaramento explícita encontrada via DESCRIBE TABLE.")
except Exception as e:
    print(f"  Não foi possível obter metadados de mascaramento: {e}")

print()

# 2. Verifica valores que aparentam estar mascarados nas colunas de tipo string
# Padrões comuns de mascaramento: ***, XXXX, ####, ****, etc.
print("=" * 80)
print("VERIFICAÇÃO DE VALORES COM APARÊNCIA DE MASCARAMENTO NOS DADOS")
print("=" * 80)

string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == 'string']

if not string_cols:
    print("  Nenhuma coluna de string encontrada para verificação.")
else:
    # Padrões de mascaramento: sequências de *, X, #, ou combinações
    masked_pattern = r"^[\*Xx#\-]{2,}$|\*{2,}|X{2,}|#{2,}"

    masked_counts = df.select([
        _sum(regexp_like(col(c), lit(masked_pattern)).cast("int")).alias(c) for c in string_cols
    ]).collect()[0].asDict()

    # Filtra apenas as colunas que possuem pelo menos um valor mascarado
    columns_with_masked = [
        (c, masked_counts[c]) for c in string_cols if (masked_counts[c] or 0) > 0
    ]

    print(f"Total de linhas na tabela: {total_rows}")
    print(f"Colunas com valores possivelmente mascarados: {len(columns_with_masked)}")
    print("-" * 80)
    print(f"{'Coluna':<40} {'Mascarados':>12} {'% Mascarados':>14}")
    print("-" * 80)

    if columns_with_masked:
        for col_name, masked_count in sorted(columns_with_masked, key=lambda x: -x[1]):
            pct = (masked_count / total_rows) * 100 if total_rows else 0
            print(f"{col_name:<40} {masked_count:>12} {pct:>13.2f}%")
    else:
        print("  Nenhuma coluna possui valores com aparência de mascaramento.")

    print()
    print("=" * 80)
    print("AMOSTRA DE VALORES POSSIVELMENTE MASCARADOS POR COLUNA")
    print("=" * 80)

    for col_name, _ in sorted(columns_with_masked, key=lambda x: -x[1]):
        print(f"\n--- {col_name} ---")
        sample_df = df.filter(regexp_like(col(col_name), lit(masked_pattern))) \
            .select(col(col_name).alias(col_name)) \
            .distinct() \
            .limit(10)
        display(sample_df)

# 3. Exibe resultado tabular consolidado
from pyspark.sql.types import StructType, StructField, StringType, LongType

if 'columns_with_masked' in locals() and columns_with_masked:
    result_df = spark.createDataFrame(
        [(c, n, (n / total_rows) * 100 if total_rows else 0.0) for c, n in columns_with_masked],
        schema=StructType([
            StructField("coluna", StringType(), False),
            StructField("valores_mascarados", LongType(), False),
            StructField("percentual_mascarados", StringType(), False),
        ])
    ).withColumn("total_linhas", lit(total_rows))
    print("\n" + "=" * 80)
    print("RESUMO TABULAR")
    print("=" * 80)
    display(result_df)
else:
    print("\nNenhuma coluna possui valores mascarados.")

# COMMAND ----------

# Verifica valores nulos em colunas que terminam com '_id' na tabela supplychain.silver.dataco_supply_chain
from pyspark.sql.functions import col, sum as _sum, lit

df = spark.table("supplychain.silver.dataco_supply_chain")
total_rows = df.count()

# Seleciona apenas as colunas cujo nome termina com '_id'
id_cols = [f.name for f in df.schema.fields if f.name.lower().endswith("_id")]

if not id_cols:
    print("Nenhuma coluna terminada em '_id' foi encontrada na tabela.")
else:
    null_counts = df.select([
        _sum(col(c).isNull().cast("int")).alias(c) for c in id_cols
    ]).collect()[0].asDict()

    columns_with_nulls = [
        (c, null_counts[c]) for c in id_cols if null_counts[c] > 0
    ]

    print(f"Total de linhas na tabela: {total_rows}")
    print(f"Colunas terminadas em '_id' encontradas: {len(id_cols)} -> {id_cols}")
    print(f"Colunas com valores nulos: {len(columns_with_nulls)}")
    print("-" * 80)
    print(f"{'Coluna':<40} {'Nulos':>10} {'% Nulos':>10}")
    print("-" * 80)
    for col_name, null_count in sorted(columns_with_nulls, key=lambda x: -x[1]):
        pct = (null_count / total_rows) * 100 if total_rows else 0
        print(f"{col_name:<40} {null_count:>10} {pct:>9.2f}%")

    if not columns_with_nulls:
        print("\nNenhuma coluna '_id' possui valores nulos.")

    # Exibe como DataFrame para visualização tabular
    from pyspark.sql.types import StructType, StructField, StringType, LongType

    result_df = spark.createDataFrame(
        [(c, n, (n / total_rows) * 100 if total_rows else 0.0) for c, n in columns_with_nulls],
        schema=StructType([
            StructField("coluna", StringType(), False),
            StructField("valores_nulos", LongType(), False),
            StructField("percentual_nulos", StringType(), False),
        ]
    )).withColumn("total_linhas", lit(total_rows))
    display(result_df)

# COMMAND ----------

# Teste de unicidade das colunas terminadas em '_id' na tabela supplychain.silver.dataco_supply_chain
from pyspark.sql.functions import col, count as _count, countDistinct, lit

spark.conf.set("spark.sql.shuffle.partitions", "200")

df = spark.table("supplychain.silver.dataco_supply_chain")
total_rows = df.count()

# Seleciona apenas as colunas cujo nome termina com '_id'
id_cols = [f.name for f in df.schema.fields if f.name.lower().endswith("_id")]

if not id_cols:
    print("Nenhuma coluna terminada em '_id' foi encontrada na tabela.")
else:
    print(f"Total de linhas na tabela: {total_rows}")
    print(f"Colunas terminadas em '_id' encontradas: {len(id_cols)} -> {id_cols}")
    print("=" * 90)
    print(f"{'Coluna':<40} {'Total':>10} {'Distintos':>12} {'Duplicados':>12} {'% Unico':>10}")
    print("=" * 90)

    results = []
    for c in id_cols:
        # Conta valores não nulos, distintos e duplicados em uma única passagem
        stats = df.agg(
            _count(col(c)).alias("non_null"),
            countDistinct(col(c)).alias("distinct"),
        ).collect()[0]

        non_null = stats["non_null"]
        distinct = stats["distinct"]
        duplicated = non_null - distinct
        pct_unique = (distinct / non_null * 100) if non_null else 0.0
        is_unique = duplicated == 0 and non_null == total_rows

        results.append((c, non_null, distinct, duplicated, pct_unique, is_unique))

    for c, non_null, distinct, duplicated, pct_unique, is_unique in results:
        flag = "SIM" if is_unique else "NAO"
        print(f"{c:<40} {non_null:>10} {distinct:>12} {duplicated:>12} {pct_unique:>9.2f}%  [{flag}]")

    # Exibe como DataFrame para visualização tabular
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

    result_df = spark.createDataFrame(
        [(c, nn, d, dup, pu, u) for c, nn, d, dup, pu, u in results],
        schema=StructType([
            StructField("coluna", StringType(), False),
            StructField("valores_nao_nulos", LongType(), False),
            StructField("valores_distintos", LongType(), False),
            StructField("valores_duplicados", LongType(), False),
            StructField("percentual_unicos", DoubleType(), False),
            StructField("e_unica", BooleanType(), False),
        ]),
    ).withColumn("total_linhas", lit(total_rows))

    print("\n" + "=" * 90)
    print("RESUMO TABULAR")
    print("=" * 90)
    display(result_df)

    # Amostra de duplicados por coluna (quando houver)
    cols_with_dups = [r for r in results if r[3] > 0]
    if cols_with_dups:
        print("\n" + "=" * 90)
        print("AMOSTRA DE VALORES DUPLICADOS POR COLUNA")
        print("=" * 90)
        for c, _, _, _, _, _ in cols_with_dups:
            print(f"\n--- {c} ---")
            dup_sample = (
                df.groupBy(c)
                .agg(_count(lit(1)).alias("ocorrencias"))
                .filter(col("ocorrencias") > 1)
                .orderBy(col("ocorrencias").desc())
                .limit(10)
            )
            display(dup_sample)
    else:
        print("\nTodas as colunas '_id' possuem valores unicos.")
