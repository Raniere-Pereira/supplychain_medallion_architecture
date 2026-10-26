# Databricks notebook source
from pyspark.sql import functions as F

# Ajuste se necessário: catálogo e schemas onde ficam as camadas
CATALOG = "supplychain"
SCHEMAS = ["bronze", "silver", "gold"]

# 1. Lista todas as tabelas por schema e classifica por camada (bronze/silver/gold)
tables = []
for schema in SCHEMAS:
    rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    tables.extend((schema, r["tableName"]) for r in rows)

def layer_of(schema: str):
    return schema if schema in SCHEMAS else None

def base_name(name: str) -> str:
    n = name.lower()
    for frag in ("bronze_", "silver_", "gold_", "_bronze", "_silver", "_gold"):
        n = n.replace(frag, "")
    return n

# 2. Conta linhas por tabela
layers_counts = {}
for schema, t in tables:
    layer = layer_of(schema)
    if layer is None:
        continue
    cnt = spark.table(f"{CATALOG}.{schema}.{t}").count()
    layers_counts.setdefault(base_name(t), {})[layer] = (t, cnt)

# 3. Compara as camadas e sinaliza possíveis perdas
print(f"{'tabela (base)':<35}{'bronze':>14}{'silver':>14}{'gold':>14}  status")
print("-" * 100)

possiveis_perdas = []
for base, layers in sorted(layers_counts.items()):
    bronze_cnt = layers.get("bronze", (None, None))[1]
    silver_cnt = layers.get("silver", (None, None))[1]
    gold_cnt = layers.get("gold", (None, None))[1]

    # Regra: linhas não podem diminuir entre bronze -> silver.
    # Na gold, menos linhas PODE ser legítimo (agregações/dimensões).
    if bronze_cnt is not None and silver_cnt is not None and silver_cnt < bronze_cnt:
        status = "⚠ POSSÍVEL PERDA bronze->silver"
        possiveis_perdas.append((base, "bronze->silver", bronze_cnt, silver_cnt))
    elif silver_cnt is not None and gold_cnt is not None and gold_cnt < silver_cnt:
        status = "⚠ revisar (gold pode ser agregação)"
    else:
        status = "✓ OK"

    fmt = lambda v: f"{v:,}" if v is not None else "-"
    print(f"{base:<35}{fmt(bronze_cnt):>14}{fmt(silver_cnt):>14}{fmt(gold_cnt):>14}  {status}")

# 4. Resumo
print("\n" + "=" * 100)
if possiveis_perdas:
    print(f"{len(possiveis_perdas)} tabela(s) com possível perda de registros entre bronze e silver:")
    for base, etapa, antes, depois in possiveis_perdas:
        print(f"  - {base}: {antes:,} -> {depois:,} ({depois - antes:,} registros)")
else:
    print("Nenhuma perda de volume de registros detectada entre bronze -> silver.")

# 5. Verificação extra: nulos e duplicatas em colunas-chave comuns (id/primary key, se existirem)
print("\nVerificação de duplicatas por colunas-chave comuns (id, *_id, chave, key):")
for schema, t in tables:
    layer = layer_of(schema)
    if layer is None:
        continue
    df = spark.table(f"{CATALOG}.{schema}.{t}")
    key_cols = [c for c in df.columns if c.lower() in ("id", "key", "chave", "primary_key") or c.lower().endswith("_id") or c.lower().endswith("_key")]
    if not key_cols:
        continue
    key = key_cols[0]
    total = df.count()
    distintos = df.select(key).dropna().distinct().count()
    nulos = df.where(F.col(key).isNull()).count()
    if total > distintos:
        print(f"  ⚠ {t}: {total - distintos:,} duplicatas em '{key}' (total={total:,}, distintos={distintos:,}, nulos em '{key}'={nulos:,})")
print("\nValidação concluída. Ajuste CATALOG se as tabelas estiverem em outro schema.")