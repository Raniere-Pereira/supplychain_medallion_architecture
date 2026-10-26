"""
View Temporária de Qualidade de Dados — supplychain.silver.dataco_supply_chain
============================================================================
Cria a view temporária vw_data_quality_flags que marca registros com:
  1. Valores nulos em colunas que não deveriam ser nulas
  2. Violações de tipagem/faixa (valores fora do domínio esperado para o tipo)
  3. Violações de regras de negócio (consistência entre colunas, domínios categóricos)

Baseado nos testes de unicidade e cardinalidade do notebook 04_Silver_Verification.

Resultado:
  - Colunas originais da tabela preservadas
  - Colunas _flag_* (BOOLEAN) para cada verificação individual
  - Coluna _violations (ARRAY<STRING>) com a lista de violações do registro
  - Coluna _has_violations (BOOLEAN) indicando se há alguma violação
  - Coluna _violation_count (INT) com o número total de violações
"""

from pyspark.sql.functions import col, when, lit, array, array_compact, size, date_format

# ============================================================================
# 1. CARREGA A TABELA SILVER
# ============================================================================
df = spark.table("supplychain.silver.dataco_supply_chain")

# ============================================================================
# 2. DEFINE AS VERIFICAÇÕES
#    Cada verificação é uma tupla: (flag_name, condition, description, is_violation)
#    is_violation=True  → incluída no array _violations
#    is_violation=False → apenas flag informativo (não conta como violação)
# ============================================================================

checks = []

# ---------------------------------------------------------------------------
# 2.1. Valores nulos esperados (informativo, não é violação)
#      product_description: 100% nulo na origem
#      order_zipcode: 86% nulo (nem todos pedidos possuem CEP)
# ---------------------------------------------------------------------------
expected_null_cols = ["product_description", "order_zipcode"]
for c in expected_null_cols:
    checks.append((f"_flag_null_{c}", col(c).isNull(), f"null_{c}", False))

# ---------------------------------------------------------------------------
# 2.2. Valores nulos não esperados (violação de qualidade)
#      Conforme verificação: customer_lname (8 nulos) e customer_zipcode (3 nulos)
#      demais colunas não possuem nulos hoje, mas a verificação protege contra
#      futuros problemas de ingestão.
# ---------------------------------------------------------------------------
not_null_cols = [
    # IDs (9 colunas — nenhuma deve ser nula)
    "category_id", "customer_id", "order_customer_id", "department_id",
    "order_id", "order_item_cardprod_id", "order_item_id",
    "product_card_id", "product_category_id",
    # Campos numéricos obrigatórios
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "late_delivery_risk", "order_item_quantity", "product_status",
    "benefit_per_order", "sales_per_customer", "latitude", "longitude",
    "order_item_discount", "order_item_discount_rate",
    "order_item_product_price", "order_item_profit_ratio",
    "sales", "order_item_total", "order_profit_per_order", "product_price",
    # Datas e tempos derivados
    "order_date_dateorders", "order_date", "order_time",
    "shipping_date_dateorders", "shipping_date", "shipping_time",
    "shipping_delay_days",
    # Strings categóricos e de endereço
    "delivery_status", "category_name", "customer_city", "customer_country",
    "customer_email", "customer_fname", "customer_lname", "customer_password",
    "customer_segment", "customer_state", "customer_street", "customer_zipcode",
    "department_name", "market", "order_city", "order_country",
    "order_region", "order_state", "order_status", "product_image",
    "product_name", "shipping_mode",
    # Metadados de ingestão
    "_ingestion_timestamp", "_source_file_name", "_source_file_path",
]
for c in not_null_cols:
    checks.append((f"_flag_null_{c}", col(c).isNull(), f"null_{c}", True))

# ---------------------------------------------------------------------------
# 2.3. Verificações de tipagem / faixa de valores
#      Valores que, embora do tipo correto, violam restrições de domínio.
# ---------------------------------------------------------------------------
range_checks = [
    (col("days_for_shipping_real") < 0,     "negative_days_for_shipping_real"),
    (col("days_for_shipment_scheduled") < 0, "negative_days_for_shipment_scheduled"),
    (col("order_item_quantity") <= 0,       "nonpositive_order_item_quantity"),
    (~col("late_delivery_risk").isin(0, 1),  "invalid_late_delivery_risk"),
    (col("product_status") != 0,            "invalid_product_status"),
    (col("order_item_discount") < 0,        "negative_order_item_discount"),
    (col("order_item_product_price") <= 0,  "nonpositive_order_item_product_price"),
    (col("product_price") <= 0,             "nonpositive_product_price"),
    (col("sales") <= 0,                     "nonpositive_sales"),
    (col("order_item_total") <= 0,          "nonpositive_order_item_total"),
    ((col("order_item_discount_rate") < 0) | (col("order_item_discount_rate") > 1), "out_of_range_discount_rate"),
    ((col("order_item_profit_ratio") < -1) | (col("order_item_profit_ratio") > 1),   "out_of_range_profit_ratio"),
    ((col("latitude") < -90) | (col("latitude") > 90),     "invalid_latitude"),
    ((col("longitude") < -180) | (col("longitude") > 180),  "invalid_longitude"),
]
for condition, desc in range_checks:
    checks.append((f"_flag_{desc}", condition, desc, True))

# ---------------------------------------------------------------------------
# 2.4. Verificações de domínios categóricos
#      Valores válidos conhecidos conforme verificação de cardinalidade.
# ---------------------------------------------------------------------------
categorical_domains = {
    "type":             ["DEBIT", "TRANSFER", "CASH", "PAYMENT"],
    "delivery_status":  ["Advance shipping", "Late delivery", "Shipping on time", "Shipping canceled"],
    "order_status":     ["COMPLETE", "PENDING", "CLOSED", "PENDING_PAYMENT",
                         "CANCELED", "PROCESSING", "SUSPECTED_FRAUD", "ON_HOLD", "PAYMENT_REVIEW"],
    "customer_segment": ["Consumer", "Home Office", "Corporate"],
    "shipping_mode":    ["Standard Class", "First Class", "Second Class", "Same Day"],
    "market":           ["Pacific Asia", "USCA", "Africa", "Europe", "LATAM"],
    "customer_country": ["Puerto Rico", "EE. UU."],
}
for col_name, valid_values in categorical_domains.items():
    condition = col(col_name).isNotNull() & ~col(col_name).isin(valid_values)
    checks.append((f"_flag_invalid_{col_name}", condition, f"invalid_{col_name}", True))

# ---------------------------------------------------------------------------
# 2.5. Verificações de consistência (regras de negócio entre colunas)
#      Colunas derivadas devem ser consistentes com suas originais.
#      Relacionamentos de mesma cardinalidade (FK) devem coincidir.
# ---------------------------------------------------------------------------
consistency_checks = [
    # Colunas derivadas de datas
    (col("order_date") != col("order_date_dateorders").cast("date"),
     "inconsistent_order_date"),
    (col("shipping_date") != col("shipping_date_dateorders").cast("date"),
     "inconsistent_shipping_date"),
    (col("order_time") != date_format(col("order_date_dateorders"), "HH:mm:ss"),
     "inconsistent_order_time"),
    (col("shipping_time") != date_format(col("shipping_date_dateorders"), "HH:mm:ss"),
     "inconsistent_shipping_time"),
    # Coluna derivada: shipping_delay_days = scheduled - real
    (col("shipping_delay_days") != (col("days_for_shipment_scheduled") - col("days_for_shipping_real")),
     "inconsistent_shipping_delay_days"),
    # Relacionamentos de mesma cardinalidade (verificação de FK)
    # customer_id = order_customer_id (cardinalidade 20652)
    (col("customer_id") != col("order_customer_id"),
     "customer_id_mismatch"),
    # product_card_id = order_item_cardprod_id (cardinalidade 118)
    (col("product_card_id") != col("order_item_cardprod_id"),
     "product_card_id_mismatch"),
    # product_category_id = category_id (cardinalidade 51)
    (col("product_category_id") != col("category_id"),
     "category_id_mismatch"),
    # Regra: late_delivery_risk=1 implica delivery_status='Late delivery'
    ((col("late_delivery_risk") == 1) & (col("delivery_status") != "Late delivery"),
     "late_delivery_risk_mismatch"),
]
for condition, desc in consistency_checks:
    checks.append((f"_flag_{desc}", condition, desc, True))

# ---------------------------------------------------------------------------
# 2.6. Flags informativos (dados mascarados — não são violações)
#      customer_email e customer_password são 100% mascarados (XXXXXXXXX)
#      na origem. Marcados para auditoria, mas não contam como violação.
# ---------------------------------------------------------------------------
checks.append(("_flag_masked_customer_email",    col("customer_email") == "XXXXXXXXX",    "masked_customer_email",    False))
checks.append(("_flag_masked_customer_password", col("customer_password") == "XXXXXXXXX", "masked_customer_password", False))

# ============================================================================
# 3. APLICA AS VERIFICAÇÕES E CRIA A VIEW TEMPORÁRIA
# ============================================================================

# Adiciona todas as colunas de flag
for flag_name, condition, _, _ in checks:
    df = df.withColumn(flag_name, condition)

# Constrói o array _violations (apenas verificações marcadas como is_violation=True)
df = df.withColumn(
    "_violations",
    array_compact(array(*[
        when(condition, lit(desc)).otherwise(lit(None))
        for _, condition, desc, is_violation in checks
        if is_violation
    ])),
)

df = df.withColumn("_has_violations", size(col("_violations")) > 0)
df = df.withColumn("_violation_count", size(col("_violations")))

# Cria a view temporária
df.createOrReplaceTempView("vw_data_quality_flags")

# ============================================================================
# 4. EXIBE RESUMO DAS VERIFICAÇÕES
# ============================================================================

print("=" * 90)
print("VIEW TEMPORÁRIA CRIADA: vw_data_quality_flags")
print("=" * 90)
n_violations = sum(1 for _, _, _, v in checks if v)
n_info = sum(1 for _, _, _, v in checks if not v)
print(f"Total de verificações: {len(checks)} ({n_violations} violações + {n_info} informativos)")

print("\n" + "=" * 90)
print("RESUMO DE VIOLAÇÕES")
print("=" * 90)

total_rows = df.count()
violation_rows = df.filter(col("_has_violations")).count()
print(f"Total de registros:                 {total_rows}")
print(f"Registros com violações:            {violation_rows} ({violation_rows / total_rows * 100:.2f}%)")
print(f"Registros sem violações:            {total_rows - violation_rows} ({(total_rows - violation_rows) / total_rows * 100:.2f}%)")

print("\n" + "-" * 90)
print(f"{'Tipo':<5} {'Verificação':<50} {'Qtd':>10} {'%':>10}")
print("-" * 90)

for flag_name, condition, desc, is_violation in checks:
    count = df.filter(col(flag_name)).count()
    if count > 0:
        pct = count / total_rows * 100
        marker = "VIOL" if is_violation else "INFO"
        print(f"{marker:<5} {desc:<50} {count:>10} {pct:>9.2f}%")

print("\n" + "=" * 90)
print("COMO CONSULTAR A VIEW")
print("=" * 90)
print("  -- Todos os registros com violações:")
print("  SELECT order_item_id, _violations, _violation_count")
print("  FROM vw_data_quality_flags WHERE _has_violations = true")
print()
print("  -- Contagem de registros por tipo de violação:")
print("  SELECT violation, count(*) as qtd")
print("  FROM vw_data_quality_flags")
print("  LATERAL VIEW explode(_violations) t AS violation")
print("  GROUP BY violation ORDER BY qtd DESC")
print()
print("  -- Apenas registros sem violações (dados válidos):")
print("  SELECT * FROM vw_data_quality_flags WHERE _has_violations = false")