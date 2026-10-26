"""
View de Qualidade com Constraints — Camada Silver Validada
============================================================
Cria views a partir de supplychain.silver.dataco_supply_chain aplicando
constraints de qualidade de dados (mesmas regras de vw_data_quality_flags).

Views criadas:
  1. vw_silver_validated   — registros que passam em TODAS as constraints
  2. vw_silver_violations  — registros que falham em ≥1 constraint (com flags)
  3. vw_constraint_summary — resumo de pass/fail por constraint

Tratamento de NULLs (ponto crítico):
  - Constraints de NÃO-NULIDADE usam:  column IS NOT NULL
    → NULL falha a constraint (é uma violação deliberada)
  - Constraints de FAIXA/DOMÍNIO/CONSISTÊNCIA usam:  column IS NULL OR <cond>
    → NULL passa na constraint (não gera violação falsa)
    → NULL não é interpretado como "fora da faixa" ou "domínio inválido"
  - Flags de violação usam:  column IS NOT NULL AND NOT <cond>
    → NULL → false (sem violação) — nunca NULL
    → Isso garante que COUNT(flag = true) não perde registros NULL
  - Colunas com nulos esperados (product_description, order_zipcode)
    NÃO possuem constraints de não-nulidade.
"""

from pyspark.sql.functions import col, when, lit, array, array_compact, size

# ============================================================================
# 1. DEFINE AS CONSTRAINTS
#    Cada constraint: (nome, descrição, condição_pass, condição_fail)
    # condição_pass: TRUE  = dado válido (passa na constraint)
#    condição_fail: TRUE  = dado inválido (viola a constraint)
#    NULL nunca aparece em condição_pass ou condição_fail — sempre é TRUE ou FALSE.
# ============================================================================

constraints = []

# ---------------------------------------------------------------------------
# 1.1. Constraints de não-nulidade (campos obrigatórios)
#      NULL falha a constraint.
#      NÃO inclui product_description (100% nulo) nem order_zipcode (86% nulo).
# ---------------------------------------------------------------------------
nn_cols = [
    # IDs
    "order_item_id", "order_id", "customer_id", "order_customer_id",
    "category_id", "department_id", "order_item_cardprod_id",
    "product_card_id", "product_category_id",
    # Campos de negócio obrigatórios
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

for c in nn_cols:
    constraints.append((
        f"nn_{c}",
        f"{c} não pode ser nulo",
        f"{c} IS NOT NULL",             # pass: NULL falha
        f"({c} IS NULL)",                # fail: NULL é violação
    ))

# ---------------------------------------------------------------------------
# 1.2. Constraints de faixa de valores
#      Padrão NULL-safe: column IS NULL OR <condition>
#      → NULL passa (não gera violação falsa de faixa)
# ---------------------------------------------------------------------------
range_defs = [
    ("days_for_shipping_real",      "days_for_shipping_real >= 0",        "days_for_shipping_real IS NOT NULL AND days_for_shipping_real < 0"),
    ("days_for_shipment_scheduled",  "days_for_shipment_scheduled >= 0",    "days_for_shipment_scheduled IS NOT NULL AND days_for_shipment_scheduled < 0"),
    ("order_item_quantity",          "order_item_quantity > 0",             "order_item_quantity IS NOT NULL AND order_item_quantity <= 0"),
    ("late_delivery_risk",           "late_delivery_risk IN (0, 1)",        "late_delivery_risk IS NOT NULL AND late_delivery_risk NOT IN (0, 1)"),
    ("product_status",              "product_status = 0",                  "product_status IS NOT NULL AND product_status != 0"),
    ("order_item_discount",         "order_item_discount >= 0",           "order_item_discount IS NOT NULL AND order_item_discount < 0"),
    ("order_item_product_price",    "order_item_product_price > 0",       "order_item_product_price IS NOT NULL AND order_item_product_price <= 0"),
    ("product_price",               "product_price > 0",                  "product_price IS NOT NULL AND product_price <= 0"),
    ("sales",                        "sales > 0",                           "sales IS NOT NULL AND sales <= 0"),
    ("order_item_total",            "order_item_total > 0",               "order_item_total IS NOT NULL AND order_item_total <= 0"),
    ("order_item_discount_rate",    "order_item_discount_rate BETWEEN 0 AND 1",
                                     "order_item_discount_rate IS NOT NULL AND (order_item_discount_rate < 0 OR order_item_discount_rate > 1)"),
    ("order_item_profit_ratio",     "order_item_profit_ratio BETWEEN -1 AND 1",
                                     "order_item_profit_ratio IS NOT NULL AND (order_item_profit_ratio < -1 OR order_item_profit_ratio > 1)"),
    ("latitude",                    "latitude BETWEEN -90 AND 90",         "latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)"),
    ("longitude",                   "longitude BETWEEN -180 AND 180",     "longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)"),
]

for col_name, pass_cond, fail_cond in range_defs:
    safe_pass = f"({col_name} IS NULL OR {pass_cond})"
    constraints.append((
        f"range_{col_name}",
        f"{col_name}: {pass_cond}",
        safe_pass,
        fail_cond,
    ))

# ---------------------------------------------------------------------------
# 1.3. Constraints de domínio categórico
#      Padrão NULL-safe: column IS NULL OR column IN (...)
# ---------------------------------------------------------------------------
domain_defs = [
    ("type",             "['DEBIT', 'TRANSFER', 'CASH', 'PAYMENT']"),
    ("delivery_status",  "['Advance shipping', 'Late delivery', 'Shipping on time', 'Shipping canceled']"),
    ("order_status",     "['COMPLETE', 'PENDING', 'CLOSED', 'PENDING_PAYMENT', 'CANCELED', 'PROCESSING', 'SUSPECTED_FRAUD', 'ON_HOLD', 'PAYMENT_REVIEW']"),
    ("customer_segment", "['Consumer', 'Home Office', 'Corporate']"),
    ("shipping_mode",    "['Standard Class', 'First Class', 'Second Class', 'Same Day']"),
    ("market",           "['Pacific Asia', 'USCA', 'Africa', 'Europe', 'LATAM']"),
    ("customer_country", "['Puerto Rico', 'EE. UU.']"),
]

for col_name, values_str in domain_defs:
    in_list = values_str.replace("[", "").replace("]", "")
    safe_pass = f"({col_name} IS NULL OR {col_name} IN ({in_list}))"
    fail_cond = f"({col_name} IS NOT NULL AND {col_name} NOT IN ({in_list}))"
    constraints.append((
        f"domain_{col_name}",
        f"{col_name} IN {values_str}",
        safe_pass,
        fail_cond,
    ))

# ---------------------------------------------------------------------------
# 1.4. Constraints de consistência (regras de negócio entre colunas)
#      Padrão NULL-safe: col1 IS NULL OR col2 IS NULL OR <condition>
# ---------------------------------------------------------------------------
consistency_defs = [
    ("consist_order_date",
     "order_date = DATE(order_date_dateorders)",
     "order_date IS NULL OR order_date_dateorders IS NULL OR order_date = CAST(order_date_dateorders AS DATE)",
     "order_date IS NOT NULL AND order_date_dateorders IS NOT NULL AND order_date != CAST(order_date_dateorders AS DATE)"),
    ("consist_shipping_date",
     "shipping_date = DATE(shipping_date_dateorders)",
     "shipping_date IS NULL OR shipping_date_dateorders IS NULL OR shipping_date = CAST(shipping_date_dateorders AS DATE)",
     "shipping_date IS NOT NULL AND shipping_date_dateorders IS NOT NULL AND shipping_date != CAST(shipping_date_dateorders AS DATE)"),
    ("consist_shipping_delay",
     "shipping_delay_days = scheduled - real",
     "shipping_delay_days IS NULL OR days_for_shipment_scheduled IS NULL OR days_for_shipping_real IS NULL OR shipping_delay_days = (days_for_shipment_scheduled - days_for_shipping_real)",
     "shipping_delay_days IS NOT NULL AND days_for_shipment_scheduled IS NOT NULL AND days_for_shipping_real IS NOT NULL AND shipping_delay_days != (days_for_shipment_scheduled - days_for_shipping_real)"),
    ("consist_customer_id",
     "customer_id = order_customer_id",
     "customer_id IS NULL OR order_customer_id IS NULL OR customer_id = order_customer_id",
     "customer_id IS NOT NULL AND order_customer_id IS NOT NULL AND customer_id != order_customer_id"),
    ("consist_product_card_id",
     "product_card_id = order_item_cardprod_id",
     "product_card_id IS NULL OR order_item_cardprod_id IS NULL OR product_card_id = order_item_cardprod_id",
     "product_card_id IS NOT NULL AND order_item_cardprod_id IS NOT NULL AND product_card_id != order_item_cardprod_id"),
    ("consist_category_id",
     "product_category_id = category_id",
     "product_category_id IS NULL OR category_id IS NULL OR product_category_id = category_id",
     "product_category_id IS NOT NULL AND category_id IS NOT NULL AND product_category_id != category_id"),
    ("consist_late_delivery",
     "late_delivery_risk=1 → delivery_status='Late delivery'",
     "late_delivery_risk IS NULL OR delivery_status IS NULL OR late_delivery_risk != 1 OR delivery_status = 'Late delivery'",
     "late_delivery_risk IS NOT NULL AND delivery_status IS NOT NULL AND late_delivery_risk = 1 AND delivery_status != 'Late delivery'"),
]

for name, desc, pass_cond, fail_cond in consistency_defs:
    constraints.append((name, desc, pass_cond, fail_cond))

# ============================================================================
# 2. VIEW: vw_silver_validated
#    Registros que passam em TODAS as constraints. Projeta colunas originais.
# ============================================================================

all_pass = "\n    AND ".join([c[2] for c in constraints])

table_name = "supplychain.silver.dataco_supply_chain"
original_cols = [f.name for f in spark.table(table_name).schema.fields]
cols_select = ",\n    ".join(original_cols)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW vw_silver_validated AS
SELECT
    {cols_select}
FROM {table_name}
WHERE
    {all_pass}
""")

# ============================================================================
# 3. VIEW: vw_silver_violations
#    Registros que falham em ≥1 constraint, com flag individual NULL-safe.
#    Cada flag _viol_* é sempre TRUE ou FALSE (nunca NULL):
#      - TRUE  = viola a constraint
#      - FALSE = passou na constraint (incluindo quando o valor é NULL)
# ============================================================================

all_fail = "\n    OR ".join([c[3] for c in constraints])
violation_flags = ",\n    ".join([f"{c[3]} AS _viol_{c[0]}" for c in constraints])

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW vw_silver_violations AS
SELECT
    order_item_id,
    order_id,
    customer_id,
    type,
    delivery_status,
    order_status,
    {violation_flags}
FROM {table_name}
WHERE
    {all_fail}
""")

# ============================================================================
# 4. VIEW: vw_constraint_summary
#    Resumo de pass/fail por constraint (contagens NULL-safe).
# ============================================================================

summary_select = ",\n    ".join([
        f"SUM(CAST({c[3]} AS INT)) AS `{c[0]}`"
        for c in constraints
    ])

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW vw_constraint_summary AS
SELECT
    {summary_select}
FROM {table_name}
""")

# ============================================================================
# 5. EXIBE RESUMO
# ============================================================================

print("=" * 90)
print("VIEWS DE CONSTRAINTS CRIADAS")
print("=" * 90)
print(f"Total de constraints: {len(constraints)}")
print(f"  - Não-nulidade: {sum(1 for c in constraints if c[0].startswith('nn_'))}")
print(f"  - Faixa:        {sum(1 for c in constraints if c[0].startswith('range_'))}")
print(f"  - Domínio:      {sum(1 for c in constraints if c[0].startswith('domain_'))}")
print(f"  - Consistência:{sum(1 for c in constraints if c[0].startswith('consist_'))}")

total_rows = spark.table(table_name).count()
validated_rows = spark.table("vw_silver_validated").count()
violation_rows = spark.table("vw_silver_violations").count()

print("\n" + "=" * 90)
print("RESUMO DE REGISTROS")
print("=" * 90)
print(f"Total de registros:              {total_rows}")
print(f"Registros válidos (pass):        {validated_rows} ({validated_rows / total_rows * 100:.2f}%)")
print(f"Registros com violações (fail):  {violation_rows} ({violation_rows / total_rows * 100:.2f}%)")

print("\n" + "-" * 90)
print(f"{'Constraint':<45} {'Qtd Falhas':>12} {'% Falhas':>10}")
print("-" * 90)

summary = spark.table("vw_constraint_summary").collect()[0].asDict()
for name, desc, _, _ in constraints:
    count = summary.get(name, 0)
    if count and count > 0:
        pct = count / total_rows * 100
        print(f"{name:<45} {count:>12} {pct:>9.2f}%")

print("\n" + "=" * 90)
print("COMO CONSULTAR AS VIEWS")
print("=" * 90)
print("  -- Dados válidos (passaram em todas as constraints):")
print("  SELECT * FROM vw_silver_validated")
print()
print("  -- Registros com violações:")
print("  SELECT order_item_id, _viol_* FROM vw_silver_violations")
print()
print("  -- Resumo de falhas por constraint:")
print("  SELECT * FROM vw_constraint_summary")
print()
print("  -- Registros que violam uma constraint específica:")
print("  SELECT * FROM vw_silver_violations WHERE _viol_range_order_item_profit_ratio = true")