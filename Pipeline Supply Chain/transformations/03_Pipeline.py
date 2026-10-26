"""
Materialized View de Quarentena - supplychain.silver.dataco_supply_chain
========================================================================
Cria a materialized view supplychain.silver.mv_dataco_quarantine com os
registros que reprovaram em pelo menos uma constraint de qualidade de dados.

IMPORTANTE: Materialized Views exigem SQL Warehouse (nao serverless compute).

A MV inclui:
  - Colunas originais da tabela silver
  - Colunas _viol_* (BOOLEAN, NULL-safe) para cada constraint
  - Coluna _violations (ARRAY<STRING>) com nomes das violacoes
  - Coluna _violation_count (INT) com o total de violacoes
  - Coluna _quarantine_timestamp (TIMESTAMP) com o momento da avaliacao

Comments:
  - Comment na view explicando os dados em quarentena
  - Comment em cada coluna _viol_* explicando o motivo da reprovacao
  - Comment nas colunas originais com violacoes conhecidas

Tratamento de NULLs (NULL-safe):
  - Non-null: (col IS NULL) -> NULL e violacao (TRUE)
  - Range:    (col IS NOT NULL AND <cond>) -> NULL nao e violacao (FALSE)
  - Domain:   (col IS NOT NULL AND NOT IN) -> NULL nao e violacao (FALSE)
  - Consist.: (col1 NOT NULL AND col2 NOT NULL AND <cond>) -> NULL nao e violacao (FALSE)
"""

# ============================================================================
# 1. DEFINE AS CONSTRAINTS
#    (nome, descricao, motivo_reprovacao, condicao_fail)
#    condicao_fail: TRUE = violacao, FALSE = sem violacao (nunca NULL)
# ============================================================================

constraints = []

# 1.1. Constraints de nao-nulidade
nn_cols = [
    "order_item_id", "order_id", "customer_id", "order_customer_id",
    "category_id", "department_id", "order_item_cardprod_id",
    "product_card_id", "product_category_id",
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "late_delivery_risk", "order_item_quantity", "product_status",
    "benefit_per_order", "sales_per_customer", "latitude", "longitude",
    "order_item_discount", "order_item_discount_rate",
    "order_item_product_price", "order_item_profit_ratio",
    "sales", "order_item_total", "order_profit_per_order", "product_price",
    "order_date_dateorders", "order_date", "order_time",
    "shipping_date_dateorders", "shipping_date", "shipping_time",
    "shipping_delay_days",
    "delivery_status", "category_name", "customer_city", "customer_country",
    "customer_email", "customer_fname", "customer_lname", "customer_password",
    "customer_segment", "customer_state", "customer_street", "customer_zipcode",
    "department_name", "market", "order_city", "order_country",
    "order_region", "order_state", "order_status", "product_image",
    "product_name", "shipping_mode",
    "_ingestion_timestamp", "_source_file_name", "_source_file_path",
]
for c in nn_cols:
    constraints.append((
        f"nn_{c}",
        f"{c} nao pode ser nulo",
        f"Reprovacao: {c} e nulo. Constraint de nao-nulidade violada - o campo e obrigatorio.",
        f"({c} IS NULL)",
    ))

# 1.2. Constraints de faixa (NULL-safe: NULL -> false = sem violacao)
range_defs = [
    ("days_for_shipping_real", "days_for_shipping_real >= 0", "days_for_shipping_real IS NOT NULL AND days_for_shipping_real < 0",
     "days_for_shipping_real < 0. O tempo real de envio nao pode ser negativo."),
    ("days_for_shipment_scheduled", "days_for_shipment_scheduled >= 0", "days_for_shipment_scheduled IS NOT NULL AND days_for_shipment_scheduled < 0",
     "days_for_shipment_scheduled < 0. O tempo previsto de envio nao pode ser negativo."),
    ("order_item_quantity", "order_item_quantity > 0", "order_item_quantity IS NOT NULL AND order_item_quantity <= 0",
     "order_item_quantity <= 0. A quantidade do item deve ser maior que zero."),
    ("late_delivery_risk", "late_delivery_risk IN (0, 1)", "late_delivery_risk IS NOT NULL AND late_delivery_risk NOT IN (0, 1)",
     "late_delivery_risk fora de (0, 1). A flag de risco de atraso deve ser binaria."),
    ("product_status", "product_status = 0", "product_status IS NOT NULL AND product_status != 0",
     "product_status != 0. O unico valor valido na origem e 0."),
    ("order_item_discount", "order_item_discount >= 0", "order_item_discount IS NOT NULL AND order_item_discount < 0",
     "order_item_discount < 0. O desconto nao pode ser negativo."),
    ("order_item_product_price", "order_item_product_price > 0", "order_item_product_price IS NOT NULL AND order_item_product_price <= 0",
     "order_item_product_price <= 0. O preco do produto deve ser maior que zero."),
    ("product_price", "product_price > 0", "product_price IS NOT NULL AND product_price <= 0",
     "product_price <= 0. O preco do produto deve ser maior que zero."),
    ("sales", "sales > 0", "sales IS NOT NULL AND sales <= 0",
     "sales <= 0. O valor de vendas deve ser maior que zero."),
    ("order_item_total", "order_item_total > 0", "order_item_total IS NOT NULL AND order_item_total <= 0",
     "order_item_total <= 0. O total do item deve ser maior que zero."),
    ("order_item_discount_rate", "order_item_discount_rate BETWEEN 0 AND 1",
     "order_item_discount_rate IS NOT NULL AND (order_item_discount_rate < 0 OR order_item_discount_rate > 1)",
     "order_item_discount_rate fora de [0, 1]. A taxa de desconto deve estar entre 0% e 100%."),
    ("order_item_profit_ratio", "order_item_profit_ratio BETWEEN -1 AND 1",
     "order_item_profit_ratio IS NOT NULL AND (order_item_profit_ratio < -1 OR order_item_profit_ratio > 1)",
     "order_item_profit_ratio fora de [-1, 1]. A razao de lucro deve estar entre -100% e 100%. VIOLACAO CONHECIDA: 6301 registros."),
    ("latitude", "latitude BETWEEN -90 AND 90", "latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)",
     "latitude fora de [-90, 90]. Coordenada geografica invalida."),
    ("longitude", "longitude BETWEEN -180 AND 180", "longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)",
     "longitude fora de [-180, 180]. Coordenada geografica invalida."),
]
for col_name, pass_cond, fail_cond, reason in range_defs:
    constraints.append((f"range_{col_name}", f"{col_name}: {pass_cond}", f"Reprovacao: {reason}", fail_cond))

# 1.3. Constraints de dominio categorico (NULL-safe)
domain_defs = [
    ("type", "['DEBIT', 'TRANSFER', 'CASH', 'PAYMENT']", "type fora do dominio. Valores: DEBIT, TRANSFER, CASH, PAYMENT."),
    ("delivery_status", "['Advance shipping', 'Late delivery', 'Shipping on time', 'Shipping canceled']",
     "delivery_status fora do dominio. Valores: Advance shipping, Late delivery, Shipping on time, Shipping canceled."),
    ("order_status", "['COMPLETE', 'PENDING', 'CLOSED', 'PENDING_PAYMENT', 'CANCELED', 'PROCESSING', 'SUSPECTED_FRAUD', 'ON_HOLD', 'PAYMENT_REVIEW']",
     "order_status fora do dominio. Valores: COMPLETE, PENDING, CLOSED, PENDING_PAYMENT, CANCELED, PROCESSING, SUSPECTED_FRAUD, ON_HOLD, PAYMENT_REVIEW."),
    ("customer_segment", "['Consumer', 'Home Office', 'Corporate']",
     "customer_segment fora do dominio. Valores: Consumer, Home Office, Corporate."),
    ("shipping_mode", "['Standard Class', 'First Class', 'Second Class', 'Same Day']",
     "shipping_mode fora do dominio. Valores: Standard Class, First Class, Second Class, Same Day."),
    ("market", "['Pacific Asia', 'USCA', 'Africa', 'Europe', 'LATAM']",
     "market fora do dominio. Valores: Pacific Asia, USCA, Africa, Europe, LATAM."),
    ("customer_country", "['Puerto Rico', 'EE. UU.']",
     "customer_country fora do dominio. Valores: Puerto Rico, EE. UU."),
]
for col_name, values_str, reason in domain_defs:
    in_list = values_str.replace("[", "").replace("]", "")
    fail_cond = f"({col_name} IS NOT NULL AND {col_name} NOT IN ({in_list}))"
    constraints.append((f"domain_{col_name}", f"{col_name} IN {values_str}", f"Reprovacao: {reason}", fail_cond))

# 1.4. Constraints de consistencia (NULL-safe)
consistency_defs = [
    ("consist_order_date", "order_date = DATE(order_date_dateorders)",
     "order_date diverge de DATE(order_date_dateorders). Coluna derivada inconsistente.",
     "order_date IS NOT NULL AND order_date_dateorders IS NOT NULL AND order_date != CAST(order_date_dateorders AS DATE)"),
    ("consist_shipping_date", "shipping_date = DATE(shipping_date_dateorders)",
     "shipping_date diverge de DATE(shipping_date_dateorders). Coluna derivada inconsistente.",
     "shipping_date IS NOT NULL AND shipping_date_dateorders IS NOT NULL AND shipping_date != CAST(shipping_date_dateorders AS DATE)"),
    ("consist_shipping_delay", "shipping_delay_days = scheduled - real",
     "shipping_delay_days diverge de (scheduled - real). Coluna derivada inconsistente.",
     "shipping_delay_days IS NOT NULL AND days_for_shipment_scheduled IS NOT NULL AND days_for_shipping_real IS NOT NULL AND shipping_delay_days != (days_for_shipment_scheduled - days_for_shipping_real)"),
    ("consist_customer_id", "customer_id = order_customer_id",
     "customer_id != order_customer_id. Mesma cardinalidade (20652), devem coincidir (FK).",
     "customer_id IS NOT NULL AND order_customer_id IS NOT NULL AND customer_id != order_customer_id"),
    ("consist_product_card_id", "product_card_id = order_item_cardprod_id",
     "product_card_id != order_item_cardprod_id. Mesma cardinalidade (118), devem coincidir (FK).",
     "product_card_id IS NOT NULL AND order_item_cardprod_id IS NOT NULL AND product_card_id != order_item_cardprod_id"),
    ("consist_category_id", "product_category_id = category_id",
     "product_category_id != category_id. Mesma cardinalidade (51), devem coincidir (FK).",
     "product_category_id IS NOT NULL AND category_id IS NOT NULL AND product_category_id != category_id"),
    ("consist_late_delivery", "late_delivery_risk=1 -> delivery_status='Late delivery'",
     "late_delivery_risk=1 mas delivery_status != 'Late delivery'. Inconsistencia flag vs status.",
     "late_delivery_risk IS NOT NULL AND delivery_status IS NOT NULL AND late_delivery_risk = 1 AND delivery_status != 'Late delivery'"),
]
for name, desc, reason, fail_cond in consistency_defs:
    constraints.append((name, desc, f"Reprovacao: {reason}", fail_cond))

# ============================================================================
# 2. CONSTRUI O SQL DA MATERIALIZED VIEW
# ============================================================================

table_name = "supplychain.silver.dataco_supply_chain"
mv_name = "supplychain.silver.mv_dataco_quarantine"

original_cols = [f.name for f in spark.table(table_name).schema.fields]

# SELECT do subquery: colunas originais + flags _viol_* + _violations
inner_parts = list(original_cols)
for name, _, _, fail_cond in constraints:
    inner_parts.append(f"    {fail_cond} AS _viol_{name}")

# Array _violations via IF() e array_compact (NULL-safe)
violations_expr = "array_compact(array(" + ", ".join([
    f"IF({fail_cond}, '{name}', NULL)"
    for name, _, _, fail_cond in constraints
]) + "))"
inner_parts.append(f"    {violations_expr} AS _violations")

all_fail = "\n    OR ".join([f"({c[3]})" for c in constraints])
inner_clause = ",\n".join(inner_parts)

# ============================================================================
# 3. CRIA A MATERIALIZED VIEW (requer SQL Warehouse, nao serverless)
# ============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {mv_name}
COMMENT 'Tabela de Quarentena - registros de supplychain.silver.dataco_supply_chain que reprovaram em pelo menos uma constraint de qualidade de dados. Contem todos os registros invalidos preservando as colunas originais e adicionando colunas _viol_* (uma por constraint), _violations (ARRAY com nomes das violacoes), _violation_count (total de violacoes) e _quarantine_timestamp (momento da avaliacao). Violacoes conhecidas: null_customer_lname (8 registros), out_of_range_profit_ratio (6301 registros com order_item_profit_ratio fora de [-1, 1]), null_customer_zipcode (3 registros).'
CLUSTER BY (order_item_id)
AS SELECT
    *,
    size(_violations) AS _violation_count,
    current_timestamp() AS _quarantine_timestamp
FROM (
    SELECT
        {inner_clause}
    FROM {table_name}
    WHERE
        {all_fail}
)
""")

# ============================================================================
# 4. COMMENTS NAS COLUNAS
# ============================================================================

for name, desc, reason, _ in constraints:
    safe_reason = reason.replace("'", "")
    spark.sql(f"COMMENT ON COLUMN {mv_name}._viol_{name} IS '{safe_reason}'")

spark.sql(f"COMMENT ON COLUMN {mv_name}._violations IS 'ARRAY<STRING> com os nomes de todas as constraints violadas pelo registro.'")
spark.sql(f"COMMENT ON COLUMN {mv_name}._violation_count IS 'INT com o numero total de constraints violadas pelo registro.'")
spark.sql(f"COMMENT ON COLUMN {mv_name}._quarantine_timestamp IS 'TIMESTAMP com o momento em que o registro foi avaliado e colocado em quarentena.'")

for col_name, comment_text in [
    ("customer_lname", "Sobrenome do cliente. VIOLACAO CONHECIDA: 8 registros com valor nulo (constraint nn_customer_lname)."),
    ("customer_zipcode", "CEP do cliente. VIOLACAO CONHECIDA: 3 registros com valor nulo (constraint nn_customer_zipcode)."),
    ("order_item_profit_ratio", "Razao de lucro do item. VIOLACAO CONHECIDA: 6301 registros com valor fora de [-1, 1]."),
]:
    safe_comment = comment_text.replace("'", "")
    spark.sql(f"COMMENT ON COLUMN {mv_name}.{col_name} IS '{safe_comment}'")

# ============================================================================
# 5. RESUMO
# ============================================================================

print("=" * 90)
print("MATERIALIZED VIEW DE QUARENTENA CRIADA")
print("=" * 90)
print(f"Nome: {mv_name}")
print(f"Source: {table_name}")
print(f"Total de constraints: {len(constraints)}")
print(f"Refresh: TRIGGER ON UPDATE | Cluster: CLUSTER BY (order_item_id)")

total_rows = spark.table(table_name).count()
quarantine_rows = spark.table(mv_name).count()
print(f"\nTotal source: {total_rows} | Quarentena: {quarantine_rows} ({quarantine_rows / total_rows * 100:.2f}%)")

print("\n" + "-" * 90)
print(f"{'Constraint':<45} {'Qtd Falhas':>12} {'% Falhas':>10}")
print("-" * 90)
for name, desc, reason, fail_cond in constraints:
    count = spark.table(table_name).filter(fail_cond).count()
    if count > 0:
        print(f"{name:<45} {count:>12} {count / total_rows * 100:>9.2f}%")

print("\n" + "=" * 90)
print("COMO CONSULTAR")
print("=" * 90)
print(f"  SELECT order_item_id, _violations, _violation_count FROM {mv_name}")
print(f"  SELECT violation, count(*) FROM {mv_name} LATERAL VIEW explode(_violations) t AS violation GROUP BY violation")
print(f"  DESCRIBE {mv_name}")