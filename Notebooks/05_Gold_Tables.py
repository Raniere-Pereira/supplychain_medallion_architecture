# Databricks notebook source
# DBTITLE 1,Título e descrição
# MAGIC %md
# MAGIC # Camada Gold — Supply Chain (DataCo)
# MAGIC
# MAGIC Cria as tabelas da camada **gold** (esquema `supplychain.gold`) a partir de `supplychain.silver.dataco_supply_chain`, modeladas em **esquema em estrela**:
# MAGIC
# MAGIC * `dim_customers` — dimensão de clientes (1 linha por `customer_id`)
# MAGIC * `dim_products` — dimensão de produtos com categoria e departamento (1 linha por `product_card_id`)
# MAGIC * `dim_markets` — dimensão de mercados/destinos do pedido (mercado → região → país → estado → cidade)
# MAGIC * `fact_sales` — tabela de fatos no grão de item de pedido (`order_item_id`), clusterizada por data

# COMMAND ----------

# DBTITLE 1,1. Schema gold + visão geral da silver
# ============================================================================
# 1. CRIA O SCHEMA GOLD E MOSTRA A VISAO GERAL DA SILVER
# ============================================================================
spark.sql("CREATE SCHEMA IF NOT EXISTS supplychain.gold")

silver_df = spark.table("supplychain.silver.dataco_supply_chain")
print(f"Total de registros na silver: {silver_df.count():,}")
display(silver_df.select("order_item_id", "order_id", "customer_id", "product_card_id", "order_date", "sales", "order_item_quantity").limit(5))

# COMMAND ----------

# DBTITLE 1,2. dim_customers
# ============================================================================
# 2. DIM_CLIENTES
#    1 linha por customer_id (chave natural). Atributos de endereco e segmento.
#    Nota: customer_password e customer_street ficam de fora da gold (sensiveis).
#    min/max(order_date) enriquecem a dimensao com recencia do cliente.
# ============================================================================
spark.sql("""
    CREATE OR REPLACE TABLE supplychain.gold.dim_customers
    COMMENT 'Dimensao de clientes (1 linha por customer_id). Origem: supplychain.silver.dataco_supply_chain. Senha e endereco completo excluidos por seguranca.'
    AS
    SELECT
        customer_id,
        first(customer_fname IGNORE NULLS)  AS first_name,
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
    FROM supplychain.silver.dataco_supply_chain
    GROUP BY customer_id
""")

display(spark.sql("SELECT COUNT(*) AS total_clientes FROM supplychain.gold.dim_customers"))

# COMMAND ----------

# DBTITLE 1,3. dim_products
# ============================================================================
# 3. DIM_PRODUCTS
#    1 linha por product_card_id, ja trazendo categoria e departamento
#    (denormalizados na dimensao, tipico de star schema).
# ============================================================================
spark.sql("""
    CREATE OR REPLACE TABLE supplychain.gold.dim_products
    COMMENT 'Dimensao de produtos (1 linha por product_card_id) com categoria e departamento. Origem: supplychain.silver.dataco_supply_chain.'
    AS
    SELECT
        product_card_id,
        first(product_name IGNORE NULLS)        AS product_name,
        first(product_description IGNORE NULLS) AS product_description,
        first(product_image IGNORE NULLS)       AS product_image,
        first(product_price IGNORE NULLS)        AS list_price,
        first(category_id IGNORE NULLS)          AS category_id,
        first(category_name IGNORE NULLS)        AS category_name,
        first(department_id IGNORE NULLS)        AS department_id,
        first(department_name IGNORE NULLS)      AS department_name
    FROM supplychain.silver.dataco_supply_chain
    GROUP BY product_card_id
""")

display(spark.sql("SELECT COUNT(*) AS total_produtos FROM supplychain.gold.dim_products"))

# COMMAND ----------

# DBTITLE 1,4. dim_markets
# ============================================================================
# 4. DIM_MARKETS
#    Hierarquia geografica do pedido: mercado > regiao > pais > estado > cidade.
#    market_key = md5 da hierarquia completa (chave substituta deterministica).
# ============================================================================
spark.sql("""
    CREATE OR REPLACE TABLE supplychain.gold.dim_markets
    COMMENT 'Dimensao de mercados/destinos do pedido (mercado > regiao > pais > estado > cidade). Origem: supplychain.silver.dataco_supply_chain.'
    AS
    SELECT DISTINCT
        md5(concat_ws('|', market, order_region, order_country, order_state, order_city)) AS market_key,
        market,
        order_region  AS region,
        order_country AS country,
        order_state   AS state,
        order_city    AS city
    FROM supplychain.silver.dataco_supply_chain
""")

display(spark.sql("SELECT COUNT(*) AS total_mercados FROM supplychain.gold.dim_markets"))

# COMMAND ----------

# DBTITLE 1,5. fact_sales
# ============================================================================
# 5. FACT_SALES
#    Tabela de fatos no grao mais fino disponivel: item de pedido (order_item_id).
#    Chaves: customer_id, product_card_id, market_key (mesma expressao da dim_markets).
#    Medidas: quantidade, precos, descontos, vendas, lucro e logistica de entrega.
#    Clusterizacao liquida por order_date para consultas por periodo.
# ============================================================================
spark.sql("""
    CREATE OR REPLACE TABLE supplychain.gold.fact_sales
    COMMENT 'Fatos de vendas por item de pedido (order_item_id), com medidas comerciais e logisticas. Origem: supplychain.silver.dataco_supply_chain.'
    CLUSTER BY (order_date)
    AS
    SELECT
        -- chaves
        s.order_item_id,
        s.order_id,
        s.customer_id,
        s.product_card_id,
        md5(concat_ws('|', s.market, s.order_region, s.order_country, s.order_state, s.order_city)) AS market_key,
        -- datas
        s.order_date,
        s.order_time,
        s.shipping_date,
        s.shipping_time,
        -- pagamento e status
        s.type               AS payment_type,
        s.order_status,
        s.delivery_status,
        s.shipping_mode,
        s.late_delivery_risk,
        -- medidas comerciais
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
        -- logistica
        s.days_for_shipping_real,
        s.days_for_shipment_scheduled,
        s.shipping_delay_days,
        -- auditoria
        current_timestamp() AS _loaded_at
    FROM supplychain.silver.dataco_supply_chain AS s
""")

print("fact_sales criada.")

# COMMAND ----------

# DBTITLE 1,6. Validação da gold
# ============================================================================
# 6. VALIDACAO DA GOLD
#    - Contagem de linhas de cada tabela
#    - Integridade referencial (chaves da fat nao podem ser nulas e devem
#      existir nas dimensoes)
# ============================================================================
gold_tables = ["dim_customers", "dim_products", "dim_markets", "fact_sales"]
for t in gold_tables:
    n = spark.table(f"supplychain.gold.{t}").count()
    print(f"supplychain.gold.{t}: {n:,} linhas")

integridade = spark.sql("""
    SELECT
        COUNT(*) AS total_fatos,
        SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS fatas_sem_cliente,
        SUM(CASE WHEN p.product_card_id IS NULL THEN 1 ELSE 0 END) AS fatos_sem_produto,
        SUM(CASE WHEN m.market_key IS NULL THEN 1 ELSE 0 END) AS fatos_sem_mercado
    FROM supplychain.gold.fact_sales AS f
    LEFT JOIN supplychain.gold.dim_customers AS c ON f.customer_id = c.customer_id
    LEFT JOIN supplychain.gold.dim_products  AS p ON f.product_card_id = p.product_card_id
    LEFT JOIN supplychain.gold.dim_markets   AS m ON f.market_key = m.market_key
""")
display(integridade)

# Amostra juntando fato com as dimensoes (visao final da gold)
display(spark.sql("""
    SELECT
        f.order_date,
        c.segment              AS cliente_segmento,
        c.country              AS cliente_pais,
        p.category_name        AS categoria,
        p.product_name         AS produto,
        m.country              AS destino_pais,
        f.order_item_quantity  AS qtd,
        f.sales                AS vendas,
        f.order_profit_per_order AS lucro
    FROM supplychain.gold.fact_sales AS f
    JOIN supplychain.gold.dim_customers AS c ON f.customer_id = c.customer_id
    JOIN supplychain.gold.dim_products  AS p ON f.product_card_id = p.product_card_id
    JOIN supplychain.gold.dim_markets   AS m ON f.market_key = m.market_key
    LIMIT 10
"""))