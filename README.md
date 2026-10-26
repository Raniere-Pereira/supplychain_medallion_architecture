# DataCo Supply Chain — Arquitetura Medalhão

Projeto de engenharia de dados que implementa uma **arquitetura medalhão** (Bronze → Silver → Gold) sobre o dataset **DataCo Smart Supply Chain**, contendo ~180.519 itens de pedido com datas de 2015 a 2018, abrangendo informações de pedidos, clientes, produtos, categorias, departamentos, vendas, descontos, lucro, métodos e custos de envio, datas de pedido e entrega, além de dados geográficos e operacionais da cadeia de suprimentos.

## Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                      VOLUME (CSV)                        │
│   /Volumes/supplychain/bronze/rawdata/                   │
│   DataCoSupplyChainDataset.csv                           │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│  BRONZE  (supplychain.bronze)                            │
│  dataco_supply_chain                                     │
│  • Dados brutos preservados como string (sem inferência)  │
│  • Metadados de ingestão (_ingestion_timestamp,          │
│    _source_file_name, _source_file_path)                  │
│  • Nomes de colunas normalizados (snake_case)             │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│  SILVER  (supplychain.silver)                             │
│  dataco_supply_chain (61 colunas tipadas)                 │
│  mv_dataco_quarantine (MV de quarentena de qualidade)     │
│  • Tipagem explicita (INT, DOUBLE, STRING, TIMESTAMP)    │
│  • Colunas derivadas: order_date, order_time,            │
│    shipping_date, shipping_time, shipping_delay_days     │
│  • Constraints de qualidade: nulidade, faixa, dominio     │
│    e consistencia                                        │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│  GOLD  (supplychain.gold)                                │
│  Esquema em estrela (star schema)                        │
│  • dim_customers  (20.652 linhas)                         │
│  • dim_products   (118 linhas)                           │
│  • dim_markets    (3.772 linhas)                          │
│  • fact_sales     (180.519 linhas, CLUSTER BY order_date) │
└──────────────────────────────────────────────────────────┘
```

## Estrutura do Repositorio

```
supplychain_medallion_architecture/
├── README.md
├── Schemas.py                          # Criação dos schemas bronze, silver, gold
├── Notebooks/
│   ├── 01_Bronze_Data_Analysis.py     # Análise exploratória do CSV
│   ├── 02_Bronze_Table.py             # Ingestão e normalização → tabela bronze
│   ├── 03_Bronze_Validation.py         # Validação da camada bronze
│   ├── 04_Silver_Verification.py      # Verificações de nulos, unicidade e mascaramento
│   ├── 04_Silver_Espelho.py           # Tipagem e colunas derivadas → tabela silver
│   ├── 05_Gold_Tables.py              # Criação do star schema na camada gold
│   └── 06_Validation_Medallion.py     # Validação cruzada bronze→silver→gold
└── Pipeline Supply Chain/
    └── transformations/
        ├── 01_Pipeline.py             # View temporária de quality flags (dq)
        ├── 02_Pipeline.py             # Views de constraints (valid/violations/summary)
        └── 03_Pipeline.py             # Materialized View de quarentena (MV)
```

## Notebooks

### `Schemas` — Criação dos Schemas

Cria os três schemas no catálogo `supplychain`:
```sql
CREATE SCHEMA IF NOT EXISTS supplychain.bronze;
CREATE SCHEMA IF NOT EXISTS supplychain.silver;
CREATE SCHEMA IF NOT EXISTS supplychain.gold;
```

### `01_Bronze_Data_Analysis` — Análise Exploratória

Le o CSV bruto do volume UC e faz análise exploratória completa:
- Contagem de linhas e colunas (~180.519 registros, 53 colunas originais)
- Schema inferido e resumo estatístico das colunas numericas
- Verificacao de valores nulos por coluna
- Cardinalidade e valores distintos de colunas categoricas
- Distribuicoes de Type, Delivery Status, Shipping Mode, Customer Segment, Order Status, Market, Department, Category

### `02_Bronze_Table` — Ingestão Bronze

Le o CSV mantendo todas as colunas como string (sem inferencia de tipo) e cria a tabela Delta na camada bronze:
- Adiciona metadados tecnicos: `_ingestion_timestamp`, `_source_file_name`, `_source_file_path`
- Normaliza nomes de colunas para snake_case (remove espacos, hifens, caracteres especiais)
- Salva como `supplychain.bronze.dataco_supply_chain` (overwrite, columnMapping mode=name)
- Adiciona comment na tabela descrevendo o conteudo
- Verificacao de preservacao: compara CSV original vs tabela Delta linha a linha

### `03_Bronze_Validation` — Validação Bronze

Validacoes SQL sobre a tabela bronze:
- Amostra e descricao do schema
- Contagem total de registros
- Verificacao de timestamps de ingestao (min/max)
- Contagem de pedidos distintos por `Order Id`
- Registros por arquivo de origem (`_source_file_path`)

### `04_Silver_Verification` — Verificações de Qualidade

Analise detalhada da qualidade dos dados na silver:
- **Nulos:** contagem e percentual de valores nulos por coluna
- **Mascaramento:** verificacao de politicas de mascaramento do Unity Catalog e valores com aparencia de mascara (\*\*\*, XXXX, ####)
- **Unicidade:** teste de unicidade em todas as colunas terminadas em `_id` (total, distintos, duplicados, % unico)
- Colunas analisadas: `order_item_id`, `order_id`, `customer_id`, `product_card_id`, `category_id`, `department_id`, `order_customer_id`, `order_item_cardprod_id`, `product_category_id`

### `04_Silver_Espelho` — Tipagem Silver

Cria a tabela silver tipada a partir da bronze:
- `CREATE OR REPLACE TABLE` com CAST explicito para INT, BIGINT, DOUBLE, STRING e TIMESTAMP
- Datas convertidas com `to_timestamp('M/d/yyyy H:mm')`
- Adiciona colunas derivadas: `order_date`, `order_time`, `shipping_date`, `shipping_time`
- Adiciona `shipping_delay_days` = `days_for_shipment_scheduled - days_for_shipping_real`
- Reordena colunas derivadas adjacentes às suas originais

### `05_Gold_Tables` — Star Schema Gold

Cria o esquema em estrela na camada gold:

| Tabela | Linhas | Grão | Chave | Descrição |
| --- | --- | --- | --- | --- |
| `dim_customers` | 20.652 | 1 linha por `customer_id` | `customer_id` | Dados de cliente (exclui `customer_password` e `customer_street` por segurança). Inclui `first_order_date` e `last_order_date` |
| `dim_products` | 118 | 1 linha por `product_card_id` | `product_card_id` | Produto com categoria e departamento denormalizados |
| `dim_markets` | 3.772 | Hierarquia geográfica | `market_key` (MD5) | Mercado → Região → País → Estado → Cidade. Chave substituta determinística via `md5(concat_ws('|', ...))` |
| `fact_sales` | 180.519 | Item de pedido (`order_item_id`) | `order_item_id` | Fatos de vendas com medidas comerciais (sales, profit, discount) e logísticas (delivery, shipping). `CLUSTER BY (order_date)` |

Validacao de integridade referencial: LEFT JOINs entre `fact_sales` e dimensoes com verificacao de fatos orfaos (0 encontrados).

### `06_Validation_Medallion` — Validação Cruzada

Validacao completa da arquitetura medalhão:
- Lista todas as tabelas por schema (bronze, silver, gold)
- Conta linhas por tabela em cada camada
- Compara volumes: bronze → silver nao pode diminuir; gold pode ser menor (agregacoes/dimensões)
- Sinaliza possiveis perdas de registros entre camadas
- Verifica duplicatas em colunas-chave (`_id`, `_key`) em todas as tabelas

## Pipeline Transformations

### `01_Pipeline.py` — Quality Flags (View Temporária)

Cria a view temporaria `vw_data_quality_flags` que marca registros com:
- Valores nulos em colunas obrigatorias (50+ colunas verificadas)
- Violações de faixa: valores negativos, nao-positivos, fora de intervalo (latitude/longitude, discount_rate, profit_ratio, etc.)
- Violações de dominio categorico: Type, Delivery Status, Order Status, Customer Segment, Shipping Mode, Market, Customer Country
- Violações de consistencia: colunas derivadas vs originais, FKs de mesma cardinalidade, regras de negocio
- Saida: colunas `_flag_*` (BOOLEAN), `_violations` (ARRAY), `_has_violations` (BOOLEAN), `_violation_count` (INT)

### `02_Pipeline.py` — Constraints (Views)

Cria tres views a partir da silver com tratamento NULL-safe:
- `vw_silver_validated` — registros que passam em TODAS as constraints
- `vw_silver_violations` — registros que falham em ≥1 constraint (com flags individuais)
- `vw_constraint_summary` — resumo de pass/fail por constraint

Tratamento de NULLs: constraints de nao-nulidade fazem NULL falhar; constraints de faixa/dominio/consistencia fazem NULL passar (evita falsos positivos).

### `03_Pipeline.py` — Materialized View de Quarentena

Cria a materialized view `supplychain.silver.mv_dataco_quarantine` com os registros que reprovaram em pelo menos uma constraint:
- Colunas `_viol_*` (BOOLEAN, NULL-safe) para cada constraint
- Coluna `_violations` (ARRAY&lt;STRING&gt;) com nomes das violacoes
- Coluna `_violation_count` (INT) com o total de violacoes
- Coluna `_quarantine_timestamp` (TIMESTAMP) com o momento da avaliacao
- Comments em cada coluna `_viol_*` explicando o motivo da reprovacao

**Violação conhecida:** `order_item_profit_ratio` fora de [-1, 1] em 6.301 registros.

> **Nota:** Materialized Views exigem SQL Warehouse (nao serverless compute).

## Catalogo e Schemas

| Catalogo | Schema | Tabelas/Views |
| --- | --- | --- |
| `supplychain` | `bronze` | `dataco_supply_chain` |
| `supplychain` | `silver` | `dataco_supply_chain`, `mv_dataco_quarantine` (MV) |
| `supplychain` | `gold` | `dim_customers`, `dim_products`, `dim_markets`, `fact_sales` |

## Dataset

- **Origem:** DataCo Smart Supply Chain Dataset
- **Registros:** ~180.519 itens de pedido
- **Periodo:** 2015 a 2018
- **Colunas originais:** 53 (normalizadas para snake_case na bronze)
- **Colunas na silver:** 61 (53 originais + 3 metadados + 5 derivadas)
- **Paises traduzidos:** nomes em portugues/espanhol (ex.: "EE. UU.", "SudAfrica", "Polonia")

## Pre-requisitos

- Databricks Workspace com Unity Catalog habilitado
- Catalogo `supplychain` criado
- Volume UC: `/Volumes/supplychain/bronze/rawdata/DataCoSupplyChainDataset.csv`
- SQL Warehouse para Materialized Views (nao compativel com serverless)
- Acesso ao catalogo `supplychain` com permissoes de CREATE TABLE/SCHEMA

## Ordem de Execucao

1. `Schemas` — cria os schemas bronze, silver e gold
2. `01_Bronze_Data_Analysis` — analise exploratoria do CSV (opcional)
3. `02_Bronze_Table` — ingestao para a tabela bronze
4. `03_Bronze_Validation` — validacao da bronze
5. `04_Silver_Espelho` — tipagem e derivacao para a silver
6. `04_Silver_Verification` — verificacoes de qualidade da silver
7. `Pipeline Supply Chain/transformations/01_Pipeline.py` — quality flags
8. `Pipeline Supply Chain/transformations/02_Pipeline.py` — views de constraints
9. `Pipeline Supply Chain/transformations/03_Pipeline.py` — MV de quarentena
10. `05_Gold_Tables` — criacao do star schema gold
11. `06_Validation_Medallion` — validacao cruzada bronze→silver→gold

## Tecnologias

- **Databricks** — plataforma de dados
- **Unity Catalog** — governanca e catalogo
- **Delta Lake** — formato de tabela transacional
- **PySpark / Spark SQL** — processamento distribuido
- **Materialized Views** — qualidade de dados e quarentena
- **UC Volumes** — armazenamento de arquivos brutos