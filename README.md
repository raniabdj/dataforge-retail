# DataForge — End-to-End Retail Analytics Engineering Platform

A **runnable, end-to-end analytics engineering portfolio project** built with Python, SQL, SQLite, DuckDB and dbt Core. It models a realistic event-data workflow: source ingestion → deduplication → data quality checks → dimensional warehouse → analytical SQL marts → CSV outputs. All input data is **synthetic**, not real customer data.

## Architecture

```text
Synthetic customers.csv / products.csv / events.jsonl
                   │
                   ▼
        Python ingestion + validation
                   │
            ┌──────┴────────┐
            ▼               ▼
      raw_events      rejected_events
            │
            ▼
    fact_orders + dim_customer + dim_product (SQLite star schema)
            │
            ▼
    SQL marts: daily sales / category / segments / monthly growth
            │
            ▼
      outputs/*.csv + quality_report.json
```

## Quick start (macOS / Windows / Linux)

Requires Python 3.10+. **No pip install required.**

```bash
python3 src/dataforge_pipeline.py all
python3 -m unittest discover -s tests -v
```

On Windows use `python` instead of `python3`.

Outputs are generated under `outputs/`; warehouse stored in `data/warehouse.sqlite` (ignored by Git).

## Engineering capabilities demonstrated

- **Incremental, idempotent ingestion:** events are keyed by `event_id`, so rerunning the pipeline does not double-count orders.
- **Data quality:** invalid measures, invalid statuses and unknown dimension keys are rejected into a quarantine table; automated checks validate uniqueness, referential integrity and nonnegative revenue.
- **Data modelling:** a documented star schema with `fact_orders`, `dim_customer`, and `dim_product`.
- **Analytical SQL:** CTEs, conditional aggregation, joins, window functions (`LAG`), and growth calculations.
- **Observability:** pipeline run metrics, rejection counts, and a machine-readable quality report.
- **CI:** GitHub Actions runs unit tests and a full pipeline smoke test.
- **Reproducibility:** deterministic event generation and no external service credentials.

## Outputs and business questions

| Output | Question |
|---|---|
| `01_daily_sales.csv` | How do order volume, returns and net revenue change daily? |
| `02_category_performance.csv` | Which product categories generate the most revenue? |
| `03_customer_segments.csv` | How do regions and customer segments compare? |
| `04_monthly_growth.csv` | What is the month-over-month revenue trend? |
| `quality_report.json` | Did warehouse quality checks pass? |
| `run_summary.json` | How many events were inserted, skipped or rejected? |

**Metric definition:** net revenue = completed-order revenue minus returned-order revenue; cancelled orders contribute zero. This is a portfolio simplification, not a full accounting treatment of refunds.

## Important limitations / roadmap

This is a **local prototype** with a working dbt Core and DuckDB analytics layer, not a production cloud deployment. Recommended next iterations:

1. Build a Power BI dashboard using the dbt analytics marts.
2. Replace SQLite with DuckDB/PostgreSQL and run via Docker Compose.
3. Add Airflow/Dagster orchestration, incremental partitioning, and late-arriving event handling.
4. Add more advanced analytics, including cohort analysis and customer retention metrics.
5. Deploy to Azure/Fabric, add monitoring, CI/CD and a cost-conscious cloud architecture.

## Interview discussion prompts

- Why use an event ID as an idempotency key? What happens if an event is corrected later?
- Why quarantine invalid records rather than silently drop them?
- What would change for 100 million events per day?
- How would you handle slowly changing customer attributes and late-arriving data?
- How would you make return accounting and profitability metrics more accurate?

**Portfolio note:** Built as an independent learning/demo project. Generated data and simulated business results are not real operational findings.

## Phase 2 — dbt and DuckDB Analytics Engineering

The project includes a dbt Core analytics layer powered by DuckDB.

### Data Models

- **Staging views:** stg_customers, stg_products, stg_orders
- **Fact table:** fct_sales
- **Analytics marts:** mart_daily_sales, mart_category_performance, mart_customer_segments

The models use dbt ref() dependencies to build a documented data lineage graph.

### Data Quality

Automated dbt tests validate identifiers, relationships, and reconciliation of sales metrics across the marts.

### Run the dbt Project

Install the dependencies:

    python3 -m pip install "dbt-core==1.12.5" "dbt-duckdb==1.11.0"

Configure the dataforge_dbt profile in ~/.dbt/profiles.yml with a DuckDB database named dev.duckdb.

Then run:

    cd dataforge_dbt
    dbt build
    dbt docs generate
    dbt docs serve --port 8080

The dbt project uses synthetic CSV seeds included in the repository.



---

## Phase 3 — Azure Data Engineering Pipeline

DataForge implements a Bronze, Silver and Gold medallion architecture using Azure Data Lake Storage Gen2, Python, PyArrow and DuckDB.

### Architecture

- **Bronze:** Raw events (JSONL), customers (CSV) and products (CSV) stored in Azure.
- **Silver:** Python validation, duplicate detection, invalid-record rejection and clean Parquet output.
- **Gold:** DuckDB sales fact table and three analytics marts, exported as Parquet and uploaded to Azure.

### Pipeline Results

| Metric | Result |
|---|---:|
| Raw events | 12,013 |
| Valid orders | 11,975 |
| Rejected events | 25 |
| Duplicate events | 13 |
| Daily sales records | 150 |
| Product categories | 4 |
| Customer segments | 3 |
| Total revenue | 4,169,175.50 |
| Estimated gross profit | 3,189,154.20 |

The dataset is synthetic. Estimated gross profit excludes shipping, tax and other operating expenses.

### Azure Storage Structure

    dataforge/
      bronze/
        events.jsonl
        customers.csv
        products.csv
      silver/
        clean_orders.parquet
      gold/
        fct_sales.parquet
        mart_daily_sales.parquet
        mart_category_performance.parquet
        mart_customer_segments.parquet

### Running the Azure Pipeline

Install dependencies:

    python3 -m pip install -r requirements.txt

Authenticate with Azure:

    az login

Run the pipeline:

    python3 src/azure_pipeline.py

The storage account must already contain the Bronze files, and the signed-in account requires appropriate Azure storage data permissions.

The pipeline reads Bronze data, validates and deduplicates events, generates Silver and Gold Parquet files, runs reconciliation checks, and uploads five datasets to Azure.

### Current Limitations

- The pipeline is executed on demand, not on a schedule.
- Gold transformations currently read customer and product reference data from local CSV copies.
- Generated Parquet files are excluded from Git.
- Authentication uses Azure CLI credentials without embedding secrets in source code.
