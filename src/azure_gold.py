
from pathlib import Path
import duckdb

# ==========================================
# DATAFORGE - AZURE GOLD LAYER
# ==========================================

OUTPUT_DIR = Path("outputs/gold")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

# ==========================================
# 1. READ SILVER AND REFERENCE DATA
# ==========================================

con.execute("""
CREATE VIEW silver_orders AS
SELECT *
FROM read_parquet('outputs/silver/clean_orders.parquet')
""")

con.execute("""
CREATE VIEW customers AS
SELECT *
FROM read_csv_auto('data/customers.csv')
""")

con.execute("""
CREATE VIEW products AS
SELECT *
FROM read_csv_auto('data/products.csv')
""")

# ==========================================
# 2. CREATE GOLD FACT SALES
# ==========================================

con.execute("""
CREATE TABLE gold_sales AS
SELECT
    o.order_id,
    o.event_id,
    CAST(o.event_time AS DATE) AS order_date,
    o.status AS order_status,
    o.customer_id,
    c.region,
    c.segment AS customer_segment,
    o.product_id,
    p.category,
    CAST(o.quantity AS INTEGER) AS quantity,
    CAST(o.revenue AS DECIMAL(18,2)) AS revenue,
    CAST(p.unit_cost AS DECIMAL(18,2)) AS unit_cost,
    ROUND(
        CAST(o.quantity AS INTEGER) *
        CAST(p.unit_cost AS DECIMAL(18,2)), 2
    ) AS total_cost,
    ROUND(
        CAST(o.revenue AS DECIMAL(18,2)) -
        (
            CAST(o.quantity AS INTEGER) *
            CAST(p.unit_cost AS DECIMAL(18,2))
        ), 2
    ) AS estimated_profit
FROM silver_orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
LEFT JOIN products p
    ON o.product_id = p.product_id
""")

# Export fact table
con.execute("""
COPY gold_sales
TO 'outputs/gold/fct_sales.parquet'
(FORMAT PARQUET, COMPRESSION SNAPPY)
""")

print("\n--- GOLD FACT TABLE ---")

result = con.execute("""
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(revenue), 2) AS total_revenue,
    ROUND(SUM(estimated_profit), 2) AS estimated_profit
FROM gold_sales
""").fetchone()

print(f"Orders: {result[0]}")
print(f"Revenue: {result[1]}")
print(f"Estimated profit: {result[2]}")

# ==========================================
# 3. CREATE GOLD ANALYTICS MARTS
# ==========================================

marts = {

    # Daily sales analytics
    "mart_daily_sales": """
        SELECT
            order_date,
            COUNT(*) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_revenue,
            ROUND(SUM(estimated_profit), 2)
                AS estimated_profit
        FROM gold_sales
        GROUP BY order_date
        ORDER BY order_date
    """,

    # Product category performance
    "mart_category_performance": """
        SELECT
            category,
            COUNT(*) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_revenue,
            ROUND(SUM(estimated_profit), 2)
                AS estimated_profit
        FROM gold_sales
        GROUP BY category
        ORDER BY total_revenue DESC
    """,

    # Customer segment analytics
    "mart_customer_segments": """
        SELECT
            customer_segment,
            COUNT(*) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_revenue,
            ROUND(SUM(estimated_profit), 2)
                AS estimated_profit
        FROM gold_sales
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
    """
}

print("\n--- GOLD ANALYTICS MARTS ---")

for name, query in marts.items():

    # Create analytics table
    con.execute(f"CREATE TABLE {name} AS {query}")

    # Export to Parquet
    output_path = OUTPUT_DIR / f"{name}.parquet"

    con.execute(f"""
        COPY {name}
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {name}"
    ).fetchone()[0]

    print(f"{name}: {row_count} rows")
    print(f"Saved: {output_path}")

# ==========================================
# 4. DATA QUALITY AND RECONCILIATION
# ==========================================

print("\n--- GOLD DATA QUALITY CHECKS ---")

fact_totals = con.execute("""
SELECT
    COUNT(*),
    SUM(revenue),
    SUM(estimated_profit)
FROM gold_sales
""").fetchone()

for name in marts:

    mart_totals = con.execute(f"""
        SELECT
            SUM(total_orders),
            SUM(total_revenue),
            SUM(estimated_profit)
        FROM {name}
    """).fetchone()

    if mart_totals != fact_totals:
        raise ValueError(
            f"Reconciliation failed for {name}: "
            f"expected {fact_totals}, got {mart_totals}"
        )

    print(f"PASS: {name} reconciles with gold_sales")

print("\nAll Gold reconciliation checks passed!")

# ==========================================
# 5. FINISH
# ==========================================

con.close()

print("\nDataForge Gold pipeline completed successfully!")
