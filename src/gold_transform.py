
"""
DataForge Gold transformation logic.

Reusable SQL for creating the Gold sales fact table
and analytics marts.

These transformations can run in DuckDB without Azure.
"""

FACT_SALES_SQL = """
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
        CAST(p.unit_cost AS DECIMAL(18,2)),
        2
    ) AS total_cost,

    ROUND(
        CAST(o.revenue AS DECIMAL(18,2)) -
        (
            CAST(o.quantity AS INTEGER) *
            CAST(p.unit_cost AS DECIMAL(18,2))
        ),
        2
    ) AS estimated_profit

FROM silver_orders o

LEFT JOIN customers c
    ON o.customer_id = c.customer_id

LEFT JOIN products p
    ON o.product_id = p.product_id
"""


MART_QUERIES = {

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


def build_gold_tables(con):
    """
    Build the Gold fact table and analytics marts
    using an existing DuckDB connection.
    """

    con.execute(FACT_SALES_SQL)

    for name, query in MART_QUERIES.items():
        con.execute(
            f"CREATE TABLE {name} AS {query}"
        )


def check_gold_quality(con):
    """
    Validate Gold uniqueness, reference integrity
    and reconciliation.
    """

    silver_count = con.execute(
        "SELECT COUNT(*) FROM silver_orders"
    ).fetchone()[0]

    fact_totals = con.execute("""
        SELECT
            COUNT(*),
            SUM(revenue),
            SUM(estimated_profit)
        FROM gold_sales
    """).fetchone()

    if silver_count != fact_totals[0]:
        raise ValueError(
            "Silver-to-Gold row count mismatch"
        )

    duplicates = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT order_id)
        FROM gold_sales
    """).fetchone()[0]

    if duplicates:
        raise ValueError(
            f"Duplicate Gold orders: {duplicates}"
        )

    missing_refs = con.execute("""
        SELECT COUNT(*)
        FROM gold_sales
        WHERE region IS NULL
           OR customer_segment IS NULL
           OR category IS NULL
           OR unit_cost IS NULL
    """).fetchone()[0]

    if missing_refs:
        raise ValueError(
            f"Missing reference data: {missing_refs}"
        )

    for name in MART_QUERIES:

        mart_totals = con.execute(f"""
            SELECT
                SUM(total_orders),
                SUM(total_revenue),
                SUM(estimated_profit)
            FROM {name}
        """).fetchone()

        if mart_totals != fact_totals:
            raise ValueError(
                f"Gold reconciliation failed: {name}"
            )

    return {
        "orders": fact_totals[0],
        "revenue": fact_totals[1],
        "estimated_profit": fact_totals[2],
        "marts_checked": len(MART_QUERIES),
    }
