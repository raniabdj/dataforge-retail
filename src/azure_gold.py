
from pathlib import Path
from io import BytesIO

import duckdb
import pandas as pd

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient


# ==========================================
# DATAFORGE - AZURE GOLD LAYER
# ==========================================

STORAGE_ACCOUNT = "dataforgelake2026"
FILE_SYSTEM = "dataforge"

SILVER_PATH = Path("outputs/silver/clean_orders.parquet")
OUTPUT_DIR = Path("outputs/gold")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# 1. CONNECT TO AZURE DATA LAKE
# ==========================================

print("\n--- CONNECTING TO AZURE DATA LAKE ---")

credential = AzureCliCredential()

service_client = DataLakeServiceClient(
    account_url=(
        f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
    ),
    credential=credential,
)

filesystem_client = service_client.get_file_system_client(
    FILE_SYSTEM
)

print("Connected to Azure Data Lake")


# ==========================================
# 2. DOWNLOAD BRONZE REFERENCE DATA
# ==========================================

def download_csv_from_azure(file_path):

    print(f"Downloading: {file_path}")

    file_client = filesystem_client.get_file_client(
        file_path
    )

    file_bytes = file_client.download_file().readall()

    dataframe = pd.read_csv(BytesIO(file_bytes))

    print(
        f"Loaded {len(dataframe)} rows from {file_path}"
    )

    return dataframe


customers_df = download_csv_from_azure(
    "bronze/customers.csv"
)

products_df = download_csv_from_azure(
    "bronze/products.csv"
)


# ==========================================
# 3. READ SILVER AND REGISTER REFERENCE DATA
# ==========================================

print("\n--- LOADING SILVER DATA ---")

if not SILVER_PATH.exists():
    raise FileNotFoundError(
        f"Silver file not found: {SILVER_PATH}. "
        "Run the Bronze-to-Silver pipeline first."
    )

con = duckdb.connect()

try:
    con.read_parquet(
        str(SILVER_PATH)
    ).create_view("silver_orders")

    con.register("customers", customers_df)
    con.register("products", products_df)

    silver_count = con.execute("""
        SELECT COUNT(*)
        FROM silver_orders
    """).fetchone()[0]

    print(f"Silver orders loaded: {silver_count}")


    # ==========================================
    # 4. CREATE GOLD FACT SALES
    # ==========================================

    print("\n--- CREATING GOLD FACT TABLE ---")

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
    """)

    # Check for missing customer or product matches
    missing_references = con.execute("""
        SELECT COUNT(*)
        FROM gold_sales
        WHERE region IS NULL
           OR customer_segment IS NULL
           OR category IS NULL
           OR unit_cost IS NULL
    """).fetchone()[0]

    if missing_references > 0:
        raise ValueError(
            f"Gold quality check failed: "
            f"{missing_references} rows have missing "
            "customer or product reference data."
        )

    # Export Gold fact table
    fact_path = OUTPUT_DIR / "fct_sales.parquet"

    con.execute(f"""
        COPY gold_sales
        TO '{fact_path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    result = con.execute("""
        SELECT
            COUNT(*) AS total_orders,
            ROUND(SUM(revenue), 2) AS total_revenue,
            ROUND(SUM(estimated_profit), 2)
                AS estimated_profit
        FROM gold_sales
    """).fetchone()

    print(f"Orders: {result[0]}")
    print(f"Revenue: {result[1]}")
    print(f"Estimated profit: {result[2]}")
    print(f"Saved: {fact_path}")


    # ==========================================
    # 5. CREATE GOLD ANALYTICS MARTS
    # ==========================================

    marts = {

        "mart_daily_sales": """
            SELECT
                order_date,
                COUNT(*) AS total_orders,
                ROUND(SUM(revenue), 2)
                    AS total_revenue,
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
                ROUND(SUM(revenue), 2)
                    AS total_revenue,
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
                ROUND(SUM(revenue), 2)
                    AS total_revenue,
                ROUND(SUM(estimated_profit), 2)
                    AS estimated_profit
            FROM gold_sales
            GROUP BY customer_segment
            ORDER BY total_revenue DESC
        """
    }

    print("\n--- CREATING GOLD ANALYTICS MARTS ---")

    for name, query in marts.items():

        con.execute(
            f"CREATE TABLE {name} AS {query}"
        )

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
    # 6. DATA QUALITY AND RECONCILIATION
    # ==========================================

    print("\n--- GOLD DATA QUALITY CHECKS ---")

    fact_totals = con.execute("""
        SELECT
            COUNT(*),
            SUM(revenue),
            SUM(estimated_profit)
        FROM gold_sales
    """).fetchone()

    # Validate that no orders were lost in Gold
    if fact_totals[0] != silver_count:
        raise ValueError(
            "Silver-to-Gold row count mismatch: "
            f"Silver={silver_count}, "
            f"Gold={fact_totals[0]}"
        )

    print("PASS: Silver and Gold order counts match")

    # Validate unique order IDs
    duplicate_count = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT order_id)
        FROM gold_sales
    """).fetchone()[0]

    if duplicate_count != 0:
        raise ValueError(
            f"Gold contains {duplicate_count} duplicate orders"
        )

    print("PASS: Gold order IDs are unique")

    # Reconcile each mart with the Gold fact table
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
                f"expected {fact_totals}, "
                f"got {mart_totals}"
            )

        print(
            f"PASS: {name} reconciles with gold_sales"
        )

    print("\nAll Gold reconciliation checks passed!")

finally:
    con.close()


# ==========================================
# 7. FINISH
# ==========================================

print("\nDataForge Gold pipeline completed successfully!")
