
"""
DataForge - Azure Gold Layer

Reads Silver Parquet and Azure Bronze reference data,
builds Gold fact and analytics tables using reusable
transformations, validates data quality, and exports Parquet.
"""

from pathlib import Path
from io import BytesIO

import duckdb
import pandas as pd

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

from gold_transform import (
    build_gold_tables,
    check_gold_quality,
    MART_QUERIES,
)


# ==========================================
# 1. CONFIGURATION
# ==========================================

STORAGE_ACCOUNT = "dataforgelake2026"
FILE_SYSTEM = "dataforge"

SILVER_PATH = Path("outputs/silver/clean_orders.parquet")
OUTPUT_DIR = Path("outputs/gold")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# 2. CONNECT TO AZURE DATA LAKE
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
# 3. DOWNLOAD BRONZE REFERENCE DATA
# ==========================================

def download_csv_from_azure(file_path):
    """Download a CSV from Azure Data Lake into pandas."""

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
# 4. LOAD SILVER DATA INTO DUCKDB
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
    # 5. BUILD GOLD FACT TABLE AND MARTS
    # ==========================================

    print("\n--- BUILDING GOLD TABLES ---")

    # Use reusable production transformation logic
    build_gold_tables(con)

    print("Gold fact table created")
    print("Three analytics marts created")


    # ==========================================
    # 6. VALIDATE GOLD DATA QUALITY
    # ==========================================

    print("\n--- GOLD DATA QUALITY CHECKS ---")

    quality = check_gold_quality(con)

    print("PASS: Silver and Gold order counts match")
    print("PASS: Gold order IDs are unique")
    print("PASS: Customer and product references are valid")

    for name in MART_QUERIES:
        print(
            f"PASS: {name} reconciles with gold_sales"
        )

    print("\n--- GOLD FACT TABLE ---")
    print(f"Orders: {quality['orders']}")
    print(f"Revenue: {quality['revenue']}")
    print(
        f"Estimated profit: {quality['estimated_profit']}"
    )

    print("\nAll Gold reconciliation checks passed!")


    # ==========================================
    # 7. EXPORT GOLD FACT TABLE
    # ==========================================

    fact_path = OUTPUT_DIR / "fct_sales.parquet"

    con.execute(f"""
        COPY gold_sales
        TO '{fact_path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    print(f"\nSaved: {fact_path}")


    # ==========================================
    # 8. EXPORT GOLD ANALYTICS MARTS
    # ==========================================

    print("\n--- EXPORTING GOLD ANALYTICS MARTS ---")

    for name in MART_QUERIES:

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

finally:
    con.close()


# ==========================================
# 9. FINISH
# ==========================================

print("\nDataForge Gold pipeline completed successfully!")
