
import tempfile
import unittest
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


class AzurePipelineTests(unittest.TestCase):

    def test_silver_parquet_and_gold_reconciliation(self):

        # Create temporary test data
        with tempfile.TemporaryDirectory() as temp_dir:

            silver_path = Path(temp_dir) / "silver.parquet"

            orders = [
                {
                    "order_id": "O001",
                    "customer_id": "C001",
                    "product_id": "P001",
                    "quantity": 2,
                    "revenue": 100.0,
                },
                {
                    "order_id": "O002",
                    "customer_id": "C002",
                    "product_id": "P002",
                    "quantity": 1,
                    "revenue": 80.0,
                },
            ]

            # Write sample Silver data to Parquet
            pq.write_table(
                pa.Table.from_pylist(orders),
                silver_path,
            )

            con = duckdb.connect()

            try:
                # Read Silver Parquet into DuckDB
                # Register a relation instead of using
                # parameters inside CREATE VIEW.
                con.read_parquet(
                    str(silver_path)
                ).create_view("silver_orders")

                # Create product reference data
                con.execute("""
                    CREATE TABLE products (
                        product_id VARCHAR,
                        category VARCHAR,
                        unit_cost DECIMAL(18,2)
                    )
                """)

                con.execute("""
                    INSERT INTO products VALUES
                    ('P001', 'Electronics', 20.00),
                    ('P002', 'Home', 30.00)
                """)

                # Create Gold sales fact table
                con.execute("""
                    CREATE TABLE gold_sales AS
                    SELECT
                        o.order_id,
                        p.category,
                        o.quantity,
                        o.revenue,

                        ROUND(
                            o.quantity * p.unit_cost,
                            2
                        ) AS total_cost,

                        ROUND(
                            o.revenue -
                            (o.quantity * p.unit_cost),
                            2
                        ) AS estimated_profit

                    FROM silver_orders o
                    JOIN products p
                        ON o.product_id = p.product_id
                """)

                # Validate Gold calculations
                totals = con.execute("""
                    SELECT
                        COUNT(*),
                        SUM(revenue),
                        SUM(total_cost),
                        SUM(estimated_profit)
                    FROM gold_sales
                """).fetchone()

                self.assertEqual(totals[0], 2)
                self.assertEqual(float(totals[1]), 180.0)
                self.assertEqual(float(totals[2]), 70.0)
                self.assertEqual(float(totals[3]), 110.0)

                # Create category analytics mart
                con.execute("""
                    CREATE TABLE mart_category_performance AS
                    SELECT
                        category,
                        COUNT(*) AS total_orders,
                        SUM(revenue) AS total_revenue,
                        SUM(estimated_profit)
                            AS estimated_profit
                    FROM gold_sales
                    GROUP BY category
                """)

                # Reconcile mart totals with fact table
                mart_totals = con.execute("""
                    SELECT
                        SUM(total_orders),
                        SUM(total_revenue),
                        SUM(estimated_profit)
                    FROM mart_category_performance
                """).fetchone()

                self.assertEqual(
                    mart_totals[0],
                    totals[0]
                )

                self.assertEqual(
                    float(mart_totals[1]),
                    float(totals[1])
                )

                self.assertEqual(
                    float(mart_totals[2]),
                    float(totals[3])
                )

                print(
                    "\nPASS: Silver Parquet read successfully"
                )
                print(
                    "PASS: Gold revenue and profit calculations"
                )
                print(
                    "PASS: Category mart reconciliation"
                )

            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
