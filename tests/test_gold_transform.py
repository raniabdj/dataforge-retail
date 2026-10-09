
import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src"),
)

from gold_transform import (
    build_gold_tables,
    check_gold_quality,
)


class GoldTransformationTests(unittest.TestCase):

    def setUp(self):
        self.con = duckdb.connect()

        self.con.execute("""
            CREATE TABLE silver_orders (
                order_id VARCHAR,
                event_id VARCHAR,
                event_time VARCHAR,
                status VARCHAR,
                customer_id VARCHAR,
                product_id VARCHAR,
                quantity INTEGER,
                revenue DECIMAL(18,2)
            )
        """)

        self.con.execute("""
            CREATE TABLE customers (
                customer_id VARCHAR,
                region VARCHAR,
                segment VARCHAR
            )
        """)

        self.con.execute("""
            CREATE TABLE products (
                product_id VARCHAR,
                category VARCHAR,
                unit_cost DECIMAL(18,2)
            )
        """)

        self.con.execute("""
            INSERT INTO customers VALUES
            ('C001', 'North', 'Consumer'),
            ('C002', 'South', 'Enterprise')
        """)

        self.con.execute("""
            INSERT INTO products VALUES
            ('P001', 'Electronics', 20.00),
            ('P002', 'Home', 30.00)
        """)

        self.con.execute("""
            INSERT INTO silver_orders VALUES
            ('O001', 'E001', '2026-01-01T10:00:00',
             'completed', 'C001', 'P001', 2, 100.00),
            ('O002', 'E002', '2026-01-01T11:00:00',
             'completed', 'C002', 'P002', 1, 80.00)
        """)

    def tearDown(self):
        self.con.close()

    def test_gold_revenue_cost_and_profit(self):
        build_gold_tables(self.con)

        results = self.con.execute("""
            SELECT
                order_id,
                revenue,
                total_cost,
                estimated_profit
            FROM gold_sales
            ORDER BY order_id
        """).fetchall()

        self.assertEqual(len(results), 2)

        self.assertEqual(float(results[0][1]), 100.0)
        self.assertEqual(float(results[0][2]), 40.0)
        self.assertEqual(float(results[0][3]), 60.0)

        self.assertEqual(float(results[1][1]), 80.0)
        self.assertEqual(float(results[1][2]), 30.0)
        self.assertEqual(float(results[1][3]), 50.0)

    def test_gold_quality_and_reconciliation(self):
        build_gold_tables(self.con)

        result = check_gold_quality(self.con)

        self.assertEqual(result["orders"], 2)
        self.assertEqual(float(result["revenue"]), 180.0)
        self.assertEqual(
            float(result["estimated_profit"]), 110.0
        )
        self.assertEqual(result["marts_checked"], 3)

    def test_customer_and_product_dimensions(self):
        build_gold_tables(self.con)

        result = self.con.execute("""
            SELECT
                region,
                customer_segment,
                category
            FROM gold_sales
            WHERE order_id = 'O001'
        """).fetchone()

        self.assertEqual(
            result,
            ("North", "Consumer", "Electronics"),
        )

    def test_duplicate_orders_are_rejected(self):
        self.con.execute("""
            INSERT INTO silver_orders
            SELECT * FROM silver_orders
            WHERE order_id = 'O001'
        """)

        build_gold_tables(self.con)

        with self.assertRaises(ValueError):
            check_gold_quality(self.con)

    def test_missing_product_reference_is_rejected(self):
        self.con.execute("""
            UPDATE silver_orders
            SET product_id = 'UNKNOWN'
            WHERE order_id = 'O001'
        """)

        build_gold_tables(self.con)

        with self.assertRaises(ValueError):
            check_gold_quality(self.con)


if __name__ == "__main__":
    unittest.main()
