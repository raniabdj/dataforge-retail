
import sys
import unittest
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src")
)

from silver_transform import validate_orders


class SilverTransformationTests(unittest.TestCase):

    def setUp(self):
        self.customers = [
            {"customer_id": "C001"},
            {"customer_id": "C002"},
        ]

        self.products = [
            {"product_id": "P001"},
            {"product_id": "P002"},
        ]

    def make_event(
        self,
        order_id,
        customer_id="C001",
        product_id="P001",
        revenue=100,
        quantity=2,
    ):
        return {
            "event_id": f"E-{order_id}",
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "revenue": revenue,
            "quantity": quantity,
        }

    def test_valid_orders(self):
        events = [
            self.make_event("O001"),
            self.make_event("O002"),
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 2)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(duplicates, 0)

    def test_duplicate_orders(self):
        events = [
            self.make_event("O001"),
            self.make_event("O001"),
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 1)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(duplicates, 1)

    def test_negative_revenue(self):
        events = [
            self.make_event("O001", revenue=-20)
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)

    def test_invalid_customer(self):
        events = [
            self.make_event(
                "O001", customer_id="UNKNOWN"
            )
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)

    def test_invalid_product(self):
        events = [
            self.make_event(
                "O001", product_id="UNKNOWN"
            )
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)

    def test_zero_quantity(self):
        events = [
            self.make_event("O001", quantity=0)
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)

    def test_missing_order_id(self):
        events = [
            self.make_event("")
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)

    def test_invalid_numeric_value(self):
        events = [
            self.make_event("O001", revenue="invalid")
        ]

        clean, rejected, duplicates = validate_orders(
            events, self.customers, self.products
        )

        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(duplicates, 0)


if __name__ == "__main__":
    unittest.main()
