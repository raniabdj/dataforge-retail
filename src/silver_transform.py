
"""
DataForge Silver transformation.

Pure Python validation and deduplication logic.
No Azure credentials or cloud connection required.
"""


def validate_orders(events, customers, products):
    """
    Validate and deduplicate Bronze order events.

    Returns:
        clean_events: Valid, unique orders
        rejected_events: Invalid orders
        duplicate_count: Number of duplicate orders
    """

    valid_customer_ids = {
        customer["customer_id"]
        for customer in customers
    }

    valid_product_ids = {
        product["product_id"]
        for product in products
    }

    clean_events = []
    rejected_events = []
    seen_order_ids = set()
    duplicate_count = 0

    for event in events:
        order_id = event.get("order_id")

        if not order_id:
            rejected_events.append(event)
            continue

        if order_id in seen_order_ids:
            duplicate_count += 1
            continue

        # Preserve the current pipeline's deduplication behaviour
        seen_order_ids.add(order_id)

        try:
            revenue = float(event["revenue"])
            quantity = int(event["quantity"])

            is_valid = (
                revenue >= 0
                and quantity > 0
                and event["customer_id"] in valid_customer_ids
                and event["product_id"] in valid_product_ids
            )

            if is_valid:
                clean_events.append(event)
            else:
                rejected_events.append(event)

        except (KeyError, TypeError, ValueError):
            rejected_events.append(event)

    return clean_events, rejected_events, duplicate_count
