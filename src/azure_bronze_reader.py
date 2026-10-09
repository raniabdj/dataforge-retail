import csv
import io
import json

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

ACCOUNT = "dataforgelake2026"
FILESYSTEM = "dataforge"

credential = AzureCliCredential()

service = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT}.dfs.core.windows.net",
    credential=credential,
)

filesystem = service.get_file_system_client(FILESYSTEM)


def read_file(path):
    file_client = filesystem.get_file_client(path)
    return file_client.download_file().readall().decode("utf-8")


# Read customers
customers_text = read_file("bronze/customers.csv")
customers = list(csv.DictReader(io.StringIO(customers_text)))

# Read products
products_text = read_file("bronze/products.csv")
products = list(csv.DictReader(io.StringIO(products_text)))

# Read order events
events_text = read_file("bronze/events.jsonl")
events = [
    json.loads(line)
    for line in events_text.splitlines()
    if line.strip()
]

print("Azure Bronze data loaded successfully!")
print(f"Customers: {len(customers)}")
print(f"Products: {len(products)}")
print(f"Raw events: {len(events)}")

print("\nFirst customer:")
print(customers[0])

print("\nFirst event:")
print(events[0])
# SILVER LAYER: VALIDATION AND DEDUPLICATION

valid_customer_ids = {c["customer_id"] for c in customers}
valid_product_ids = {p["product_id"] for p in products}

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

print("\n--- SILVER VALIDATION RESULTS ---")
print(f"Raw events: {len(events)}")
print(f"Valid events: {len(clean_events)}")
print(f"Rejected events: {len(rejected_events)}")
print(f"Duplicate events: {duplicate_count}")

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

silver_dir = Path("outputs/silver")
silver_dir.mkdir(parents=True, exist_ok=True)

silver_path = silver_dir / "clean_orders.parquet"

silver_table = pa.Table.from_pylist(clean_events)

pq.write_table(
    silver_table,
    silver_path,
    compression="snappy",
)

print(f"\nSilver Parquet created: {silver_path}")
print(f"Rows written: {silver_table.num_rows}")