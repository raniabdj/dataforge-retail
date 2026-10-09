
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_script(script_name):
    print(f"\nRunning {script_name}...", flush=True)

    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "src" / script_name)],
        cwd=PROJECT_ROOT,
        check=True,
    )


def upload_file(local_path, azure_path):
    print(f"\nUploading {azure_path}...", flush=True)

    subprocess.run(
        [
            "az", "storage", "fs", "file", "upload",
            "--account-name", "dataforgelake2026",
            "--file-system", "dataforge",
            "--path", azure_path,
            "--source", str(PROJECT_ROOT / local_path),
            "--auth-mode", "login",
            "--overwrite", "true",
            "--only-show-errors",
            "--output", "none",
        ],
        check=True,
    )

    print(f"Uploaded: {azure_path}", flush=True)


def main():
    print("Starting DataForge Azure pipeline...", flush=True)

    # 1. Read Bronze data and generate Silver
    run_script("azure_bronze_reader.py")

    # 2. Upload Silver
    upload_file(
        "outputs/silver/clean_orders.parquet",
        "silver/clean_orders.parquet",
    )

    # 3. Generate Gold and run reconciliation checks
    run_script("azure_gold.py")

    # 4. Upload all Gold datasets
    gold_files = [
        "fct_sales",
        "mart_daily_sales",
        "mart_category_performance",
        "mart_customer_segments",
    ]

    for name in gold_files:
        upload_file(
            f"outputs/gold/{name}.parquet",
            f"gold/{name}.parquet",
        )

    print("\nDataForge Azure pipeline completed successfully!")


if __name__ == "__main__":
    main()
