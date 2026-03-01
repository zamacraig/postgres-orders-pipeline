"""Main entry point for the orders ETL pipeline."""

import os

from ingest import ingest
from transform import transform
from validate import validate
from load import load


DATA_FILE = os.environ.get("DATA_FILE", "/app/data/orders.csv")


def run() -> None:
    print(f"[pipeline] Starting ETL for {DATA_FILE}")

    df_raw = ingest(DATA_FILE)
    print(f"[ingest]   Read {len(df_raw)} rows from {DATA_FILE}")

    df = transform(df_raw)
    print("[transform] Transformations applied.")

    validate(df)

    load(df)

    print("[pipeline] ETL complete.")


if __name__ == "__main__":
    run()
