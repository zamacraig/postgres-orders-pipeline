"""Ingest: read the raw orders CSV into a pandas DataFrame."""

import pandas as pd


EXPECTED_COLUMNS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_date",
    "status",
}


def ingest(filepath: str) -> pd.DataFrame:
    """Read orders CSV and return a raw DataFrame."""
    df = pd.read_csv(filepath)
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in source file: {missing}")
    return df
