"""Transform: apply pandas transformations to the raw orders DataFrame."""

import pandas as pd

from config import VALID_STATUSES


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the orders DataFrame."""
    df = df.copy()

    # Parse date column
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

    # Cast numeric columns
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)

    # Derive total price
    df["total_price"] = (df["quantity"] * df["unit_price"]).round(2)

    # Normalise status to lowercase
    df["status"] = df["status"].str.strip().str.lower()

    return df
