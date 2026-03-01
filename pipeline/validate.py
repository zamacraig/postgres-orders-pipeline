"""Validate: run data-quality checks on the transformed DataFrame."""

import pandas as pd

from config import VALID_STATUSES


def validate(df: pd.DataFrame) -> None:
    """Raise ValueError if any data-quality rule is violated."""
    errors: list[str] = []

    # No null values allowed in key columns
    key_cols = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_date", "status"]
    for col in key_cols:
        null_count = df[col].isna().sum()
        if null_count:
            errors.append(f"Column '{col}' has {null_count} null value(s).")

    # Quantities must be positive
    if (df["quantity"] <= 0).any():
        errors.append("Found rows with non-positive quantity.")

    # Unit prices must be non-negative
    if (df["unit_price"] < 0).any():
        errors.append("Found rows with negative unit_price.")

    # Status must be one of the allowed values
    invalid_statuses = set(df["status"].unique()) - VALID_STATUSES
    if invalid_statuses:
        errors.append(f"Found invalid status values: {invalid_statuses}")

    # Order IDs must be unique
    if df["order_id"].duplicated().any():
        errors.append("Duplicate order_id values detected.")

    if errors:
        raise ValueError("Data quality checks failed:\n" + "\n".join(f"  - {e}" for e in errors))

    print(f"[validate] All checks passed for {len(df)} rows.")
