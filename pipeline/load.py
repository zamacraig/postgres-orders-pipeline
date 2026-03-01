"""Load: bulk-load the validated DataFrame into PostgreSQL using psycopg3."""

import os

import pandas as pd
import psycopg


def _get_connection() -> psycopg.Connection:
    """Build a psycopg3 connection from environment variables."""
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ.get("PGDATABASE", "orders"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def load(df: pd.DataFrame) -> None:
    """Truncate the orders table and bulk-insert all rows."""
    columns = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_price", "order_date", "status"]
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE orders RESTART IDENTITY CASCADE;")
            with cur.copy(
                "COPY orders (order_id, customer_id, product_id, quantity, unit_price, total_price, order_date, status) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)
        conn.commit()

    print(f"[load] Loaded {len(rows)} rows into the orders table.")
