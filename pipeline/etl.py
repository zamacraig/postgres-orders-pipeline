"""ETL Pipeline for Orders Database."""

import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg

DATA_DIR = os.environ.get("DATA_DIR")
VALID_STATUSES = {"placed", "shipped", "cancelled", "refunded"}


def log(msg):
    """Print message with timestamp."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")


def is_valid_email(email):
    """Check if email matches pattern: local@domain.tld (min 2-char TLD)."""
    return bool(re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email or ""))


@contextmanager
def log_step(name):
    """Context manager to log step start/end with duration."""
    log(f"[{name}] Starting...")
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    log(f"[{name}] Done ({elapsed:.2f}s)")


def get_connection():
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ.get("PGDATABASE", "orders"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def transform(customers_df, orders_df, order_items_df):
    """
    Transform and validate all data.
    Returns (valid_customers, valid_orders, valid_items, rejected_customers, rejected_orders, rejected_items).
    """
    # === CUSTOMERS ===
    customers = customers_df.copy()
    customers["email"] = customers["email"].str.strip().str.lower()
    customers["full_name"] = customers["full_name"].str.strip()
    customers["country_code"] = customers["country_code"].str.strip().str.upper().replace("", None)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"]).dt.date
    customers["is_active"] = customers["is_active"].astype(str).str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    )

    # Validate customers
    null_id = customers["customer_id"].isna()
    bad_email = ~customers["email"].apply(lambda x: is_valid_email(x) if pd.notna(x) else False)

    rej_cust = pd.concat([
        customers[null_id].assign(reason="Null customer_id"),
        customers[bad_email].assign(reason="Invalid email"),
    ])

    valid_cust = customers[~null_id & ~bad_email].sort_values("signup_date")
    dup_email = valid_cust["email"].duplicated(keep="first")
    rej_cust = pd.concat([rej_cust, valid_cust[dup_email].assign(reason="Duplicate email")])
    valid_customers = valid_cust[~dup_email]
    valid_customer_ids = set(valid_customers["customer_id"])

    # === ORDERS ===
    orders = orders_df.copy()
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], utc=True, format="mixed")
    orders["status"] = orders["status"].str.strip().str.lower()
    orders["total_amount"] = orders["total_amount"].astype(float).round(2)
    orders["currency"] = orders["currency"].str.strip().str.upper()

    # Validate orders
    null_oid = orders["order_id"].isna()
    unknown_cust = ~orders["customer_id"].isin(valid_customer_ids)
    invalid_status = ~orders["status"].isin(VALID_STATUSES)

    rej_ord = pd.concat([
        orders[null_oid].assign(reason="Null order_id"),
        orders[unknown_cust].assign(reason="Unknown customer_id"),
        orders[invalid_status].assign(reason="Invalid status"),
    ])

    valid_ord = orders[~null_oid & ~unknown_cust & ~invalid_status]
    dup_oid = valid_ord["order_id"].duplicated(keep="first")
    rej_ord = pd.concat([rej_ord, valid_ord[dup_oid].assign(reason="Duplicate order_id")])
    valid_orders = valid_ord[~dup_oid]
    valid_order_ids = set(valid_orders["order_id"])

    # === ORDER ITEMS ===
    items = order_items_df.copy()
    items["quantity"] = pd.to_numeric(items["quantity"], errors="coerce").fillna(0).astype(int)
    items["unit_price"] = pd.to_numeric(items["unit_price"], errors="coerce").fillna(-1).round(2)
    items["sku"] = items["sku"].astype(str).str.strip()
    items["category"] = items["category"].str.strip().replace("", None)

    # Validate order items
    unknown_ord = ~items["order_id"].isin(valid_order_ids)
    bad_qty = items["quantity"] <= 0
    bad_price = items["unit_price"] < 0

    rej_items = pd.concat([
        items[unknown_ord].assign(reason="Unknown order_id"),
        items[bad_qty].assign(reason="Non-positive quantity"),
        items[bad_price].assign(reason="Negative price"),
    ])

    valid_items_mask = ~unknown_ord & ~bad_qty & ~bad_price
    valid_itm = items[valid_items_mask]
    dup_pk = valid_itm.duplicated(subset=["order_id", "line_no"], keep="first")
    rej_items = pd.concat([rej_items, valid_itm[dup_pk].assign(reason="Duplicate PK")])
    valid_items = valid_itm[~dup_pk]

    return valid_customers, valid_orders, valid_items, rej_cust, rej_ord, rej_items


# --- Load ---

def load_table(conn, table, columns, df):
    """Load DataFrame using TRUNCATE + COPY (idempotent full refresh)."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        if not df.empty:
            buf = StringIO()
            df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
            buf.seek(0)
            with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH CSV NULL '\\N'") as copy:
                copy.write(buf.read())
    log(f"  Loaded {len(df)} rows into {table}")


def load_rejected(conn, table, columns, df):
    """Load rejected rows using TRUNCATE + COPY (idempotent full refresh)."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
        if not df.empty:
            buf = StringIO()
            df[columns].astype(str).replace("nan", "").to_csv(buf, index=False, header=False)
            buf.seek(0)
            with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH CSV") as copy:
                copy.write(buf.read())
    return len(df)


# --- Main ---

def run():
    log("ETL Pipeline Started")
    start = time.perf_counter()

    try:
        # Ingest
        with log_step("Ingest"):
            customers_raw = pd.read_csv(f"{DATA_DIR}/customers.csv")
            orders_raw = pd.read_json(f"{DATA_DIR}/orders.json", lines=True)
            order_items_raw = pd.read_csv(f"{DATA_DIR}/order_items.csv")
            log(f"  Read {len(customers_raw)} customers, {len(orders_raw)} orders, {len(order_items_raw)} items")

        # Transform & Validate
        with log_step("Transform"):
            customers, orders, order_items, rej_cust, rej_ord, rej_items = transform(customers_raw, orders_raw, order_items_raw)
            log(f"  Valid: {len(customers)} customers, {len(orders)} orders, {len(order_items)} items")
            log(f"  Rejected: {len(rej_cust)} customers, {len(rej_ord)} orders, {len(rej_items)} items")

        # Load
        with log_step("Load"):
            with get_connection() as conn:
                load_table(conn, "customers", ["customer_id", "email", "full_name", "signup_date", "country_code", "is_active"], customers)
                load_table(conn, "orders", ["order_id", "customer_id", "order_ts", "status", "total_amount", "currency"], orders)
                load_table(conn, "order_items", ["order_id", "line_no", "sku", "quantity", "unit_price", "category"], order_items)

                rej_count = 0
                rej_count += load_rejected(conn, "rejected_customers", ["reason", "customer_id", "email", "full_name", "signup_date", "country_code", "is_active"], rej_cust)
                rej_count += load_rejected(conn, "rejected_orders", ["reason", "order_id", "customer_id", "order_ts", "status", "total_amount", "currency"], rej_ord)
                rej_count += load_rejected(conn, "rejected_order_items", ["reason", "order_id", "line_no", "sku", "quantity", "unit_price", "category"], rej_items)
                log(f"  Logged {rej_count} rejected rows")
                conn.commit()

        elapsed = time.perf_counter() - start
        log(f"ETL Complete in {elapsed:.2f}s")
        return 0

    except FileNotFoundError as e:
        log(f"ERROR: File not found - {e.filename}")
        return 1
    except psycopg.OperationalError as e:
        log(f"ERROR: Database connection failed - {e}")
        return 1
    except Exception as e:
        log(f"ERROR: {type(e).__name__} - {e}")
        return 1


if __name__ == "__main__":
    sys.exit(run())
