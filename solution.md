# Database Schema Design Decisions

## Overview

This document outlines the design decisions made for the PostgreSQL orders pipeline database schema.

## Tables

### customers

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `customer_id` | INTEGER | PRIMARY KEY | Unique identifier for each customer |
| `email` | TEXT | NOT NULL, UNIQUE, CHECK | Must be lowercase; validated via `CHECK (email = LOWER(email))` |
| `full_name` | TEXT | nullable | Name may not always be provided at signup |
| `signup_date` | DATE | NOT NULL | Required to track customer acquisition |
| `country_code` | CHAR(2) | nullable | ISO 3166-1 alpha-2 code; may be unknown for some customers |
| `is_active` | BOOLEAN | NOT NULL DEFAULT TRUE | Tracks active/inactive customer status |

**Design Decisions:**
- Email uniqueness enforced via `CONSTRAINT uq_customers_email_lower UNIQUE (email)` to prevent duplicate accounts
- Email lowercase enforced via `CHECK (email = LOWER(email))` — rejects non-lowercase input; **ETL is responsible for normalizing emails before insert**
- `full_name` is nullable because users may register with email only
- `country_code` is nullable to accommodate customers whose location is unknown
- `is_active` defaults to TRUE for new customers

### orders

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `order_id` | BIGINT | PRIMARY KEY | BIGINT to support high-volume order systems |
| `customer_id` | INTEGER | NOT NULL, FK → customers | Enforces referential integrity |
| `order_ts` | TIMESTAMPTZ | NOT NULL | Timezone-aware timestamp for global operations |
| `status` | TEXT | NOT NULL, CHECK | Restricted to: `placed`, `shipped`, `cancelled`, `refunded` |
| `total_amount` | NUMERIC(12,2) | NOT NULL | Supports values up to 9,999,999,999.99 |
| `currency` | CHAR(3) | NOT NULL | ISO 4217 currency code (e.g., USD, EUR) |

**Design Decisions:**
- `BIGINT` for `order_id` to handle large-scale e-commerce volumes
- `TIMESTAMPTZ` preserves timezone information for accurate global reporting
- CHECK constraint on `status` enforces valid order lifecycle states
- `NUMERIC(12,2)` provides precise decimal arithmetic for financial calculations
- All columns are NOT NULL as they are essential for order processing

### order_items

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `order_id` | BIGINT | NOT NULL, FK → orders | Part of composite PK |
| `line_no` | INTEGER | NOT NULL | Line number within an order; part of composite PK |
| `sku` | TEXT | NOT NULL | Stock Keeping Unit identifier |
| `quantity` | INTEGER | NOT NULL, CHECK > 0 | Must be positive |
| `unit_price` | NUMERIC(12,2) | NOT NULL, CHECK >= 0 | Price at time of order |
| `category` | TEXT | nullable | Product category; may not always be assigned |

**Design Decisions:**
- Composite primary key `(order_id, line_no)` uniquely identifies each line item
- `quantity` CHECK constraint prevents zero or negative quantities
- `unit_price` CHECK constraint allows zero (for free items) but prevents negative values
- `category` is nullable because products may not always have an assigned category

## Relationships

```
customers (1) ──────< (N) orders (1) ──────< (N) order_items
```

- One customer can have many orders
- One order can have many order items
- Foreign keys enforce referential integrity with cascading prevented (default behavior)

## Indexes

PostgreSQL automatically creates indexes for PRIMARY KEY and UNIQUE constraints:

| Table | Index | Created by |
|-------|-------|------------|
| `customers` | `customer_id` | PRIMARY KEY |
| `customers` | `email` | UNIQUE constraint |
| `orders` | `order_id` | PRIMARY KEY |
| `order_items` | `(order_id, line_no)` | Composite PRIMARY KEY |

## Email Normalization Strategy

**Approach:** CHECK constraint with ETL normalization

```sql
email TEXT NOT NULL CHECK (email = LOWER(email))
```

**Rationale:**
- Separates concerns: ETL handles data transformation, database enforces data integrity
- Simple implementation with no additional database objects (no triggers or functions)
- Makes ETL bugs visible immediately — if normalization fails, the insert fails
- Keeps the database schema declarative and easy to understand

## Nullability Strategy

**NOT NULL columns** (business-critical):
- Primary keys and foreign keys
- Email (required for customer identification)
- Timestamps (required for audit/tracking)
- Financial amounts (required for calculations)
- Status fields (required for business logic)
- Quantity and SKU (required for order fulfillment)

**Nullable columns** (optional data):
- `full_name` - may not be collected
- `country_code` - may be unknown
- `category` - may not be assigned to all products

## Rejection Log Tables

Invalid records are quarantined to rejection tables for audit and investigation:

| Table | Purpose |
|-------|---------|
| `rejected_customers` | Invalid customer records |
| `rejected_orders` | Invalid order records |
| `rejected_order_items` | Invalid order item records |

Each table includes:
- `id` - Auto-incrementing primary key
- `rejected_at` - Timestamp of rejection (default: NOW())
- `reason` - Why the record was rejected
- All original columns as TEXT (to preserve malformed data)

## Data Validation & Cleaning (ETL)

| Issue | Approach | Reason Column |
|-------|----------|---------------|
| **Emails** | Normalize to lowercase via `str.lower()` | — |
| **Datetimes** | Parse and convert to UTC via `pd.to_datetime(..., utc=True)` | — |
| **Numerics** | Cast to appropriate types (`float`, `int`) with rounding | — |
| **Duplicate emails** | Keep earliest `signup_date`, quarantine duplicates | `Duplicate email` |
| **Invalid emails** | Quarantine (regex validation) | `Invalid email` |
| **Invalid status** | Quarantine (not in allowed set) | `Invalid status` |
| **Unknown customer_id** | Quarantine (referential integrity) | `Unknown customer_id` |
| **Unknown order_id** | Quarantine (referential integrity) | `Unknown order_id` |
| **Non-positive quantity** | Quarantine | `Non-positive quantity` |
| **Negative unit_price** | Quarantine | `Negative price` |

**Why quarantine instead of fix/filter?**
- Preserves original data for investigation
- Makes data quality issues visible
- Supports audit requirements
- Avoids silent data loss or incorrect assumptions

## Data Quality Views

### Required Views

| View | Description |
|------|-------------|
| `v_dq_duplicate_emails` | Customers rejected for duplicate email |
| `v_dq_orphan_orders` | Orders rejected for unknown customer_id |

### Optional Views

| View | Description |
|------|-------------|
| `v_dq_invalid_order_items` | Items with non-positive qty or negative price |
| `v_dq_invalid_status` | Orders with invalid status values |
| `v_dq_rejection_summary` | Counts by table and rejection reason |

## Analytics Views

| View | Description |
|------|-------------|
| `v_daily_metrics` | Daily orders_count, total_revenue, average_order_value |
| `v_top_customers_by_spend` | Top 10 customers by lifetime spend |
| `v_top_skus` | Top 10 SKUs by revenue and units sold |

## Configuration

ETL is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_DIR` | Path to data files | — |
| `PGHOST` | PostgreSQL host | `localhost` |
| `PGPORT` | PostgreSQL port | `5432` |
| `PGDATABASE` | Database name | `orders` |
| `PGUSER` | Database user | `postgres` |
| `PGPASSWORD` | Database password | — |

## Logging

ETL emits structured logs:
- Step start/end with duration (`[Ingest]`, `[Transform]`, `[Load]`)
- Row counts: read, valid, rejected
- Total execution time

## Idempotency

The pipeline is fully idempotent — running it multiple times produces the same result.

### Load Strategy (TRUNCATE + COPY)

All tables use the same idempotent full-refresh approach:

| Table | Strategy |
|-------|----------|
| `customers` | TRUNCATE CASCADE, then COPY |
| `orders` | TRUNCATE CASCADE, then COPY |
| `order_items` | TRUNCATE CASCADE, then COPY |
| `rejected_customers` | TRUNCATE, then COPY |
| `rejected_orders` | TRUNCATE, then COPY |
| `rejected_order_items` | TRUNCATE, then COPY |

**Why full refresh?**
- Simple and SQL-free (no complex INSERT ON CONFLICT logic)
- Guarantees consistency with source data
- Handles deletes automatically (records removed from source are removed from DB)
- Fast via PostgreSQL COPY protocol

### Bulk Loading Implementation

Data is loaded using pandas `to_csv()` into an in-memory `StringIO` buffer, then sent via psycopg's `COPY ... WITH CSV`:

```python
buf = StringIO()
df[columns].to_csv(buf, index=False, header=False)
buf.seek(0)
with cur.copy(f"COPY {table} (...) FROM STDIN WITH CSV") as copy:
    copy.write(buf.read())
```

**Benefits:**
- No Python row loops — bulk transfer in one operation
- In-memory only — no disk I/O on the Python side
- PostgreSQL handles CSV parsing natively