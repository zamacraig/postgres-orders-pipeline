# postgres-orders-pipeline

A Python ETL pipeline that ingests, validates, and loads order data into PostgreSQL. Features pandas transformations, psycopg3 bulk loading, SQL analytics views, and data quality checks.

## Prerequisites

**Required:** Create a `.env` file before running the pipeline:

```bash
cp .env.example .env
```

The `.env` file contains database credentials and is excluded from git. See [Configuration](#configuration) for available settings.

## Quick Start

### Option 1: Local PostgreSQL (default)

```bash
cp .env.example .env            # Create .env file (if not done)
docker compose --profile local up --build
```

This starts a PostgreSQL container, initializes the schema, runs the ETL pipeline, and exits.

### Option 2: External Database

Connect to your own PostgreSQL database:

1. Create and configure `.env`:

```bash
cp .env.example .env
```

2. Edit `.env` with your database credentials:

```env
POSTGRES_DB=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
PGHOST=your-db-host.example.com
PGPORT=5432
```

3. Initialize the schema on your database:

```bash
psql -h $PGHOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/init.sql
```

4. Run the pipeline:

```bash
docker compose --profile external up --build
```

## Testing the Pipeline

### 1. Run the full pipeline

```bash
docker compose down -v                       # Clean slate (removes volumes)
docker compose --profile local up --build    # Build and run
```

Expected output:
```
pipeline-1  | ETL Pipeline Started
pipeline-1  | [Ingest] Starting...
pipeline-1  |   Read 6 customers, 10 orders, 12 items
pipeline-1  | [Ingest] Done (0.02s)
pipeline-1  | [Transform] Starting...
pipeline-1  |   Valid: 4 customers, 6 orders, 7 items
pipeline-1  |   Rejected: 2 customers, 4 orders, 6 items
pipeline-1  | [Transform] Done (0.08s)
pipeline-1  | [Load] Starting...
pipeline-1  |   Loaded 4 rows into customers
pipeline-1  |   Loaded 6 rows into orders
pipeline-1  |   Loaded 7 rows into order_items
pipeline-1  |   Logged 12 rejected rows
pipeline-1  | [Load] Done (0.35s)
pipeline-1  | ETL Complete in 0.45s
pipeline-1 exited with code 0
```

### 2. Query the database

Connect to the local database:

```bash
docker exec -it postgres-orders-pipeline-orders-db-1 psql -U postgres -d orders
```

Or connect to an external database:

```bash
psql -h $PGHOST -U $POSTGRES_USER -d $POSTGRES_DB
```

Verify loaded data:

```sql
SELECT COUNT(*) FROM customers;      -- 4
SELECT COUNT(*) FROM orders;         -- 6
SELECT COUNT(*) FROM order_items;    -- 7
```

Check rejection tables:

```sql
SELECT reason, COUNT(*) FROM rejected_customers GROUP BY reason;
SELECT reason, COUNT(*) FROM rejected_orders GROUP BY reason;
SELECT reason, COUNT(*) FROM rejected_order_items GROUP BY reason;
```

### 3. Test analytics views

```sql
SELECT * FROM v_daily_metrics;
SELECT * FROM v_top_customers_by_spend;
SELECT * FROM v_top_skus;
```

### 4. Test data quality views

```sql
SELECT * FROM v_dq_duplicate_emails;
SELECT * FROM v_dq_orphan_orders;
SELECT * FROM v_dq_invalid_order_items;
SELECT * FROM v_dq_invalid_status;
SELECT * FROM v_dq_rejection_summary;
```

### 5. Test idempotency

Run the pipeline twice — results should be identical:

```bash
docker compose up pipeline       # Run again
docker compose up pipeline       # And again
```

Query counts remain the same (TRUNCATE + COPY ensures full refresh).

## Project Structure

```
├── docker-compose.yml    # PostgreSQL + pipeline services
├── Dockerfile            # Python 3.12 image for pipeline
├── requirements.txt      # pandas, psycopg[binary]
├── data/
│   ├── customers.csv     # Test data with intentional errors
│   ├── orders.jsonl      # JSONL format orders
│   └── order_items.csv   # Order line items
├── pipeline/
│   └── etl.py            # Consolidated ETL pipeline
├── sql/
│   └── init.sql          # Schema, rejection tables, views
└── solution.md           # Design decisions documentation
```

## Test Data

The sample data includes intentional errors to exercise validation:

| Issue | Example |
|-------|---------|
| Uppercase emails | `ALICE@EXAMPLE.COM` (normalized) |
| Invalid emails | `not-an-email` (rejected) |
| Duplicate emails | Same email, different customer_id (rejected) |
| Invalid status | `processing` not in allowed set (rejected) |
| Unknown customer_id | Order references non-existent customer (rejected) |
| Unknown order_id | Item references non-existent order (rejected) |
| Non-positive quantity | `quantity <= 0` (rejected) |
| Negative price | `unit_price < 0` (rejected) |

## Configuration

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_DB` | Database name | `orders` |
| `POSTGRES_USER` | Database user | `postgres` |
| `POSTGRES_PASSWORD` | Database password | `postgres` |
| `PGHOST` | PostgreSQL host (external mode) | `localhost` |
| `PGPORT` | PostgreSQL port | `5432` |
| `DATA_DIR` | Path to data files (set by Docker) | `/app/data` |
