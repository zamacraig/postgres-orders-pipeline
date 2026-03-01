-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id   INTEGER PRIMARY KEY,
    email         TEXT           NOT NULL CHECK (email = LOWER(email)),
    full_name     TEXT,
    signup_date   DATE           NOT NULL,
    country_code  CHAR(2),
    is_active     BOOLEAN        NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_customers_email_lower UNIQUE (email)
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGINT PRIMARY KEY,
    customer_id   INTEGER        NOT NULL REFERENCES customers(customer_id),
    order_ts      TIMESTAMPTZ    NOT NULL,
    status        TEXT           NOT NULL CHECK (status IN ('placed', 'shipped', 'cancelled', 'refunded')),
    total_amount  NUMERIC(12, 2) NOT NULL,
    currency      CHAR(3)        NOT NULL
);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    order_id      BIGINT         NOT NULL REFERENCES orders(order_id),
    line_no       INTEGER        NOT NULL,
    sku           TEXT           NOT NULL,
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    category      TEXT,
    PRIMARY KEY (order_id, line_no)
);

-- ============================================================
-- REJECTION LOG TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS rejected_customers (
    id            SERIAL PRIMARY KEY,
    rejected_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    reason        TEXT           NOT NULL,
    customer_id   INTEGER,
    email         TEXT,
    full_name     TEXT,
    signup_date   TEXT,
    country_code  TEXT,
    is_active     TEXT
);

CREATE TABLE IF NOT EXISTS rejected_orders (
    id            SERIAL PRIMARY KEY,
    rejected_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    reason        TEXT           NOT NULL,
    order_id      BIGINT,
    customer_id   INTEGER,
    order_ts      TEXT,
    status        TEXT,
    total_amount  TEXT,
    currency      TEXT
);

CREATE TABLE IF NOT EXISTS rejected_order_items (
    id            SERIAL PRIMARY KEY,
    rejected_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    reason        TEXT           NOT NULL,
    order_id      BIGINT,
    line_no       INTEGER,
    sku           TEXT,
    quantity      TEXT,
    unit_price    TEXT,
    category      TEXT
);

-- ============================================================
-- ANALYTICS VIEWS
-- ============================================================

-- 1. Daily metrics
CREATE OR REPLACE VIEW v_daily_metrics AS
SELECT
    DATE(order_ts) AS date,
    COUNT(*) AS orders_count,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value
FROM orders
GROUP BY DATE(order_ts)
HAVING SUM(total_amount) > 0
ORDER BY date;

-- 2. Top 10 customers by lifetime spend
CREATE OR REPLACE VIEW v_top_customers_by_spend AS
SELECT
    c.customer_id,
    c.email,
    c.full_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS lifetime_spend
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.email, c.full_name
HAVING SUM(o.total_amount) > 0
ORDER BY lifetime_spend DESC
LIMIT 10;

-- 3. Top 10 SKUs by revenue and units sold
CREATE OR REPLACE VIEW v_top_skus AS
SELECT
    oi.sku,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
GROUP BY oi.sku
ORDER BY revenue DESC, units_sold DESC
LIMIT 10;

-- ============================================================
-- DATA QUALITY VIEWS (Required)
-- ============================================================

-- Duplicate customers by lowercase email (from rejection log)
CREATE OR REPLACE VIEW v_dq_duplicate_emails AS
SELECT rejected_at, customer_id, email, full_name, signup_date
FROM rejected_customers
WHERE reason = 'Duplicate email';

-- Orders referencing missing customers (from rejection log)
CREATE OR REPLACE VIEW v_dq_orphan_orders AS
SELECT rejected_at, order_id, customer_id, order_ts, status, total_amount, currency
FROM rejected_orders
WHERE reason = 'Unknown customer_id';

-- ============================================================
-- DATA QUALITY VIEWS (Optional)
-- ============================================================

-- Order items with non-positive quantities or unit prices
CREATE OR REPLACE VIEW v_dq_invalid_order_items AS
SELECT rejected_at, order_id, line_no, sku, quantity, unit_price, category, reason
FROM rejected_order_items
WHERE reason IN ('Non-positive quantity', 'Negative price');

-- Orders with status outside allowed set
CREATE OR REPLACE VIEW v_dq_invalid_status AS
SELECT rejected_at, order_id, customer_id, order_ts, status, total_amount, currency
FROM rejected_orders
WHERE reason = 'Invalid status';

-- Summary of all rejections
CREATE OR REPLACE VIEW v_dq_rejection_summary AS
SELECT 'customers' AS table_name, reason, COUNT(*) AS count
FROM rejected_customers GROUP BY reason
UNION ALL
SELECT 'orders', reason, COUNT(*)
FROM rejected_orders GROUP BY reason
UNION ALL
SELECT 'order_items', reason, COUNT(*)
FROM rejected_order_items GROUP BY reason
ORDER BY table_name, reason;