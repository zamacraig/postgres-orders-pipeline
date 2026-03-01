-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id      INTEGER PRIMARY KEY,
    customer_id   VARCHAR(20)    NOT NULL,
    product_id    VARCHAR(20)    NOT NULL,
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price   NUMERIC(10, 2) NOT NULL,
    order_date    DATE           NOT NULL,
    status        VARCHAR(20)    NOT NULL,
    loaded_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- Analytics view: revenue by customer
CREATE OR REPLACE VIEW v_revenue_by_customer AS
SELECT
    customer_id,
    COUNT(*)                    AS order_count,
    SUM(total_price)            AS total_revenue,
    AVG(total_price)            AS avg_order_value
FROM orders
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY total_revenue DESC;

-- Analytics view: revenue by product
CREATE OR REPLACE VIEW v_revenue_by_product AS
SELECT
    product_id,
    SUM(quantity)               AS units_sold,
    SUM(total_price)            AS total_revenue,
    AVG(unit_price)             AS avg_unit_price
FROM orders
WHERE status = 'completed'
GROUP BY product_id
ORDER BY total_revenue DESC;

-- Analytics view: daily order summary
CREATE OR REPLACE VIEW v_daily_order_summary AS
SELECT
    order_date,
    COUNT(*)                    AS total_orders,
    SUM(CASE WHEN status = 'completed'  THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN status = 'pending'    THEN 1 ELSE 0 END) AS pending_orders,
    SUM(CASE WHEN status = 'cancelled'  THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN status = 'completed' THEN total_price ELSE 0 END) AS daily_revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;
