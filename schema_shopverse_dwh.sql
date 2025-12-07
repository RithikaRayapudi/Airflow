-- Database

-- STAGING TABLES

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id   BIGINT,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(255),
    signup_date   TIMESTAMP,
    country       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id    BIGINT,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    unit_price    NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id        BIGINT,
    order_timestamp TIMESTAMP,
    customer_id     BIGINT,
    product_id      BIGINT,
    quantity        INTEGER,
    total_amount    NUMERIC(12,2),
    currency        VARCHAR(10),
    status          VARCHAR(50)
);

-- DIMENSION TABLES

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   BIGINT PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(255),
    signup_date   TIMESTAMP,
    country       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id    BIGINT PRIMARY KEY,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    unit_price    NUMERIC(12,2)
);

-- FACT TABLE

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id               BIGINT PRIMARY KEY,
    order_timestamp_utc    TIMESTAMP NOT NULL,
    order_date             DATE NOT NULL,
    customer_id            BIGINT NOT NULL,
    product_id             BIGINT NOT NULL,
    quantity               INTEGER NOT NULL,
    total_amount           NUMERIC(12,2) NOT NULL,
    currency               VARCHAR(10),
    currency_mismatch_flag SMALLINT DEFAULT 0,
    status                 VARCHAR(50),
    load_timestamp         TIMESTAMP DEFAULT NOW()
);

-- Optional indexes to speed up queries
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date
    ON fact_orders(order_date);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_id
    ON fact_orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_product_id
    ON fact_orders(product_id);
