CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     VARCHAR(10)  NOT NULL UNIQUE,
    age             INT,
    gender          VARCHAR(10),
    region          VARCHAR(100),
    loyalty_tier    VARCHAR(20),
    signup_date     DATE,
    email_opt_in    BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk      SERIAL PRIMARY KEY,
    product_id      VARCHAR(10)  NOT NULL UNIQUE,
    product_name    VARCHAR(255),
    category        VARCHAR(50),
    base_price      NUMERIC(10, 2),
    margin_pct      NUMERIC(5, 2),
    brand           VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_sk     SERIAL PRIMARY KEY,
    campaign_id     VARCHAR(10)  NOT NULL UNIQUE,
    campaign_name   VARCHAR(255),
    channel         VARCHAR(50),
    target_category VARCHAR(50),
    start_date      DATE,
    end_date        DATE,
    budget_usd      NUMERIC(12, 2),
    cpm             NUMERIC(6, 2),
    target_audience VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk         INT PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(20),
    week            INT,
    day_of_week     INT,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_sk  SERIAL PRIMARY KEY,
    transaction_id  VARCHAR(15) NOT NULL UNIQUE,
    customer_sk     INT REFERENCES dim_customer(customer_sk),
    product_sk      INT REFERENCES dim_product(product_sk),
    campaign_sk     INT REFERENCES dim_campaign(campaign_sk),
    date_sk         INT REFERENCES dim_date(date_sk),
    quantity        INT,
    unit_price      NUMERIC(10, 2),
    discount_pct    NUMERIC(5, 2),
    total_amount    NUMERIC(12, 2),
    channel         VARCHAR(20),
    ad_exposed      BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fact_customer  ON fact_transactions(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product   ON fact_transactions(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_campaign  ON fact_transactions(campaign_sk);
CREATE INDEX IF NOT EXISTS idx_fact_date      ON fact_transactions(date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_exposed   ON fact_transactions(ad_exposed);
