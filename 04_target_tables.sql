-- ============================================================
-- 04 — Target (Clean) Tables + Rejected Records
-- ============================================================
CREATE OR REPLACE TABLE OBM_DEV_OBM.INT.CUSTOMERS (
    customer_id INT,
    name        STRING,
    email       STRING,
    country     STRING
);

CREATE OR REPLACE TABLE OBM_DEV_OBM.INT.ORDERS (
    order_id     INT,
    order_date   TIMESTAMP,
    status       STRING,
    customer_id  INT
);

CREATE OR REPLACE TABLE OBM_DEV_OBM.INT.ORDER_ITEMS (
    order_id     INT,
    item_id      INT,
    product      STRING,
    qty          INT,
    unit_price   NUMBER(10,2)
);

CREATE OR REPLACE TABLE OBM_DEV_OBM.RAW.REJECTED_RECORDS (
    source_table    STRING,
    raw_row         VARIANT,
    error_message   STRING,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
