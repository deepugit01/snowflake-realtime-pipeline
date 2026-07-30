-- ============================================================
-- 10 — SCD Type 2: Target Table + Stream + Staging
-- ============================================================
CREATE OR REPLACE TABLE OBM_DEV_OBM.INT.ACCOUNTS_SCD2 (
    account_id    INT,
    customer_id   INT,
    account_type  STRING,
    status        STRING,
    balance       NUMBER(12,2),
    branch        STRING,
    valid_from    TIMESTAMP,   -- when this version became true
    valid_to      TIMESTAMP,   -- when this version stopped being true (NULL = still current)
    is_current    BOOLEAN      -- fast flag; equivalent to valid_to IS NULL
);

CREATE OR REPLACE STREAM OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON_STREAM
ON TABLE OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON;

CREATE OR REPLACE TABLE OBM_DEV_OBM.RAW.ACCOUNTS_BATCH_STAGING (
    raw_data VARIANT
);

-- Initial historical load (Day 1 style) — all rows are new, all become current versions
INSERT INTO OBM_DEV_OBM.INT.ACCOUNTS_SCD2
SELECT
    raw_data:account_id::INT,
    raw_data:customer_id::INT,
    raw_data:account_type::STRING,
    raw_data:status::STRING,
    raw_data:balance::NUMBER(12,2),
    raw_data:branch::STRING,
    raw_data:updated_at::TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON;
