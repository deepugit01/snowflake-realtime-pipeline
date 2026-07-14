-- ============================================================
-- 05 — Stream + Staging Table
-- ============================================================
-- A stream's offset can only be safely consumed once per batch (it advances
-- on first commit). We snapshot that one batch into a plain staging table,
-- then all downstream loads read from staging — avoiding any race condition
-- that would occur if multiple procedures each tried to read the stream directly.

CREATE OR REPLACE STREAM OBM_DEV_OBM.RAW.ORDERS_RAW_JSON_STREAM
ON TABLE OBM_DEV_OBM.RAW.ORDERS_RAW_JSON;

CREATE OR REPLACE TABLE OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING (
    raw_data          VARIANT,
    metadata_action   STRING,
    metadata_isupdate BOOLEAN
);
