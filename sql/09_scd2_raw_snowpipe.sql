-- ============================================================
-- 09 — SCD Type 2: Raw Table + Snowpipe (Accounts)
-- ============================================================
CREATE OR REPLACE TABLE OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON (
    raw_data   VARIANT,
    load_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE OBM_DEV_OBM.RAW.ACCOUNTS_JSON_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON (raw_data)
FROM @OBM_DEV_OBM.RAW.orders_json_stage
FILE_FORMAT = (FORMAT_NAME = OBM_DEV_OBM.RAW.JSON_FF)
PATTERN = '.*accounts.*[.]json'
ON_ERROR = 'CONTINUE';

-- Get the SQS ARN to wire into AWS S3 Event Notifications
SHOW PIPES LIKE 'ACCOUNTS_JSON_PIPE';
SELECT SYSTEM$PIPE_STATUS('OBM_DEV_OBM.RAW.ACCOUNTS_JSON_PIPE');

-- One-time catch-up load for files already sitting in S3 before Snowpipe was wired up
COPY INTO OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON (raw_data)
FROM @OBM_DEV_OBM.RAW.orders_json_stage
FILE_FORMAT = (FORMAT_NAME = OBM_DEV_OBM.RAW.JSON_FF)
PATTERN = '.*accounts.*[.]json';

-- Note: PURGE is NOT supported inside a PIPE definition, only on standalone COPY INTO.
