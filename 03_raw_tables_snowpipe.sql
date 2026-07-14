-- ============================================================
-- 03 — Raw Landing Table + Snowpipe (Auto-Ingest)
-- ============================================================
CREATE OR REPLACE TABLE OBM_DEV_OBM.RAW.ORDERS_RAW_JSON (
    raw_data     VARIANT,
    load_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file  STRING
);

CREATE OR REPLACE PIPE OBM_DEV_OBM.RAW.ORDERS_JSON_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO OBM_DEV_OBM.RAW.ORDERS_RAW_JSON (raw_data)
FROM @OBM_DEV_OBM.RAW.orders_json_stage
FILE_FORMAT = (FORMAT_NAME = OBM_DEV_OBM.RAW.JSON_FF)
PATTERN = '.*orders.*[.]json'
ON_ERROR = 'CONTINUE';

-- Get the SQS ARN to wire into AWS S3 Event Notifications
SHOW PIPES LIKE 'ORDERS_JSON_PIPE';
SELECT SYSTEM$PIPE_STATUS('OBM_DEV_OBM.RAW.ORDERS_JSON_PIPE');

-- One-time catch-up load for files already sitting in S3 before Snowpipe was wired up
COPY INTO OBM_DEV_OBM.RAW.ORDERS_RAW_JSON (raw_data)
FROM @OBM_DEV_OBM.RAW.orders_json_stage
FILE_FORMAT = (FORMAT_NAME = OBM_DEV_OBM.RAW.JSON_FF)
PATTERN = '.*orders.*[.]json';

-- AWS side (console, not SQL):
-- S3 bucket -> Properties -> Event notifications -> Create:
--   Prefix = your folder path, Suffix = .json, Event = All object create events
--   Destination = SQS Queue -> paste the notification_channel ARN from SHOW PIPES above
