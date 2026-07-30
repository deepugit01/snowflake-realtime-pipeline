-- ============================================================
-- 12 — SCD Type 2: Task Scheduling + Validation Queries
-- ============================================================
CREATE OR REPLACE TASK OBM_DEV_OBM.INT.LOAD_ACCOUNTS_SCD2_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON_STREAM')
AS
CALL OBM_DEV_OBM.INT.LOAD_ACCOUNTS_SCD2();

ALTER TASK OBM_DEV_OBM.INT.LOAD_ACCOUNTS_SCD2_TASK RESUME;

-- ---------------- Validation queries ----------------

SELECT COUNT(*) FROM OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON;
SELECT COUNT(*) FROM OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON_STREAM;
SELECT COUNT(*) FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2;
SELECT COUNT(*) FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2 WHERE is_current = TRUE;
SELECT COUNT(*) FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2 WHERE is_current = FALSE;

-- Full history of a single account (common interview query)
SELECT account_id, status, balance, branch, valid_from, valid_to, is_current
FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2
WHERE account_id = 3005
ORDER BY valid_from;

-- State of a record as of a specific past date (point-in-time query)
SELECT *
FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2
WHERE account_id = 3005
  AND valid_from <= '2026-07-03'
  AND (valid_to > '2026-07-03' OR valid_to IS NULL);

-- Task run history / errors
SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(OBM_DEV_OBM.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'LOAD_ACCOUNTS_SCD2_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -2, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
