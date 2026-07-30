-- ============================================================
-- 11 — SCD Type 2: LOAD_ACCOUNTS_SCD2() Procedure
-- ============================================================
-- Wrapped in an explicit transaction: if any step fails, everything rolls
-- back atomically — including the stream read — so a downstream failure
-- can never silently "lose" a batch from the stream's tracking.
--
-- Uses two statements (UPDATE + INSERT) instead of a single MERGE, because
-- SCD Type 2 requires two distinct actions per changed record (expire the
-- old row AND insert a new one) — something a single MERGE's WHEN MATCHED
-- clause cannot express for the same matched row.

CREATE OR REPLACE PROCEDURE OBM_DEV_OBM.INT.LOAD_ACCOUNTS_SCD2()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    BEGIN TRANSACTION;

    TRUNCATE TABLE OBM_DEV_OBM.RAW.ACCOUNTS_BATCH_STAGING;

    INSERT INTO OBM_DEV_OBM.RAW.ACCOUNTS_BATCH_STAGING (raw_data)
    SELECT raw_data FROM OBM_DEV_OBM.RAW.ACCOUNTS_RAW_JSON_STREAM;

    -- STEP 1: Expire existing current rows where something actually changed
    UPDATE OBM_DEV_OBM.INT.ACCOUNTS_SCD2 target
    SET valid_to = src.raw_data:updated_at::TIMESTAMP,
        is_current = FALSE
    FROM OBM_DEV_OBM.RAW.ACCOUNTS_BATCH_STAGING src
    WHERE target.account_id = src.raw_data:account_id::INT
      AND target.is_current = TRUE
      AND (
            target.status   != src.raw_data:status::STRING
         OR target.balance  != src.raw_data:balance::NUMBER(12,2)
         OR target.branch   != src.raw_data:branch::STRING
      );

    -- STEP 2: Insert new current version for changed rows + brand new accounts
    INSERT INTO OBM_DEV_OBM.INT.ACCOUNTS_SCD2
        (account_id, customer_id, account_type, status, balance, branch, valid_from, valid_to, is_current)
    SELECT
        src.raw_data:account_id::INT,
        src.raw_data:customer_id::INT,
        src.raw_data:account_type::STRING,
        src.raw_data:status::STRING,
        src.raw_data:balance::NUMBER(12,2),
        src.raw_data:branch::STRING,
        src.raw_data:updated_at::TIMESTAMP,
        NULL,
        TRUE
    FROM OBM_DEV_OBM.RAW.ACCOUNTS_BATCH_STAGING src
    LEFT JOIN OBM_DEV_OBM.INT.ACCOUNTS_SCD2 target
        ON target.account_id = src.raw_data:account_id::INT
        AND target.is_current = TRUE
    WHERE target.account_id IS NULL
       OR target.status   != src.raw_data:status::STRING
       OR target.balance  != src.raw_data:balance::NUMBER(12,2)
       OR target.branch   != src.raw_data:branch::STRING;

    COMMIT;
    RETURN 'SCD2 load complete';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;
