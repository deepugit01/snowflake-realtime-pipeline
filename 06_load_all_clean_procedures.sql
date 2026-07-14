-- ============================================================
-- 06 — LOAD_ALL_CLEAN() — the core validation + MERGE procedure
-- ============================================================
-- Key design notes:
--   * IS_NULL_VALUE() is checked alongside IS NULL everywhere — a JSON literal
--     `null` is stored as a VARIANT null, not a SQL null, so `col IS NULL`
--     alone does NOT catch it. (Found via testing, not documentation.)
--   * Every target load uses MERGE, not plain INSERT — correctly handles
--     INSERT, UPDATE (delete+insert pair with ISUPDATE=TRUE), and DELETE.
--   * ORDER_ITEMS matches on (order_id, item_id) composite key, since that's
--     the real uniqueness at the line-item level.

CREATE OR REPLACE PROCEDURE OBM_DEV_OBM.INT.LOAD_ALL_CLEAN()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- STEP A: Consume the stream ONCE, snapshot into staging
    TRUNCATE TABLE OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING;

    INSERT INTO OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING (raw_data, metadata_action, metadata_isupdate)
    SELECT raw_data, METADATA$ACTION, METADATA$ISUPDATE
    FROM OBM_DEV_OBM.RAW.ORDERS_RAW_JSON_STREAM;

    -- STEP B: ORDERS (MERGE)
    MERGE INTO OBM_DEV_OBM.INT.ORDERS AS target
    USING (
        SELECT raw_data:order_id::INT AS order_id,
               raw_data:order_date::TIMESTAMP AS order_date,
               raw_data:status::STRING AS status,
               raw_data:customer:customer_id::INT AS customer_id,
               metadata_action, metadata_isupdate
        FROM OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING
        WHERE raw_data:order_id IS NOT NULL
          AND NOT IS_NULL_VALUE(raw_data:order_id)
          AND raw_data:customer:customer_id IS NOT NULL
          AND NOT IS_NULL_VALUE(raw_data:customer:customer_id)
    ) AS src
    ON target.order_id = src.order_id
    WHEN MATCHED AND src.metadata_action = 'DELETE' AND src.metadata_isupdate = FALSE THEN
        DELETE
    WHEN MATCHED AND src.metadata_action = 'INSERT' AND src.metadata_isupdate = TRUE THEN
        UPDATE SET order_date = src.order_date, status = src.status, customer_id = src.customer_id
    WHEN NOT MATCHED AND src.metadata_action = 'INSERT' THEN
        INSERT (order_id, order_date, status, customer_id)
        VALUES (src.order_id, src.order_date, src.status, src.customer_id);

    -- STEP C: CUSTOMERS (MERGE)
    MERGE INTO OBM_DEV_OBM.INT.CUSTOMERS AS target
    USING (
        SELECT DISTINCT raw_data:customer:customer_id::INT AS customer_id,
               raw_data:customer:name::STRING AS name,
               raw_data:customer:email::STRING AS email,
               raw_data:customer:country::STRING AS country,
               metadata_action, metadata_isupdate
        FROM OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING
        WHERE raw_data:customer:customer_id IS NOT NULL
          AND NOT IS_NULL_VALUE(raw_data:customer:customer_id)
          AND raw_data:customer:email IS NOT NULL
          AND NOT IS_NULL_VALUE(raw_data:customer:email)
    ) AS src
    ON target.customer_id = src.customer_id
    WHEN MATCHED AND src.metadata_action = 'DELETE' AND src.metadata_isupdate = FALSE THEN
        DELETE
    WHEN MATCHED AND src.metadata_action = 'INSERT' AND src.metadata_isupdate = TRUE THEN
        UPDATE SET name = src.name, email = src.email, country = src.country
    WHEN NOT MATCHED AND src.metadata_action = 'INSERT' THEN
        INSERT (customer_id, name, email, country)
        VALUES (src.customer_id, src.name, src.email, src.country);

    -- STEP D: ORDER_ITEMS (MERGE, composite key order_id + item_id)
    MERGE INTO OBM_DEV_OBM.INT.ORDER_ITEMS AS target
    USING (
        SELECT s.raw_data:order_id::INT AS order_id,
               item.value:item_id::INT AS item_id,
               item.value:product::STRING AS product,
               item.value:qty::INT AS qty,
               item.value:unit_price::NUMBER(10,2) AS unit_price,
               s.metadata_action, s.metadata_isupdate
        FROM OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING s,
        LATERAL FLATTEN(input => s.raw_data:items) AS item
        WHERE item.value:qty::INT > 0 AND item.value:unit_price::NUMBER(10,2) >= 0
    ) AS src
    ON target.order_id = src.order_id AND target.item_id = src.item_id
    WHEN MATCHED AND src.metadata_action = 'DELETE' AND src.metadata_isupdate = FALSE THEN
        DELETE
    WHEN MATCHED AND src.metadata_action = 'INSERT' AND src.metadata_isupdate = TRUE THEN
        UPDATE SET product = src.product, qty = src.qty, unit_price = src.unit_price
    WHEN NOT MATCHED AND src.metadata_action = 'INSERT' THEN
        INSERT (order_id, item_id, product, qty, unit_price)
        VALUES (src.order_id, src.item_id, src.product, src.qty, src.unit_price);

    -- STEP E: Rejected — order/customer level
    INSERT INTO OBM_DEV_OBM.RAW.REJECTED_RECORDS (source_table, raw_row, error_message)
    SELECT 'ORDERS_CUSTOMERS', raw_data,
        CASE
            WHEN raw_data:order_id IS NULL OR IS_NULL_VALUE(raw_data:order_id) THEN 'Missing order_id'
            WHEN raw_data:customer:customer_id IS NULL OR IS_NULL_VALUE(raw_data:customer:customer_id) THEN 'Missing customer_id'
            WHEN raw_data:customer:email IS NULL OR IS_NULL_VALUE(raw_data:customer:email) THEN 'Missing email'
        END
    FROM OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING
    WHERE metadata_action != 'DELETE'
      AND (
        raw_data:order_id IS NULL OR IS_NULL_VALUE(raw_data:order_id)
        OR raw_data:customer:customer_id IS NULL OR IS_NULL_VALUE(raw_data:customer:customer_id)
        OR raw_data:customer:email IS NULL OR IS_NULL_VALUE(raw_data:customer:email)
      );

    -- STEP F: Rejected — item level
    INSERT INTO OBM_DEV_OBM.RAW.REJECTED_RECORDS (source_table, raw_row, error_message)
    SELECT 'ORDER_ITEMS', item.value,
        CASE WHEN item.value:qty::INT <= 0 THEN 'Invalid quantity'
             ELSE 'Negative unit_price' END
    FROM OBM_DEV_OBM.RAW.ORDERS_BATCH_STAGING s,
    LATERAL FLATTEN(input => s.raw_data:items) AS item
    WHERE s.metadata_action != 'DELETE'
      AND (item.value:qty::INT <= 0 OR item.value:unit_price::NUMBER(10,2) < 0);

    RETURN 'Load complete';
END;
$$;
