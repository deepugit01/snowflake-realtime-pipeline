-- ============================================================
-- 02 — Database, Schema, File Format, Stage
-- ============================================================
USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE OBM_DEV_OBM;
CREATE OR REPLACE SCHEMA OBM_DEV_OBM.RAW;
CREATE OR REPLACE SCHEMA OBM_DEV_OBM.INT;

CREATE OR REPLACE FILE FORMAT OBM_DEV_OBM.RAW.JSON_FF
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE OBM_DEV_OBM.RAW.orders_json_stage
  STORAGE_INTEGRATION = S3_INT_SNOW
  URL = 's3://<bucket>/Bank_dev/snowflake_fullload_dev_csv/'
  FILE_FORMAT = OBM_DEV_OBM.RAW.JSON_FF;

LIST @OBM_DEV_OBM.RAW.orders_json_stage;
