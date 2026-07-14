-- ============================================================
-- 01 — S3 <-> Snowflake Storage Integration
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION S3_INT_SNOW
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-aws-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/Bank_dev/snowflake_fullload_dev_csv/');

-- Confirms the integration and gives you the AWS IAM user/external ID
-- needed to complete the trust policy on the AWS side
DESC INTEGRATION S3_INT_SNOW;

-- Optional: widen allowed paths later
-- ALTER STORAGE INTEGRATION S3_INT_SNOW
--   SET STORAGE_ALLOWED_LOCATIONS = (
--     's3://<bucket>/Bank_dev/snowflake_fullload_dev_csv/',
--     's3://<bucket>/Bank_dev/snowflake_fullload_dev_json/'
--   );
