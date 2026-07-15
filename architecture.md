# Architecture — Real-Time Snowflake Pipeline

## Overview

This pipeline ingests nested JSON order events from AWS S3 into Snowflake in near real-time,
applies change data capture, validates records against business rules, and lands the result
into three normalized tables — with invalid records routed to a dead-letter table instead of
being dropped or crashing the pipeline.

## Components

### 1. AWS S3 + Storage Integration

Files land in an S3 bucket. Snowflake authenticates to S3 via a **Storage Integration**, which
uses an IAM role trust policy rather than hardcoded access keys — Snowflake assumes a scoped
IAM role to read only the allowed S3 paths.

### 2. Snowpipe (event-driven auto-ingest)

An S3 **event notification** fires on every new object creation matching a prefix/suffix
filter, pushing a message to an SQS queue that Snowflake manages. Snowpipe listens on that
queue and loads the file into the raw table within seconds — no polling, no fixed schedule.

Files that existed in S3 *before* the event notification was configured require a one-time
manual `COPY INTO` to backfill, since Snowpipe only reacts to new arrivals.

### 3. Raw landing table (VARIANT)

`RAW.ORDERS_RAW_JSON` stores each JSON object as-is in a `VARIANT` column — no fixed schema,
so upstream format changes don't break ingestion. Parsing-level errors (malformed JSON) are
handled by `ON_ERROR = CONTINUE` on the pipe.

### 4. Stream (CDC)

A **Stream** on the raw table tracks every row change (insert/update/delete) since it was last
read. Streams have a single global offset — reading from the stream in a committed DML
statement advances that offset. This matters: if multiple independent procedures each try to
read the same stream directly, only the first to commit "wins" the data; the rest see nothing.

### 5. Staging table (consumed exactly once)

To avoid the multi-consumer race condition above, the stream is read **exactly once** per
pipeline run, into a plain staging table (`ORDERS_BATCH_STAGING`). All downstream validation
and fan-out logic then reads from staging, not from the stream directly — this keeps the batch
consistent across all three target tables and avoids any offset conflicts.

### 6. Validation & MERGE (the core procedure)

`LOAD_ALL_CLEAN()` does the following, in order:
1. Truncates and repopulates staging from the stream (one commit)
2. For each target (`ORDERS`, `CUSTOMERS`, `ORDER_ITEMS`), runs a `MERGE` against staging:
   - Matched + DELETE action → row is deleted
   - Matched + INSERT action with `ISUPDATE = TRUE` → row is updated
   - Not matched + INSERT action → row is inserted
3. Rows failing validation (missing required fields, invalid quantity/price) are inserted into
   `REJECTED_RECORDS` with a specific error reason instead of being silently dropped

**Note on JSON null handling:** a JSON literal `null` (e.g. `"order_id": null`) is stored as a
VARIANT null, not a SQL null. `column IS NULL` does not catch this on its own — validation
uses `IS_NULL_VALUE(column)` in addition to `IS NULL` everywhere required-field checks occur.

### 7. Nested array handling (LATERAL FLATTEN)

Order-level fields (`customer`, top-level order attributes) are single nested objects — direct
`:field` drill-down is sufficient. The `items` field is an array, and can contain a variable
number of elements per order. `LATERAL FLATTEN` explodes that array into one row per item,
which is why `ORDER_ITEMS` is matched on a composite key (`order_id`, `item_id`) rather than
`order_id` alone — one order can have many item rows.

### 8. Task (orchestration)

A **Task** runs on a 1-minute schedule but only actually executes when
`SYSTEM$STREAM_HAS_DATA(...)` evaluates true — avoiding wasted warehouse credits on empty runs.
It calls `LOAD_ALL_CLEAN()`.

## Data Quality Pattern (Dead-Letter / Rejected Records)

Two categories of "bad data" are handled differently:
- **Parsing/format errors** (malformed JSON, wrong type) — handled by Snowflake's built-in
  `ON_ERROR` option at the Snowpipe/COPY level.
- **Business rule violations** (missing required field, negative quantity/price) — not
  something Snowflake validates automatically; handled explicitly in the procedure, with
  failing rows routed to `REJECTED_RECORDS` along with a specific reason string.

This ensures no record is ever silently lost — every row either lands in a clean table or in
the rejected table with an explanation.

## Real Bugs Found & Fixed During Development

| Bug | Root Cause | Fix |
|---|---|---|
| Task failed: `USAGE privilege on warehouse` | Task's owner role lacked warehouse USAGE grant | Explicit `GRANT USAGE ON WAREHOUSE`, then re-`RESUME` (auto-suspend persists after the grant is fixed) |
| `METADATA$FILENAME` invalid identifier | Only valid when querying directly from a stage, not a regular table | Capture into a column at load time if lineage tracking is needed |
| Three separate procedures reading one stream silently missed data | Stream offset is global; first committed reader consumes it | Single stream, consumed once into a staging table; all fan-out reads from staging |
| `raw_data:order_id IS NULL` didn't catch `"order_id": null` | JSON null is a VARIANT null, not a SQL null | Added `IS_NULL_VALUE(...)` check alongside `IS NULL` |
| Clean tables didn't reflect updates/deletes | Plain `INSERT ... SELECT` instead of `MERGE` | Rewrote all three target loads as `MERGE` handling DELETE/UPDATE/INSERT |
| Snowpipe missed pre-existing S3 files | Snowpipe only reacts to new object-creation events | One-time manual `COPY INTO` backfill |

## Possible Future Improvements

- Zero-copy cloning for isolated dev/staging environments without duplicating storage
- Time Travel for point-in-time recovery / audit
- CI/CD pipeline (GitHub Actions) to deploy schema/procedure changes on merge to main
- Alerting on `REJECTED_RECORDS` growth rate via a scheduled monitoring task
