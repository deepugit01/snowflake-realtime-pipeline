# Real-Time Data Pipeline on Snowflake (S3 → Snowpipe → CDC → Validated Data Marts)

A production-style, event-driven data pipeline that ingests JSON order data from AWS S3 into
Snowflake in near real-time, applies change data capture (CDC), validates records against
business rules, and lands clean data into normalized tables — with a dead-letter pattern for
rejected records.

---

## Architecture

```
S3 file upload (event notification)
        │
        ▼
Snowpipe (AUTO_INGEST, event-driven, no polling)
        │
        ▼
RAW.ORDERS_RAW_JSON  (VARIANT — schema-flexible landing zone)
        │
        ▼
Stream  (tracks INSERT / UPDATE / DELETE since last read)
        │
        ▼
Task  (1-min schedule, runs only when the stream has data)
        │
        ▼
Stored Procedure — LOAD_ALL_CLEAN()
   ├─ Snapshot stream → staging table (consumed exactly once)
   ├─ MERGE → ORDERS        (validated: required fields present)
   ├─ MERGE → CUSTOMERS     (validated: required fields present)
   ├─ MERGE → ORDER_ITEMS   (validated: qty > 0, price ≥ 0, via LATERAL FLATTEN)
   └─ Invalid rows → REJECTED_RECORDS  (with a specific error reason)
```

## Why this project

Most beginner Snowflake tutorials stop at "load a CSV." This project instead simulates how a
real ingestion pipeline is built and operated: event-driven ingestion, incremental processing,
nested JSON normalization, and — critically — **error handling that doesn't silently drop bad
data**. Every design decision below was driven by a real bug found during development, not
just theory.

## Features

- **Event-driven ingestion** via Snowpipe + S3 event notifications + SQS — no polling, no
  fixed-interval batch jobs, near real-time (seconds, not minutes)
- **Secure S3 access** via a Storage Integration with an IAM role trust policy — no hardcoded
  AWS access keys
- **Change Data Capture** using Streams — the pipeline only processes what's new, not the full
  table on every run
- **Full CRUD support** — inserts, updates, and deletes are all correctly propagated using
  `MERGE`, not naive `INSERT`-only logic
- **Semi-structured data handling** — nested JSON is loaded into a `VARIANT` column and
  normalized into three relational tables using `LATERAL FLATTEN`
- **Data quality / dead-letter pattern** — records failing validation (missing required fields,
  invalid quantities, negative prices) are captured in a `REJECTED_RECORDS` table with the
  specific reason, instead of being silently dropped or crashing the pipeline
- **Orchestration via Snowflake Tasks** — fully automated, scheduled, condition-driven
  (`SYSTEM$STREAM_HAS_DATA`) execution

## Tech Stack

`AWS S3` · `AWS IAM` · `AWS SQS` · `Snowflake` (Storage Integration, Stages, Snowpipe, Streams,
Tasks, Stored Procedures, `VARIANT`/`LATERAL FLATTEN`) · `SQL`

## Repository Structure

```
├── README.md                          — this file
├── sql/
│   ├── 01_storage_integration.sql     — S3 ↔ Snowflake integration setup
│   ├── 02_database_stage_setup.sql    — database, schema, file format, stage
│   ├── 03_raw_tables_snowpipe.sql     — raw landing table + Snowpipe pipe
│   ├── 04_target_tables.sql           — clean tables + rejected_records table
│   ├── 05_stream_staging.sql          — stream + staging table
│   ├── 06_load_all_clean_procedure.sql — the core MERGE + validation procedure
│   ├── 07_task_scheduling.sql         — task automation
│   └── 08_validation_queries.sql      — debug / verification queries
├── sample_data/
│   └── orders_sample.json             — sample nested JSON test payload
└── docs/
    └── architecture.md                — full architecture write-up + bugs found/fixed
```

## Key Design Decisions & Real Bugs Fixed

| Decision / Bug | Reasoning |
|---|---|
| Single stream + staging table, not one stream per target table | A stream's offset is global — if 3 separate procedures each read the same stream independently, only the first to commit gets the data; the others see nothing. Snapshotting into a staging table first avoids this entirely. |
| `IS_NULL_VALUE()` used alongside `IS NULL` | A JSON literal `null` is stored as a VARIANT null in Snowflake, not a SQL null — `column IS NULL` alone does not catch it. Found via testing, not documentation. |
| `MERGE` instead of `INSERT` for all target loads | Naive `INSERT ... SELECT FROM stream` only handles new rows — updates silently fail to apply and deletes are never removed. `MERGE` on the stream's `METADATA$ACTION` / `METADATA$ISUPDATE` correctly handles all three. |
| `ORDER_ITEMS` matched on `(order_id, item_id)` composite key | A single order can contain multiple line items — matching on `order_id` alone would incorrectly treat every item update as the same row. |
| One manual catch-up `COPY INTO` for pre-existing S3 files | Snowpipe only reacts to *new* file-arrival events — files already sitting in the bucket before the event notification was configured need a one-time backfill load. |

## Sample Data

`sample_data/orders_sample.json` — nested order events (NDJSON, one JSON object per line):

```json
{"order_id": 5001, "customer": {"customer_id": 701, "name": "...", "email": "..."}, "items": [{"item_id": 1, "product": "...", "qty": 2, "unit_price": 15.50}]}
```

## How to Run This Yourself

1. Set up an AWS S3 bucket and IAM role (trust policy instructions in `docs/architecture.md`)
2. Run `sql/01` through `sql/07` in order in a Snowflake worksheet
3. Configure the S3 → SQS event notification using the ARN from `SHOW PIPES`
4. Upload a JSON file matching `sample_data/orders_sample.json`'s shape to your S3 stage path
5. Watch it auto-ingest within seconds — verify with `sql/08_validation_queries.sql`

## What I'd Add Next

- Zero-copy cloning for a dev/staging/prod environment split
- Time Travel for point-in-time recovery testing
- CI/CD via GitHub Actions (branch → PR → review → deploy pattern)
- Monitoring/alerting on `REJECTED_RECORDS` volume via a scheduled task + email notification

---

*Built as a hands-on learning project to understand real-time data engineering patterns on
Snowflake — including the debugging process, not just the finished code.*
