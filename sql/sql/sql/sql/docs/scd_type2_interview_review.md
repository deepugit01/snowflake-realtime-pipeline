# SCD Type 2 on Snowflake — Full Implementation & Interview Review

Automated, real-time SCD Type 2 pipeline: S3 → Snowpipe → Stream → Transaction-safe
Stored Procedure → Task. Built and tested end-to-end with 100+100 record batches.

---

## 1. What is SCD Type 2, and why does it exist?

**SCD (Slowly Changing Dimension)** describes how a data warehouse handles changes to
dimension data (like a bank account's status or balance) over time.

- **SCD Type 1** — overwrite the old value. No history. (`MERGE ... WHEN MATCHED THEN UPDATE`)
- **SCD Type 2** — preserve history. When a value changes, the old row is marked "expired"
  and a new row is inserted as the new "current" version. This lets you answer questions
  like *"what was this account's status on July 3rd?"* — impossible with Type 1, since the
  old value is destroyed on overwrite.

**Interview one-liner:** *"SCD Type 1 overwrites and loses history; SCD Type 2 preserves
every version of a row over time using valid_from/valid_to/is_current columns, so you can
reconstruct the state of the data at any past point in time."*

---

## 2. Table Structure

```sql
CREATE OR REPLACE TABLE OBM_DEV_OBM.INT.ACCOUNTS_SCD2 (
    account_id    INT,
    customer_id   INT,
    account_type  STRING,
    status        STRING,
    balance       NUMBER(12,2),
    branch        STRING,
    valid_from    TIMESTAMP,   -- when this version became true
    valid_to      TIMESTAMP,   -- when this version stopped being true (NULL = still current)
    is_current    BOOLEAN      -- fast flag, avoids checking "valid_to IS NULL" everywhere
);
```

**Why `is_current` in addition to `valid_to`?** Purely for query performance/readability —
`WHERE is_current = TRUE` is a simple boolean filter, versus `WHERE valid_to IS NULL`, which
works identically but is less immediately obvious to someone reading the query.

---

## 3. Architecture

```
S3 file upload (bank_accounts_dayN.json)
        │
        ▼
Snowpipe (AUTO_INGEST, event-driven)
        │
        ▼
RAW.ACCOUNTS_RAW_JSON (VARIANT landing table)
        │
        ▼
Stream (ACCOUNTS_RAW_JSON_STREAM) — tracks new rows since last read
        │
        ▼
Task (1-min schedule, WHEN stream has data)
        │
        ▼
Procedure LOAD_ACCOUNTS_SCD2() — wrapped in an explicit transaction:
   1. Snapshot stream → staging table (consumed exactly once)
   2. UPDATE: expire old current rows where a real value changed
   3. INSERT: new current version for changed accounts + brand new accounts
   4. COMMIT (or ROLLBACK everything, including the stream read, on any failure)
```

---

## 4. The Procedure — Full Code, Line by Line

```sql
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
```

### Line-by-line explanation

| Code | What it does |
|---|---|
| `BEGIN TRANSACTION;` | Groups every statement below into one all-or-nothing unit. Nothing commits individually until `COMMIT` is reached. |
| `TRUNCATE ... ACCOUNTS_BATCH_STAGING` | Clears out the previous batch's leftover data before loading a fresh one. |
| `INSERT INTO staging SELECT FROM stream` | Reads and consumes the stream **exactly once**, snapshotting the current batch of changes into a plain table. This avoids the multi-consumer race condition where reading the same stream twice could silently miss data. |
| **STEP 1 — UPDATE** | For every staging row, find the matching *current* row in `ACCOUNTS_SCD2`. If `status`, `balance`, or `branch` differs from what's in staging, close out that old row: set `valid_to` and flip `is_current` to `FALSE`. Rows where nothing changed are correctly skipped by this WHERE clause. |
| **STEP 2 — INSERT** | Uses a `LEFT JOIN` to detect two cases: (a) `target.account_id IS NULL` — a brand new account with no prior row at all, or (b) an existing account whose values genuinely differ. Either case gets a fresh row inserted as the new current version (`valid_to = NULL`, `is_current = TRUE`). |
| `COMMIT;` | Locks in everything above — staging, the UPDATE, the INSERT, and the stream consumption — all together, only if execution reaches this line. |
| `EXCEPTION WHEN OTHER THEN` | A safety net (like try/except) — catches any error from the steps above instead of letting the procedure crash silently. |
| `ROLLBACK;` | Undoes everything since `BEGIN TRANSACTION`, **including the stream read** — so a failure doesn't silently "lose" a batch of data from the stream's tracking. |
| `RETURN 'FAILED: ' \|\| SQLERRM;` | `SQLERRM` holds the actual error message; concatenating it into the return value surfaces the real failure reason immediately, without digging through TASK_HISTORY. |

---

## 5. Why Two Statements (UPDATE + INSERT) Instead of One MERGE?

This is one of the most likely interview follow-ups on this topic.

**Answer:** SCD Type 1 uses one `MERGE` because each row needs exactly one action — update
in place, or insert if new. SCD Type 2 needs a genuinely different row-per-key outcome: the
*same* incoming record needs to trigger **two separate actions** on the target table — expire
an old row AND insert a different new row. A single `MERGE` statement's `WHEN MATCHED` clause
can only take one action per matched row (UPDATE, DELETE, or nothing) — it cannot both update
the matched row AND separately insert an unrelated new row in the same clause. So SCD Type 2
pipelines conventionally use two explicit statements instead.

---

## 6. Real Bug Found & Fixed During This Build

**The bug:** the target table (`ACCOUNTS_SCD2`) didn't exist yet when the procedure first ran.
Step A (stream → staging) succeeded and committed on its own (in the pre-transaction version
of the procedure). The later UPDATE/INSERT steps then failed because the table didn't exist.
Because Step A had already committed independently, the stream was left in a "consumed" state
even though the data never actually reached the target table — effectively losing the batch
from the stream's perspective (though the raw data itself was still safe in the raw table).

**The fix:** wrapping the whole procedure in `BEGIN TRANSACTION` / `COMMIT`, with a `ROLLBACK`
in the exception handler, ensures the stream read is undone too if anything downstream fails —
the batch becomes available to the stream again on the next run instead of silently vanishing.

**Interview value:** this is a genuinely strong, specific answer to *"tell me about a bug you
debugged"* — it demonstrates understanding of stream offset semantics and transactional
integrity, not just SQL syntax.

---

## 7. Test Design & Validation

Two 100-record NDJSON batches were used to stress-test the logic:

- **Day 1** — 100 fresh accounts, all `ACTIVE`.
- **Day 2** — 100 records split into:
  - **60 unchanged** (identical values) → should create **zero** new versions — the key
    negative test, proving the change-detection logic doesn't blindly version everything
  - **~30 changed** (balance and/or status modified) → should expire the old row and insert
    a new current version
  - **10 brand new accounts** → should insert cleanly with no prior version to expire

**Actual result:** 135 total rows (100 + 35 new), 110 `is_current = TRUE`, 25
`is_current = FALSE`. The count of 25 (not 30) is explained by the test data's random change
logic occasionally producing accounts with no *actual* value difference despite being intended
as "changed" — the procedure correctly detected this and skipped them, which is the desired,
correct behavior, not a bug.

---

## 8. Useful Interview Queries on SCD Type 2 Data

**Get the full history of one record (very common interview ask):**
```sql
SELECT account_id, status, balance, branch, valid_from, valid_to, is_current
FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2
WHERE account_id = 3005
ORDER BY valid_from;
```

**Get the state of a record as of a specific past date:**
```sql
SELECT *
FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2
WHERE account_id = 3005
  AND valid_from <= '2026-07-03'
  AND (valid_to > '2026-07-03' OR valid_to IS NULL);
```

**Get only current, active state of all records (typical dashboard query):**
```sql
SELECT * FROM OBM_DEV_OBM.INT.ACCOUNTS_SCD2 WHERE is_current = TRUE;
```

---

## 9. Common Interview Questions on This Topic

**Q: What's the difference between SCD Type 1 and Type 2?**
A: Type 1 overwrites, no history. Type 2 preserves every version with valid_from/valid_to/
is_current, enabling point-in-time queries.

**Q: Why not just use Time Travel instead of building SCD Type 2 yourself?**
A: Time Travel has a retention limit (default 1 day, up to 90 on Enterprise+) and applies to
the *whole table's* query history — it's not designed for long-term, queryable, row-level
business history. SCD Type 2 gives permanent, indefinite history at the business-logic level,
independent of Time Travel's retention window.

**Q: How would you handle SCD Type 2 for a table with millions of rows changing daily?**
A: The staging + transaction-wrapped procedure pattern scales reasonably well since it only
processes the incremental batch (via the stream), not the whole table. For very high-volume
cases, clustering keys on `account_id` and `is_current` would help query/update performance.

**Q: What would break this design, and how would you harden it?**
A: If the source sends duplicate records for the same account within one batch with different
values, the UPDATE/INSERT logic as written could behave unexpectedly (matching the same row
multiple times). A more robust version would deduplicate staging by taking the latest
`updated_at` per `account_id` before the UPDATE/INSERT steps run.
