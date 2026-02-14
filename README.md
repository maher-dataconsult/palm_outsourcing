# Question 1 — Company Activity Pipeline & Data Model

---

## 1. Target Analytics Table

### Table: `fact_company_activity`

**Grain:** One row per **company per day**.

This grain allows analysts to track daily engagement trends, detect churn signals, and aggregate up to weekly/monthly views as needed.

| Column | Data Type | Source | Description |
|---|---|---|---|
| `company_id` | VARCHAR | CRM & API | Primary key (part 1) — Company identifier |
| `activity_date` | DATE | API `date` | Primary key (part 2) — Calendar date of activity |
| `company_name` | VARCHAR | CRM | Company display name |
| `country` | VARCHAR | CRM | Company country |
| `industry_tag` | VARCHAR | CRM | Industry classification |
| `last_contact_at` | TIMESTAMP | CRM | Most recent CRM contact date |
| `active_users` | INT | API | Daily active users |
| `events` | INT | API | Total events recorded that day |
| `7d_active_users` | INT | Derived | Rolling 7-day distinct active users (window function) |
| `is_churn_risk` | BOOLEAN | Derived | `TRUE` if zero events in last 14 days |
| `events_per_user` | FLOAT | Derived | `events / NULLIF(active_users, 0)` — engagement intensity |
| `days_since_last_contact` | INT | Derived | `DATEDIFF(day, last_contact_at, activity_date)` |
| `loaded_at` | TIMESTAMP | System | Pipeline load timestamp |

---

## 2. Sample SQL

Assumes two staging tables already landed:

- `stg_crm_companies` (from daily CRM CSV)
- `stg_product_usage` (from Product API)

```sql
-- fact_company_activity: one row per company per day
WITH daily_usage AS (
    SELECT
        company_id,
        date            AS activity_date,
        active_users,
        events
    FROM stg_product_usage
),

crm AS (
    SELECT
        company_id,
        name            AS company_name,
        country,
        industry_tag,
        last_contact_at
    FROM stg_crm_companies
),

joined AS (
    SELECT
        u.company_id,
        u.activity_date,
        c.company_name,
        c.country,
        c.industry_tag,
        c.last_contact_at,
        u.active_users,
        u.events,

        -- Derived metric 1: rolling 7-day active users
        SUM(u.active_users) OVER (
            PARTITION BY u.company_id
            ORDER BY u.activity_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS "7d_active_users",

        -- Derived metric 2: churn risk flag (no events in last 14 days)
        CASE
            WHEN SUM(u.events) OVER (
                PARTITION BY u.company_id
                ORDER BY u.activity_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) = 0 THEN TRUE
            ELSE FALSE
        END AS is_churn_risk,

        -- Derived metric 3: engagement intensity
        u.events / NULLIF(u.active_users, 0) AS events_per_user,

        -- Derived metric 4: days since last CRM contact
        DATEDIFF(day, c.last_contact_at, u.activity_date) AS days_since_last_contact,

        CURRENT_TIMESTAMP AS loaded_at

    FROM daily_usage u
    LEFT JOIN crm c
        ON u.company_id = c.company_id
)

SELECT * FROM joined;
```

---

## 3. ADF Flow Sketch

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    ADF PIPELINE: pl_company_activity_daily              │
│                    Trigger: Daily schedule (06:00 UTC)                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────────────────────────────────────────────────┐                   │
│  │  STAGE 1 — Ingest CRM CSV                       │                   │
│  │  Activity: Copy Data                             │                   │
│  │  Name: act_copy_crm_csv                          │                   │
│  │  Source: Azure Blob (crm/daily/*.csv)             │                   │
│  │  Sink: SQL DB / Lakehouse → stg_crm_companies    │                   │
│  └────────────────────┬────────────────────────────┘                   │
│                       │ on success                                      │
│                       ▼                                                 │
│  ┌─────────────────────────────────────────────────┐                   │
│  │  STAGE 2 — Ingest Product API                    │                   │
│  │  Activity: Azure Function / Web Activity         │                   │
│  │  Name: act_call_product_api                      │                   │
│  │  Calls: Python Function App (see Q1.4)           │                   │
│  │  Sink: Blob JSON → stg_product_usage             │                   │
│  └────────────────────┬────────────────────────────┘                   │
│                       │ on success                                      │
│                       ▼                                                 │
│  ┌─────────────────────────────────────────────────┐                   │
│  │  STAGE 3 — Transform & Load                      │                   │
│  │  Activity: Data Flow / Stored Procedure          │                   │
│  │  Name: df_build_fact_company_activity             │                   │
│  │  Logic: JOIN + window functions (SQL from Q1.2)  │                   │
│  │  Sink: Analytics DB → fact_company_activity       │                   │
│  └────────────────────┬────────────────────────────┘                   │
│                       │ on success                                      │
│                       ▼                                                 │
│  ┌─────────────────────────────────────────────────┐                   │
│  │  STAGE 4 — Validation & Alerting                 │                   │
│  │  Activity: Stored Procedure / Notebook           │                   │
│  │  Name: act_validate_row_counts                   │                   │
│  │  Checks: row count > 0, no null company_id       │                   │
│  └────────────────────┬────────────────────────────┘                   │
│                       │                                                 │
│              on failure (any stage)                                     │
│                       ▼                                                 │
│  ┌─────────────────────────────────────────────────┐                   │
│  │  ALERT — Web Activity / Logic App                │                   │
│  │  Name: act_send_failure_alert                    │                   │
│  │  Action: POST to Slack/Teams webhook             │                   │
│  │  Payload: pipeline name, stage, error message    │                   │
│  └─────────────────────────────────────────────────┘                   │
│                                                                        │
│  Naming Convention: pl_ (pipeline), act_ (activity),                   │
│                     df_ (data flow), stg_ (staging)                    │
└──────────────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- **API ingestion** is handled by an Azure Function (triggered by ADF Web Activity) so we get retry logic, timeout handling, and pagination outside ADF.
- **Failure alerts** are attached at the pipeline level using an "on failure" dependency so any stage failure triggers a notification.
- **Validation** runs as a final gate before the dashboard reads the data.

---

## 4. Python Pseudocode — Product API Ingestion

```python
import requests
import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# Configuration
API_BASE_URL = "https://api.example.com/v1/product-usage"
BLOB_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=..."
CONTAINER_NAME = "raw-data"
PAGE_SIZE = 100


def fetch_product_usage(start_date: str, end_date: str) -> list[dict]:
    """
    Call the product API for a given date range and return all records.
    Handles pagination automatically.
    """
    all_records = []
    page = 1
    has_more = True

    while has_more:
        response = requests.get(
            API_BASE_URL,
            params={
                "start_date": start_date,   # e.g. "2026-02-12"
                "end_date": end_date,         # e.g. "2026-02-13"
                "page": page,
                "page_size": PAGE_SIZE,
            },
            headers={"Authorization": "Bearer <TOKEN>"},
            timeout=30,
        )
        response.raise_for_status()  # Raise on 4xx / 5xx

        payload = response.json()
        records = payload.get("data", [])
        all_records.extend(records)

        # Check if there are more pages
        has_more = len(records) == PAGE_SIZE
        page += 1

    return all_records


def upload_to_blob(records: list[dict], target_date: str) -> str:
    """
    Write records as newline-delimited JSON to Azure Blob staging area.
    Returns the blob path.
    """
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_name = f"product_usage/{target_date}/usage_{target_date}.json"
    blob_client = blob_service.get_blob_client(CONTAINER_NAME, blob_name)

    ndjson = "\n".join(json.dumps(record) for record in records)
    blob_client.upload_blob(ndjson, overwrite=True)

    return blob_name


def main(start_date: str = None, end_date: str = None):
    """
    Entry point — defaults to yesterday's date if no range provided.
    Intended to be called by Azure Function triggered from ADF.
    """
    if not start_date:
        yesterday = datetime.utcnow() - timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
        end_date = start_date

    print(f"Fetching product usage: {start_date} to {end_date}")

    records = fetch_product_usage(start_date, end_date)
    print(f"Fetched {len(records)} records")

    if records:
        blob_path = upload_to_blob(records, start_date)
        print(f"Uploaded to blob: {blob_path}")
    else:
        print("No records found — skipping upload")

    return {
        "status": "success",
        "records_count": len(records),
        "date_range": f"{start_date} to {end_date}",
    }


if __name__ == "__main__":
    main()
```

---

## 5. 30-Minute Prioritization

> If you only have **30 minutes** before tomorrow's run, which **one part** would you implement first, and what would you explicitly postpone?

### I would implement first: **Stage 1 + Stage 2 (Ingestion)**
Specifically, the CRM CSV Copy Activity and the Python function for API ingestion, landing data into Blob/staging tables.

**Why ingestion first:**
- Without data landing, nothing downstream can work. The transform SQL and analytics table are useless without source data.
- Ingestion is the hardest part to retroactively backfill if it's missed — CRM CSVs may be overwritten daily, and API data may have retention windows.
- The SQL transformation (Stage 3) can be run manually or added in a follow-up pipeline within hours — the logic is already written.

### I would explicitly postpone:
- **Data Flow / Transform stage** — can be run as a manual SQL query against staging tables until automated.
- **Validation & alerting** — important but not blocking; we can visually verify the first few runs.
- **Dashboard connection** — stakeholders can wait 1 day for the automated view; a quick ad-hoc query can satisfy urgent needs.

# Question 2 — Performance, Data Quality & Communication

**Context — the existing query:**

```sql
SELECT company_id, date, SUM(events) AS events
FROM fact_events
GROUP BY company_id, date;
```

The daily job takes **3+ hours**, and analysts see mismatches between the dashboard and the **raw API** for a given `company_id` and `date`.

---

## 1. Three Optimisation Changes (Ranked 1→3)

### Rank 1 — Add a Clustered / Partition Key on `(date, company_id)`

**Why first:** This is the single highest-impact, lowest-risk change. The query filters and groups by `date` and `company_id`, so partitioning the table by `date` (and clustering/sort key by `company_id`) lets the engine skip irrelevant file segments entirely—often reducing scan volume by 90%+.

**One file to touch:** The table DDL or dbt model config — add `PARTITION BY (date)` and `CLUSTER BY (company_id)`.

```sql
-- Example for Fabric / Synapse / Snowflake
ALTER TABLE fact_events
  CLUSTER BY (date, company_id);
```

---

### Rank 2 — Incremental Load (Process Only New/Changed Data)

**Why second:** Even with good partitioning, scanning the entire table every day is wasteful. Convert the job to an **incremental model** that only processes records since the last successful run (e.g., `WHERE date >= CURRENT_DATE - INTERVAL '2 days'`). This reduces both compute cost and runtime dramatically.

**One file:** The transformation query or dbt model — add an `{% if is_incremental() %}` block or a `WHERE` filter on the load watermark.

```sql
-- Incremental approach
SELECT company_id, date, SUM(events) AS events
FROM fact_events
WHERE date >= CURRENT_DATE - INTERVAL '2 days'   -- only recent data
GROUP BY company_id, date;
```

---

### Rank 3 — Materialise the Aggregation as a Persistent Table / View

**Why third:** If analysts are hitting the raw `fact_events` table directly (millions of rows), creating a **materialised summary table** (`agg_company_daily_events`) that this query populates will make dashboard queries instant. This decouples ingestion latency from BI query performance.

**One file:** Create a new dbt model or scheduled view with `materialized='table'`.

---

**Order rationale:** Rank 1 gives the biggest bang with near-zero risk. Rank 2 compounds the gain by reducing scope. Rank 3 is the polish layer that protects downstream consumers.

---

## 2. Risks & Flaws in the SQL Snippet

### Flaw 1 — No `WHERE` Filter on Date Range (Full Table Scan)

```sql
-- Current: scans ALL historical data every run
SELECT company_id, date, SUM(events) AS events
FROM fact_events
GROUP BY company_id, date;
```

Every execution reads the full table. As volume grows, runtime scales linearly.

**Fix:** Add a date filter to process only the relevant window:

```sql
SELECT company_id, date, SUM(events) AS events
FROM fact_events
WHERE date >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY company_id, date;
```

---

### Flaw 2 — Possible Duplicate Events Inflating Counts

The mismatch analysts see between the dashboard and the raw API could be caused by duplicate records in `fact_events`. If the ingestion pipeline retries or replays data without deduplication, `SUM(events)` will overcount.

**Fix:** Either deduplicate at ingestion (using a unique key like `company_id + date + event_id`) or use a dedup CTE:

```sql
WITH deduped AS (
    SELECT DISTINCT company_id, date, event_id, events
    FROM fact_events
)
SELECT company_id, date, SUM(events) AS events
FROM deduped
GROUP BY company_id, date;
```

---

### Flaw 3 — `SUM(events)` Might Double-Count When Late-Arriving Data Appends

If the API sends corrections or late-arriving events, and the pipeline **appends** rather than **upserts**, the same day's data gets summed twice. This directly explains why dashboard numbers (aggregated) are **higher** than the raw API (single snapshot).

**Fix:** Use a `MERGE` / upsert pattern at the staging layer, or apply a `ROW_NUMBER()` window to take only the latest version per `(company_id, date)`.

---

## 3. Real Pipeline Performance Issue

### The Problem

In a previous project (Real-Time NYC Transit Monitoring), I had a pipeline ingesting MTA's GTFS real-time Protobuf feeds via AWS Lambda → Kinesis → TimeStream. After about two weeks of running, the Lambda execution time crept from ~3 seconds to **over 30 seconds**, occasionally timing out. The downstream Grafana dashboards started showing gaps.

### How I Debugged It

1. **CloudWatch Logs** — Checked Lambda invocation duration metrics and saw a clear upward trend correlated with growing payload size.
2. **Kinesis Iterator Age** — Noticed the iterator age spiking, confirming the consumer (Lambda writing to TimeStream) couldn't keep up.
3. **Root Cause** — The Lambda was deserialising the full Protobuf feed into a Python list, then writing records **one by one** to TimeStream. As MTA added more routes to the feed, the per-invocation record count grew from ~200 to ~1,200.
4. **Fix** — Refactored to use **batch writes** (TimeStream accepts up to 100 records per `write_records` call) and added **parallelism** by splitting the feed into chunks and using `concurrent.futures`. Execution time dropped back to ~4 seconds.

### Investigation Steps (Generalised)

- Check execution time/duration trends (CloudWatch, ADF Monitor, pipeline logs)
- Check data volume growth (row counts, file sizes over time)
- Profile the bottleneck: is it I/O (network, disk) or CPU (serialisation, transformation)?
- Look for N+1 patterns (single-record writes instead of batch)
- Test with a fixed, small dataset to isolate whether the issue is data-volume or code-related

---

## 4. Slack Update to Analytics Team

> 📣 **Pipeline Optimisation — Company Activity Dashboard**
>
> Hey team, quick update on the `fact_events` performance work:
>
> **What's changing:**
> - We're adding **partitioning by `date`** and **clustering by `company_id`** on `fact_events` — this should cut the daily job from 3+ hours to minutes
> - Converting the aggregation to an **incremental model** so we only process the last 2 days of data per run instead of the full history
> - Adding a **deduplication step** to fix the mismatch you've been seeing between dashboard numbers and the raw API
>
> **Temporary caveats:**
> - There will be a **one-time full backfill** when we deploy (may take ~1 hour during which the table is rebuilding)
> - The first run after deployment will still process all historical data; subsequent runs will be incremental
>
> **What to watch for:**
> - After deployment, please spot-check a few `company_id` + `date` combos against the raw API to confirm numbers now match
> - If you see any discrepancies, ping me with the `company_id` and `date` and I'll investigate immediately
>
> Rolling this out tomorrow morning (UTC). I'll post a ✅ once it's live. Let me know if you have questions!
