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
