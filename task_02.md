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
