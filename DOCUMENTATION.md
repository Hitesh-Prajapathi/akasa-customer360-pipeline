# Customer 360 Pipeline

A small, careful data pipeline built for the Akasa Air data engineering assessment. This page explains what it does, how to run it, how it works, and how it would grow in production.

## 1. The problem and what you get

An online store keeps its customers in three systems that never talked to each other.

| System | Format | Holds |
|---|---|---|
| CRM | JSON | who the customer is |
| Order management | CSV | what they spent |
| Support desk | XML | how often they called for help |

Each one names things differently (`cust_id`, `customerId`, `customer_id`). Nobody can see one customer in one place.

The pipeline reads all three, cleans them, checks the quality, and produces:

- **Customer 360**: one row per customer, all three systems joined (500 customers).
- **A data quality report**: what was wrong, and what we did about it.
- **Five business insights**: top 10 customers by revenue, Gold/Silver/Bronze segments, customers with more than 5 tickets, revenue by city, customer count by city.
- **A Streamlit dashboard** (data quality, revenue and customers by city, segments, top 10, high-support customers, a city filter) and **unit tests**.

"Revenue" means the order system's `lifetime_value`. That is our reading of the problem.

## 2. Quick start

**With Docker (works on any OS):**

```
docker compose run --rm pipeline      # runs the pipeline, writes output/ and logs/
docker compose up                     # dashboard at http://localhost:8501
```

**Without Docker (Python 3.12):**

```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py                # optional: --config path/to/other.json
streamlit run dashboard/app.py
pytest                                # first: pip install -r requirements-dev.txt
```

A full run takes under a second on this data (0.16 s on the author's laptop). If `output/` is missing, the dashboard runs the pipeline itself.

## 3. How it works

One command runs five steps. Each step reads only what the step before it wrote.

```
 CRM (JSON)    Orders (CSV)    Support (XML)         inputs, never modified
     |              |               |
     v              v               v   1. read as text, stamp file + row order
 output/raw/         exactly what was in the files
     |                                  2. rename to one vocabulary, 3. check each row
     v
 output/clean/        usable rows (broken values blanked)
 output/quarantine/   unusable rows, kept for review
     |                                  4. join on customer_id, nobody dropped
     v
 output/curated/      customer_360 (Parquet + JSON)
     |                                  5. business questions as SQL
     v
 output/reports/      dq_report, dq_issues, top 10, segments, city views
```

**Why raw / clean / curated?** The usual names (bronze, silver, gold) clash with the problem's Gold/Silver/Bronze customer segments. Using both would confuse everyone.

**What each module does** (all in `customer360/`):

| Module | One line |
|---|---|
| `readers.py` | One small reader class per format (JSON, CSV, XML), picked by file extension. |
| `standardize.py` | Renames each system's columns to the shared names. |
| `quality.py` | Flags every row's problems, splits clean from quarantine, builds the DQ report. |
| `merge.py` | Joins the three sources into Customer 360. |
| `insights.py` | The five business reports, each one short SQL query. |
| `pipeline.py` | Runs the steps in order and checks the books balance. |
| `logger.py` | Logs to the screen and to `logs/pipeline.log`. |

The work is done in SQL through DuckDB (an in-process analytics database). Only XML parsing and the dashboard use pandas.

## 4. Naming conventions and schema changes

Everything about column names lives in `config/pipeline.json`, not in the code.

- **Config mapping.** Each source lists its own column names and the standard name they become (`cust_id` and `customerId` both become `customer_id`).
- **Alias matching.** Names are first normalised: lowercase, with `_`, `-` and spaces removed. Then they are matched against a list of known variants. So `cust_id`, `customerId`, `c_id` and `CustomerID` all land on `customer_id`. An explicit mapping in the config always wins. If two columns in one file could both be the same standard column, the run fails loudly, because guessing is worse than stopping. Unknown extra columns are not an error. They are logged as warnings and ignored.

### Degrade, don't die

Real feeds change without warning. The aim is that one bad source does not stop the others. We tested it: rename the orders ID column to `C_ID` and every number stays the same. Remove it entirely and the run still finishes, with the orders batch quarantined and marked failed while CRM and support go through.

| What changed | What happens |
|---|---|
| A non-key column is missing | Filled with NULL, flagged, pipeline continues |
| The key column (`customer_id`) is missing | That source's whole batch goes to quarantine, like a dead-letter queue. The other sources still run. The status is reported. |
| An unknown extra column appears | Logged as a warning, ignored |

In a real stream, the same ideas appear as a schema registry (checks the shape of incoming data), a dead-letter queue (parks messages that fail), and a per-source circuit breaker (stops one failing feed from taking down the rest).

## 5. Data quality

We opened every file and counted before writing code. Seven problem rows were planted in the data (1 + 3 + 1 + 1 + 1). Here is what happens to each.

| # | Problem | Where | What the pipeline does |
|---|---|---|---|
| 1 | Exact duplicate customer (C0105 twice) | CRM | The later copy (row 501) is kept. The earlier one (row 105) goes to quarantine, labelled `exact`. |
| 2 | Email without `@` (C0125, C0250, C0375) | CRM | Customer kept, email set to NULL, flagged. Raw value stays in the issues file. |
| 3 | Order count empty (C0310) | Orders | Left NULL, flagged. Not guessed. |
| 4 | Lifetime value of -5,000 (C0420) | Orders | Set to NULL, flagged, left out of revenue totals. |
| 5 | Blank customer ID (8 tickets) | Support | Quarantined. A hint says "probably C0450". |

The three bad emails make the seven. The 35 warnings below are a separate, subtler find.

**Severity model.** Every problem has one of three levels:

- **Reject**: the row is unusable (duplicate copy, bad or blank ID). It goes to quarantine.
- **Field**: one value is broken. The customer stays, only that value becomes NULL, and a flag says why.
- **Warning**: values contradict each other. Everything is kept, and the issue is reported.

**The subtler findings.** These are the ones per-column checks miss:

- **35 customers have 0 tickets but a last ticket date.** Each value looks fine alone. Together they contradict. We keep both and raise a warning, because we cannot tell which is wrong.
- **C0250 is in the top 10 by revenue and has an invalid email.** Dropping every row with a bad email would have silently changed the top-10 report. This is why we blank the field instead of dropping the row.
- **The duplicate sits at the very end of the CRM file.** That is what a batch loaded twice looks like. It is the case "latest load wins" is designed for.
- **The orders file has Windows line endings (CRLF).** Harmless here, but it breaks naive readers.
- **268 of 500 customers have more than 5 tickets.** That is over half, so the list is not very selective. The threshold is in the config, so the business can tune it.

**The blank support ID.** The record sits between C0449 and C0451 in the file, and C0450 is the only customer with no support record. So it is very probably C0450's. We write `suggested_customer_id: C0450` in quarantine but never apply it. A wrong guess would corrupt data silently.

**Why nothing was imputed.** We checked whether spend predicts order count. The correlation is 0.003, which is nothing. A "smart" fill would just invent the average. With one gap in 500 rows, NULL plus a flag is honest. In other situations the order of preference would be: median or mode for a few gaps; KNN or regression when other columns carry real signal; MICE or MissForest (both fill gaps by modelling the other columns) for many interacting gaps. None applies here.

## 6. Duplicates

Every row is stamped on arrival with its source file, its row position, and a load time. After cleaning, each row also gets a content hash (a fingerprint of its business columns).

For each `customer_id`, the **latest loaded copy wins**. The rest go to quarantine with a type:

- **exact**: same fingerprint. Usually a batch loaded twice. Safe to ignore.
- **conflict**: same ID, different details. Needs a human.

The load time is the file's own timestamp, not the clock. That is what keeps reruns identical. `dedupe_keep` in the config can switch to "first".

## 7. How we know it is right

We wrote the expected numbers down before building. The output matches all of them.

| Check | Expected | Actual |
|---|---|---|
| Records read | 1501 | 1501 |
| Duplicates | 1 | 1 |
| Invalid emails | 3 | 3 |
| Missing fields | 1 | 1 |
| Negative revenue | 1 | 1 |
| Invalid customer IDs | 1 | 1 |
| Successful records | 1494 | 1494 |
| Customer 360 rows | 500 | 500 |
| Warnings | 35 | 35 |

- **Reconciliation.** For each file, rows read = clean rows + quarantined rows. CRM is 501 = 500 + 1, orders is 500 = 500 + 0, support is 500 = 499 + 1. The run stops with an error if this ever fails.
- **Reruns are byte-identical.** We checked with file hashes.
- **Counting rule.** A row can have several problems, so counts may overlap. Each bad row is counted once in `successful_records`. Warnings do not count against a row.
- **Tests.** 100 automated tests, all passing in about 2 seconds. They cover the readers, column-name matching and schema drift, each quality rule, duplicates, merge edge cases (a customer with no order row, a customer missing from the CRM), segment boundaries, every number above on the real data, and byte-identical reruns.

## 8. Outputs reference

| File | Contents |
|---|---|
| `output/raw/*.parquet` | Source rows exactly as read, plus file name and row order |
| `output/clean/*.parquet` | Typed rows with broken values blanked, plus `dq_issues` |
| `output/quarantine/*.parquet` | Rejected rows with their reasons, duplicate type, and suggested ID |
| `output/curated/customer_360.parquet` and `.json` | The Customer 360 dataset |
| `output/reports/dq_report.json` | Counts in the problem's format, plus per-source breakdown and reconciliation |
| `output/reports/dq_issues.json` | One entry per problem row: what was wrong and what we did |
| `output/reports/top_customers_by_revenue.json` | Top 10 by lifetime value |
| `output/reports/customer_segments.json`, `segment_summary.json` | Each customer's segment, and counts per segment |
| `output/reports/high_support_customers.json` | Customers with more than 5 tickets |
| `output/reports/revenue_by_city.json`, `customer_count_by_city.json` | The regional views |

**Customer 360 fields:** `customer_id, name, email, city, registration_date, total_orders, lifetime_value, last_order_date, support_tickets, last_ticket_date`, then `in_crm`, `in_orders`, `in_support` and `dq_flags`.

**How to read a NULL.** Check the presence flags first.

- `in_support = false`: the customer has no support record. NULL tickets is expected.
- `in_support = true` and tickets is NULL: a record exists but that field was broken. Look in `dq_flags`.

`dq_flags` lists every problem from any of the customer's records, plus `no_order_record` or `no_support_record` when a source is absent.

**Segments.** Gold is above 100,000. Silver is 50,000 to 100,000 inclusive. Bronze is below 50,000. Today that gives 318 Gold, 101 Silver, 80 Bronze, and 1 Unknown (C0420, whose revenue is NULL). Unknown is never forced into Bronze.

## 9. Design decisions and what we turned down

| Decision | Why | Turned down |
|---|---|---|
| DuckDB with SQL | The same queries move to Spark, Athena or Redshift at scale | pandas only: no scale story |
| Parquet in layers, JSON at the end | Parquet is compact and typed. JSON matches the shape the problem asks for. | CSV: cannot hold the `dq_flags` list |
| Full outer join with presence flags | Nobody is lost, and every NULL has an explanation | Inner join: would quietly drop customers |
| NULL, not imputation | Correlation of 0.003: filling would invent data | Filling with 0 or the average |
| `email-validator`, offline | Handles real-world email forms. Offline means fast and repeatable. | A hand-written pattern |
| Config-driven | Column names, rules, thresholds and paths change without touching code | Hardcoded names |
| Docker as the main way to run | We do not know the reviewer's machine | "It works on my laptop" |
| `BaseReader` classes | A new format is one new class, nothing else changes | One big if/else loader |

## 10. Scaling to production

At millions of rows, or around 5 TB a day, the logic stays and the machinery changes. The SQL carries over.

| Today (local) | Production (AWS) |
|---|---|
| Input files in a folder | S3 landing area, files dropped by each system |
| `output/raw`, `clean`, `curated` folders | S3 raw, clean and curated zones, partitioned by ingest date |
| DuckDB on one machine | Spark on Glue or EMR, or Athena or Redshift. Lambda for small files. |
| Run by hand or Docker | EventBridge schedule, or S3 events when a file lands |
| Full reload every run | Incremental loads with a watermark (only data newer than the last run) |
| Overwrite output files | MERGE (upsert) into the warehouse, so reruns never double-count |
| `logs/pipeline.log` | CloudWatch logs, metrics and alerts |
| Quarantine folder | A quarantine bucket or table with alerts when it grows |
| Batch files | Kinesis or Kafka, with a schema registry and a dead-letter queue |

## 11. Assumptions, limits, and what is next

**Assumptions**

- Revenue means `lifetime_value`.
- The CRM is the master customer list, though the join keeps customers found only in other systems and flags them (`in_crm = false`).
- The problem's sample names (Amit Sharma and others) do not appear in the real files. We treated them as illustrations.
- The sample report in the problem is a shape, not a formula. We added keys and explained the counting rule above.

**Known limitations**

- The suggested ID for a blank customer ID only works when the neighbours in the file are consecutive.
- The `>5 tickets` rule flags over half the customers, so it is a weak filter.
- It runs on one machine. The scale-out path is described, not built.

**What is next**

- Upsert into SQLite or a warehouse for idempotent loads
- Incremental runs with a watermark
- Data contracts per source
- A CI workflow
- Simulated streaming of new orders
- Deploying the dashboard to Streamlit Community Cloud
