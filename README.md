# Customer 360 Data Pipeline

[![CI](https://github.com/Hitesh-Prajapathi/akasa-customer360-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Hitesh-Prajapathi/akasa-customer360-pipeline/actions/workflows/ci.yml) [![Live dashboard](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://akasa-customer360-pipeline-yleacgfug2v7qqljs5qdbp.streamlit.app/) ![Python 3.12](https://img.shields.io/badge/python-3.12-blue) ![Tests](https://img.shields.io/badge/tests-124%20passing-brightgreen) ![DuckDB](https://img.shields.io/badge/engine-DuckDB-yellow)

An e-commerce company keeps its customers in three systems that never talk to each other: a CRM (JSON), an order system (CSV) and a support desk (XML). I built a pipeline that ingests all three, cleans and checks them, merges them into **one trusted profile per customer**, and turns that into business insights on a live dashboard.

| | |
|---|---|
| **Live dashboard** | https://akasa-customer360-pipeline-yleacgfug2v7qqljs5qdbp.streamlit.app/ |
| **Code** | https://github.com/Hitesh-Prajapathi/akasa-customer360-pipeline |
| **CI runs** | https://github.com/Hitesh-Prajapathi/akasa-customer360-pipeline/actions |
| **Stack** | Python 3.12 · DuckDB (SQL) · Parquet · SQLite · Streamlit · Groq LLM · Docker · GitHub Actions · pytest |

---

## 1. The pipeline at a glance

```
 SOURCES             STAGING / RAW          TRANSFORM + VALIDATE          LAKE ZONES             WAREHOUSE          BI
┌──────────────┐    ┌──────────────┐     ┌───────────────────────┐    ┌───────────────┐     ┌────────────┐    ┌──────────────┐
│ CRM   (JSON) │──┐ │ raw/*.parquet│     │ standardise names     │    │ clean/        │     │ SQLite     │    │ Streamlit    │
│ Orders (CSV) │──┼▶│ as-is + file,│────▶│ quality rules + dedupe│───▶│ quarantine/   │────▶│ customer_  │───▶│ 4 pages +    │
│ Support (XML)│──┘ │ time, row no.│     │ merge on customer_id  │    │ curated/ 360  │     │ 360 upsert │    │ LLM chat     │
└──────────────┘    └──────────────┘     └───────────────────────┘    │ reports/      │     └────────────┘    └──────────────┘
                                                                       └───────────────┘
 stream batches ──▶ incoming/orders ──▶ same rules, only new files ──▶ upsert changed customers only
```
One command runs it all: `python run_pipeline.py`. Every step reads only the step before it, so any stage can be rebuilt or replayed.

---

## 2. How my pipeline maps to a standard data-engineering workflow

| Stage | What it does | My components (files) | In production I would use |
|---|---|---|---|
| **Data sources** | Where the data is born | `DE-Customer360/` (JSON, CSV, XML) · `stream/producer.py` simulates new order batches | Operational DBs, APIs, Kafka/Kinesis topics |
| **Staging / landing** | Keep an untouched copy of what arrived | `output/raw/*.parquet`, stamped with `_source_file`, `_ingested_at`, `_row_num` · `incoming/orders/` for stream files | S3 landing zone, partitioned by ingest date |
| **Extractor** | Read each format into one shape | `customer360/readers.py`: `BaseReader` → `JsonReader` / `CsvReader` / `XmlReader`, chosen by file extension | Glue / Spark readers, Debezium CDC |
| **Transformations** | Standardise, merge, aggregate | `standardize.py` (column aliases) · `merge.py` (full outer join) · `insights.py` (business SQL) | Spark SQL, or dbt models in the warehouse |
| **Validators** | Decide what can be trusted | `quality.py` (rules, severity, dedupe, `email-validator`) · reconciliation in `pipeline.py` · schema-drift guard · 124 tests in CI | Great Expectations / dbt tests, data contracts, alerts |
| **Loader** | Write results safely, without duplicates | `pipeline.py` (Parquet via DuckDB `COPY`) · `load.py` (SQLite **upsert** in one transaction) · `incremental.py` (stream loads) | `MERGE` into Redshift/Snowflake, Glue jobs |
| **Data lake** | Organised zones of files | `output/raw`, `clean`, `quarantine`, `curated`, `reports` | S3 + Iceberg/Delta tables, Athena on top |
| **Warehouse / pooling** | One queryable home for curated data | `output/warehouse/customer360.db`: `customer_360`, `load_runs` audit, `watermarks` | Redshift / Snowflake / BigQuery |
| **BI & serving** | Answer business questions | `dashboard/` (4 Streamlit pages + a Groq chat per page) · `output/reports/*.json` | QuickSight / Power BI / Metabase on a semantic layer |
| **Orchestration & ops** | Run, repeat, observe | `run_pipeline.py` · `run_stream.py` · `config/pipeline.json` · `logs/` · Docker Compose · GitHub Actions | Airflow / Step Functions, EventBridge triggers, CloudWatch |

I named the lake zones raw/clean/curated on purpose. The problem already uses Gold/Silver/Bronze for *customer segments*, and reusing the medallion names would confuse everyone.

---

## 3. ETL or ELT?

**Both, in different places.**
- **Inside the lake it's ELT.** I extract and load the raw data first, untouched. Then I transform it with SQL on top of the stored copy. That's why I can replay any run from `raw/`.
- **Into the warehouse it's ETL.** Only cleaned, validated, merged data is loaded into SQLite.
- **In production** I'd lean towards ELT at scale. Land everything in S3, transform with dbt or Spark SQL, and keep a light ETL step only where bad data must never reach the warehouse (PII or contract violations). My transforms are already plain SQL in DuckDB, so they move to Spark, Athena or Redshift almost unchanged.

---

## 4. What the data told me (EDA: `notebooks/01_eda.ipynb`)

I profiled every file before writing any pipeline code, and wrote down the numbers I expected the pipeline to produce.

**The 7 planted issues**

| Source | Issue | What I did |
|---|---|---|
| CRM | C0105 appears twice, identical | Kept the latest load, quarantined the copy as an *exact* duplicate |
| CRM | 3 emails without "@" (C0125, C0250, C0375) | Kept the customer, blanked the email, flagged it |
| Orders | C0310 has no order count | Left it blank and flagged it. Not imputed (see below) |
| Orders | C0420 revenue = −5000 | Blanked and flagged it, and excluded it from revenue totals |
| Support | One record with an empty customer ID (8 tickets) | Quarantined, with a hint: *probably C0450* (it sits between C0449 and C0451, and C0450 is the only customer with no support row) |

**The 5 easy-to-miss findings**
- **35 customers have 0 tickets but a "last ticket date".** Only a cross-field rule catches this, so I added it as a warning.
- **C0250 is a top-10 spender with a broken email.** Dropping bad-email rows would have silently changed the Top-10 report. That's why I blank fields instead of deleting rows.
- **The duplicate is the very last row of the file.** That's the classic sign of a batch loaded twice.
- **The orders CSV has Windows line endings.** It's harmless here, but it breaks naive readers.
- **268 of 499 customers have more than 5 tickets**, so that threshold isn't selective. I made it configurable.

**Why I didn't impute.** Order count and revenue have a correlation of **0.003**, so any model would just return the median and pretend it was data. With 1 gap out of 500, blank plus a flag is the honest choice. With real volumes and real signal I'd use median → KNN → MICE/MissForest.

**Result:** the pipeline matched my predictions exactly: 1501 records, 1 duplicate, 3 invalid emails, 1 missing field, 1 negative revenue, 1 invalid ID, **1494 successful, 500 customers**.

---

## 5. How I handled the ambiguities

| Ambiguity | My decision |
|---|---|
| The example report says 1500 records | The real files hold 1501 (the duplicate). I report the truth |
| "Revenue" isn't defined | Revenue = `lifetime_value` from the order system |
| Segment ranges overlap at 50,000 and 100,000 | Both boundaries count as Silver. Unknown revenue becomes segment "Unknown", not Bronze |
| Which system is the master list? | None is dropped. I used a full outer join, and every customer carries `in_crm`, `in_orders`, `in_support` |
| A blank value: unknown, or broken? | The presence flags plus a `dq_flags` list explain every blank |
| A row with several issues | Counts can overlap. "Successful" = rows with no error-level issue (warnings don't count against a row) |
| A broken field vs a broken row | **Severity model:** *reject* (quarantine the row), *field* (blank one value), *warning* (keep and report) |
| No ingestion timestamp in the data | I stamp each row with the file's own timestamp, so reruns stay byte-identical |
| "Orders missing" never occurs in the real data | The code handles it anyway, and a test proves it with synthetic data |
| Stream data with a bad value | Stricter than batch: the whole row is quarantined, so a bad value never overwrites a good one in the warehouse |

---

## 6. What each component does, in short

- **`readers.py`:** one small class per format, all returning the same raw shape. A new format means one new class.
- **`standardize.py`:** maps every system's column names to one vocabulary. Names are tidied first (lowercase, no `_ - space`), then matched against aliases in config. So `cust_id`, `customerId`, `c_id` and `Customer ID` all become `customer_id`. Two columns claiming the same field stops the run, because guessing is worse than stopping.
- **`quality.py`:** gives every row a list of issues, splits clean from quarantine, removes duplicates by arrival order (latest wins, labelled *exact* vs *conflict* via a content hash), and builds the DQ report.
- **`merge.py`:** builds the Customer 360 on `customer_id`, keeping everyone.
- **`insights.py`:** Top 10 by revenue, Gold/Silver/Bronze segments, customers with more than 5 tickets, revenue and customer count by city. One SQL query each.
- **`load.py`:** upserts into SQLite in a single transaction. A rerun reports *0 new, 0 changed, 500 unchanged*, and a crash midway rolls back completely.
- **`incremental.py` + `run_stream.py`:** process only new batch files (tracked by a watermark saved in the same transaction), so each file is applied exactly once.
- **`pipeline.py`:** runs the steps in order and checks *rows read = clean + quarantined* for every source. It stops if that ever fails.
- **`dashboard/`:** Overview, Pipeline flow, Customer lookup and Quality & risk pages, plus an LLM assistant on each page.

---

## 7. Extras beyond the brief

- **Degrade, don't die.**
  - A renamed column is matched automatically.
  - A missing non-key column is filled with blanks and flagged.
  - A missing ID column quarantines only *that* source's batch, and the other sources still go through.
- **"Ask about this page" (Groq, `openai/gpt-oss-120b`).**
  - The assistant sees only what's on the current page, plus a glossary. Arithmetic is precomputed in code, so the model explains numbers rather than calculating them.
  - Emails are never sent to the model, and names are sent only on the lookup page.
  - Three "Try asking" buttons per page get a reviewer started.
- **Business insight.** **171 Gold customers have more than 5 tickets, and they hold 54.7% of Gold revenue.** That's the group the business can least afford to lose.
- **Simulated streaming.** Micro-batches of order updates are processed incrementally with the same quality rules.
- **Warehouse upsert** with an audit table of every load.
- **124 tests**, CI on every push (lint, pipeline run, tests, Docker build), byte-identical reruns, and Docker for any OS.

---

## 8. If this went to production

| Component | One-line plan for real-world data (e.g. 5 TB/day) |
|---|---|
| Sources | Pull through CDC or event streams instead of files. Agree a data contract per source |
| Staging | S3 landing zone, partitioned by ingest date, immutable, with lifecycle rules |
| Extractor | Spark or Glue readers running in parallel across partitions. Schema registry for every feed |
| Transformations | The same SQL on Spark or dbt. Incremental models instead of full rebuilds |
| Validators | Great Expectations or dbt tests as gates. Alert on thresholds, and a circuit breaker per source |
| Loader | `MERGE` into the warehouse, idempotent and keyed on `customer_id`. Dead-letter queue for bad batches |
| Data lake | Iceberg/Delta tables for ACID and time travel, partitioned by date and city |
| Warehouse | Redshift or Snowflake, with clustering on `customer_id` and the date columns |
| BI | QuickSight or Power BI on a semantic layer, with row-level security for PII |
| LLM assistant | Retrieval over a metrics catalogue, prompt and response logging, and PII masking at the gateway |
| Orchestration | Airflow or Step Functions on EventBridge triggers. CloudWatch dashboards and alerts |

---

## 9. Run it

**Docker (any OS):**
```bash
docker compose run --rm pipeline        # run the pipeline → output/ and logs/
docker compose up                       # pipeline, then dashboard at http://localhost:8501
docker compose run --rm stream          # process new stream batches
```

**Plain Python 3.12:**
```bash
pip install -r requirements-dev.txt
python run_pipeline.py                  # batch run
streamlit run dashboard/app.py          # dashboard
python -m stream.producer --batches 5   # drop 5 order-update batches
python run_stream.py --once             # process only the new ones
pytest                                  # 124 tests
```

**LLM chat:** add `GROQ_API_KEY = "gsk_..."` to `.streamlit/secrets.toml` (never committed), or to the app's Secrets on Streamlit Cloud. Without a key, the dashboard still works and the chat says it's unavailable.

**Configuration:** `config/pipeline.json` holds the sources, column aliases, validation rules, segment thresholds and paths. Changing any of them needs no code change.

---

## 10. Project layout

```
config/pipeline.json     every rule, path and threshold
customer360/             the pipeline: readers · standardize · quality · merge · insights · load · incremental · pipeline
dashboard/               Streamlit app, 3 extra pages, LLM chat
stream/producer.py       simulated order stream
notebooks/01_eda.ipynb   the data investigation
tests/                   124 pytest tests
DE-Customer360/          input data + problem statement
run_pipeline.py · run_stream.py · Dockerfile · docker-compose.yml · .github/workflows/ci.yml
```
