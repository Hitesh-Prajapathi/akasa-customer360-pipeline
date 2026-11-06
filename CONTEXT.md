# CONTEXT.md: Akasa Air DE Assessment, Customer 360 Pipeline

> Single source of truth for me and any coding agent. Read it first every session.
> Update it BEFORE re-prompting whenever anything changes (section 9 log).

## 0. Fixed facts

- 3h slot = 30 min start buffer + 120 min build + 30 min end buffer. Any agentic AI tool allowed.
- They grade whether I can **explain my own code**. If I can't explain a block, simplify it.
- Evaluation: DSA, problem solving, OOP design, API/DB design, scalability/reliability/security, GenAI, communication.
- Rule: simple, correct, reproducible, explained > ambitious, half-working.
- Separate 2:00–3:00 assessment (DSA/SQL/OOP/GenAI) is not part of this build.

## 1. The problem (summary of `DE-Customer360/De-Customer360-problem.docx`)

An e-commerce company has customer data in 3 independent systems. Build a pipeline that:

| Step | Requirement | Deliverable |
|---|---|---|
| 1 | Load JSON, CSV, XML | readers for all 3 formats |
| 2 | Standardise schemas (`cust_id`, `customerId` → `customer_id`) | one canonical schema |
| 3 | DQ checks: duplicates, invalid emails, missing mandatory fields, negative revenue, invalid customer IDs | DQ report JSON with keys `total_records, duplicates, invalid_emails, missing_fields, successful_records` (we may add more keys) |
| 4 | Merge on `customer_id` | Customer 360 dataset, field order as in the expected JSON |
| 5 | Insights | Top 10 by revenue · segments Gold >100000 / Silver 50000–100000 / Bronze <50000 · customers with >5 tickets · revenue by city · customer count by city |
| Brownie | Config-driven (JSON `input_sources`), logging, unit tests, Streamlit dashboard (revenue by city, segments, DQ metrics) | |

Target Customer 360 record:
`customer_id, name, email, city, registration_date, total_orders, lifetime_value, last_order_date, support_tickets, last_ticket_date`

"Revenue" means `lifetime_value` (assumption, goes in the README).

**Naming clash:** the problem uses Gold/Silver/Bronze as *customer segments*. Pipeline layers are therefore called **raw / clean / curated**, never bronze/silver/gold.

## 2. Data profile (measured, not assumed)

Tiny data (~150 KB total). We still use DuckDB/SQL (D1) so the design carries over to the 5 TB/day scenario.

| Source | File | Rows | ID field | Planted defects found |
|---|---|---|---|---|
| CRM | `crm_customers.json` (list of flat objects) | 501 | `customer_id` | 1 exact duplicate (C0105 ×2) · 3 invalid emails, missing `@` (C0125, C0250, C0375) |
| Orders | `orders.csv` | 500 | `cust_id` | C0310 `total_orders` empty · C0420 `lifetime_value` = -5000 |
| Support | `support_tickets.xml` (`<customers><customer>…`) | 500 | `customerId` | 1 record with empty `<customerId />` (8 tickets, 2026-07-03) → C0450 has no support row |

Other facts:
- All IDs otherwise match `^C\d{4}$`, C0001–C0500. 500 unique customers.
- No orphans: every orders/support ID exists in the CRM.
- Every CRM customer has an order row, so the "orders missing" challenge shows up only as C0310's missing field. The code must still handle customers with no order row, so this needs a test with synthetic data.
- Dates are clean ISO `YYYY-MM-DD`. 8 cities. Tickets range 0–12. LTV max is 249,919.
- The docx sample records (Amit Sharma etc.) do **not** match the real files ("Priya Nair 1", user1@example.com). Treat the samples as illustrative only.

**Acceptance targets.** If the M1 output doesn't show these numbers, there's a bug:

| Metric | Expected |
|---|---|
| total_records (sum of all source rows) | 1501 |
| duplicates | 1 |
| invalid_emails | 3 |
| missing_fields | 1 (C0310). The blank XML ID counts once, under invalid_customer_ids |
| negative_revenue | 1 |
| invalid_customer_ids | 1 |
| successful_records | 1494 (1501 − 7 issues, each counted once) |
| Customer 360 rows | 500 |

## 3. Milestones

### M1, Baseline: ✅ DONE, committed as `v1-baseline`

Verified: all section 2 numbers match, warnings = 35, rerun is byte-identical, `docker compose run --rm pipeline` works.

Actual module layout: package `customer360/` at the root (not `src/`). The 360 is written as Parquet + JSON (no CSV, because `dq_flags` is a list). Reports are JSON.
DuckDB 2.0 note: lambdas use `lambda x: …`. The `x -> …` syntax raises an error.

Covers every mandatory deliverable. Config and logging are built in from the start because retrofitting them costs more than adding them now.

1. Scaffold: folders, `requirements.txt`, venv, `config/pipeline.json`, logger.
2. Readers: JSON, CSV, XML → DataFrames, plus `_source` column. Log row counts. (Step 1)
3. Standardise: rename columns to canonical names, cast types. Unparseable values become null and are flagged. (Step 2)
4. DQ checks: flag each row with issue codes, write quarantine file(s) and `dq_report.json`. (Step 3)
5. Merge: CRM left join orders and support on `customer_id` → `customer_360.json` + `.csv`. (Step 4)
6. Insights: 5 outputs written to `output/reports/` and printed as a summary. (Step 5)
7. `run_pipeline.py` runs everything with one command. Rerun gives identical output (idempotent overwrite).
8. README: run steps, assumptions, DQ results.

**M1 exit check:** a clean run from scratch matches the section 2 numbers, a second run gives identical files, and every step 1–5 deliverable exists as a file.

### M2, Brownie points (after the M1 commit)

- Unit tests (pytest): readers, standardisation, each DQ rule, merge with a missing order row, segment boundaries.
- Streamlit dashboard: revenue by city, segment split, DQ metrics.
- Expanded logging (per-step timings, counts reconciliation).

### M3, Hardening and upgrades, one commit each, pick by time

Upsert/merge into SQLite (idempotent load, ACID talking point) · Parquet outputs · schema-drift handling (unknown or missing columns) · CLI args · Dockerfile · CI yaml · incremental runs via watermark · data contracts per source · simulated streaming of new orders.

README-only (explain, don't build): AWS mapping, Airflow, real streaming, scaling to millions of rows.

## 4. Time plan (relative to start, T = 0 at problem handout)

| T | Phase | Exit |
|---|---|---|
| 0:00–0:30 | Buffer: read, profile, decisions | This file filled ✅ |
| 0:30–1:30 | M1 build | M1 exit check passes, commit |
| 1:30–2:00 | M2 tests + dashboard | Tests green, dashboard renders, commit |
| 2:00–2:30 | M3 picks / polish | Commit per upgrade |
| 2:30–3:00 | End buffer: clean-room run, README, rehearse | Submitted with 5 min to spare |

Rules:
- M1 is not committed by 1:40 → stop everything else and fix M1.
- Feature freeze at 2:30.
- A hit-and-trial item (an install, an XML edge case) gets 10 min, then the fallback.

## 5. Decisions

**All decided** (round 1, see section 9). The EDA in `notebooks/01_eda.ipynb` backs D4, D5 and D9.

| # | Decision | Chosen | Detail |
|---|---|---|---|
| D1 | Engine | **DuckDB** | All transforms are plain SQL run through DuckDB from Python. DuckDB reads JSON/CSV natively. XML is parsed with stdlib `xml.etree` and registered as a DataFrame. pandas is used only as that bridge and for Streamlit. The SQL is the scale story: the same queries port to Spark SQL / Athena / Redshift for the 5 TB/day scenario. |
| D2 | Layer storage | **raw / clean / curated folders** | Plus `quarantine/` and `reports/` under `output/` |
| D3 | Format | **Parquet for every layer**, final outputs also JSON | Both via DuckDB `COPY … TO` (no pyarrow needed). `customer_360.json` matches the expected shape |
| D4 | Bad data | **Severity-based** | EDA: nothing is safely imputable. Unusable rows (blank ID, duplicate copy) → quarantine. Fixable fields → NULL + flag, row kept, raw value kept in quarantine |
| D5 | Neg. revenue / missing orders | **NULL + flag** | corr(orders, LTV) = 0.003, so imputing would invent data. The README covers when median/KNN/MICE/MissForest would apply |
| D6 | DQ report | **Example keys + per-source + extra keys + row-level `dq_issues` file** | |
| D7 | Join | **Full outer join on `customer_id`** + presence flags `in_crm, in_orders, in_support` | Nobody is dropped. Non-CRM customers are kept and flagged (`in_crm = false`) |
| D8 | Missing source row | **NULL + presence flags + `dq_flags` list per customer** | Separates "no record" (`in_support = false`) from "record exists, field broken" (flag). Insights state their NULL handling explicitly (e.g. NULL tickets never count as >5, NULL revenue → segment `Unknown`) |
| D9 | Duplicates | **Ingestion metadata + window-function dedupe** | Raw adds `_source_file, _ingested_at, _row_num`. Clean adds `_record_hash` (hash of business columns). `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _row_num DESC)`: keep the latest ingested (last-write-wins, `dedupe_keep` in config). Losers → quarantine with `dup_type` (`exact` if same hash, `conflict` otherwise) and `duplicate_of_row` |
| D10 | Email validation | **`email-validator`** (`check_deliverability=False`: offline, deterministic) registered as a DuckDB Python UDF. ID regex `^C\d{4}$` in config | |
| D11 | Segments | **Config thresholds**, Gold > 100000, Silver 50000–100000 inclusive, Bronze < 50000, NULL → `Unknown` | |
| D12 | Code | **Modules per stage + `BaseReader` → Json/Csv/XmlReader**, picked by file extension | |
| D13 | Config | **JSON** | |
| D14 | Logging | **stdlib logging** → console + `logs/pipeline.log` | |
| D15 | Environment | **Docker is the deliverable** (`python:3.12-slim`, compose services `pipeline` + `dashboard`, `output/` volume-mounted). Local dev: `uv` venv on Python 3.12 (`.venv/` already created) | Evaluators may be on any OS. Without Docker: `uv`/pip commands in the README |

## 6. Architecture

```
config/pipeline.json ──► run_pipeline.py
                            │
   DE-Customer360/*.{json,csv,xml}   (inputs, never modified)
                            │  readers (BaseReader → Json/Csv/XmlReader)
                            ▼
   output/raw/        source rows as read + _source, _row_num
                            │  standardize (rename + type cast)
                            ▼
   output/clean/      canonical schema + dq_issues column
   output/quarantine/ unusable rows + reason
                            │  merge (CRM left join orders, support)
                            ▼
   output/curated/    customer_360.json / .csv
   output/reports/    dq_report.json, top10, segments, high_support, city_revenue, city_counts
                            ▼
   dashboard/app.py (M2) reads curated + reports
```

Proposed files (confirm or adjust in D12):

```
config/pipeline.json      sources, column maps, validation rules, segment thresholds, paths
src/customer360/
  readers.py              BaseReader + JsonReader / CsvReader / XmlReader + get_reader()
  standardize.py          column rename map, type casting
  quality.py              DQ rules → issue flags, quarantine split, dq_report
  merge.py                build Customer 360
  insights.py             the 5 insights
  logger.py               logging setup
  pipeline.py             orchestrates the steps, logs counts
run_pipeline.py           entry point: `python run_pipeline.py [--config path]`
tests/                    M2
dashboard/app.py          M2
notebooks/01_eda.ipynb    EDA (done)
Dockerfile · docker-compose.yml · .dockerignore · .gitignore
output/  logs/            generated, git-ignored
README.md (Claude) · readme_by_me (mine, Claude never edits) · CONTEXT.md · requirements.txt
```

Each step reads only the previous step's output. A requirement change touches one module.

## 7. Working rules for the coding agent

> Work one milestone step at a time. After each step print row counts and show what changed. Don't add anything beyond the current step. All paths, rules and thresholds come from config. Every run overwrites outputs deterministically (idempotent). Add a 2-line comment on each non-trivial block. Update README.md after each step. Don't touch `readme_by_me`.

Prompt shape: **goal, input, output, constraints, how I'll verify.** Verify against the section 2 numbers.

## 8. Collaboration protocol

- Claude lists decisions, each with options, trade-offs and a recommendation. I reply compactly (`D4: A, D9: default`). Anything I skip takes the recommendation.
- `README.md`: Claude maintains it after every step and decision. It's written in plain human language: what, why, what was rejected, what changed. No code dumps.
- `readme_by_me`: mine, in my own words. Claude doesn't edit it.
- Commit at every milestone and every M3 upgrade, with small, explainable diffs.

## 9. Change protocol and decision log

On a change: (1) log it here, (2) find the lowest affected step, (3) change only that module and the ones after it, (4) rerun and re-check the section 2 numbers, (5) re-check the time plan and cut M3 first.

| When | Change / decision | Why | Modules touched |
|---|---|---|---|
| T+0:15 | Layers named raw/clean/curated | Avoid clash with Gold/Silver/Bronze segments | all |
| Round 1 | D1 pandas → DuckDB | Real-world 5 TB/day story. SQL ports to Spark/Athena | all |
| Round 1 | D3 Parquet layers + JSON outputs | Columnar layers, JSON keeps the expected deliverable shape | writers |
| Round 1 | D7 full outer join + presence flags | Keep everyone, flag gaps | merge |
| Round 1 | D9 ingestion-order dedupe with hash | Production pattern: same as replayed/double-loaded batches | raw, quality |
| Round 1 | D10 email-validator | Handles real-world email forms better than a regex | quality |
| Round 1 | D15 Docker deliverable, py3.12 | OS-independent for evaluators | packaging |
| M1 | Deep EDA: 35 rows with 0 tickets but a ticket date → `warning_rules` in config. C0250 (top-10) has a bad email, which justifies field-level nulling | Hidden issues | config, quality |
| M1 | Built in parallel: insights.py + Docker packaging by agents, core by Claude | Speed | all |
| Round 1 | EDA: nothing imputable | corr = 0.003. Blank ID inferable only by position → suggestion, not auto-fix | quality |

## 10. Fallbacks

| Situation | Move |
|---|---|
| Docker build fails or is slow | Ship anyway, with the `uv`/pip run path in the README. Debug Docker last |
| DuckDB Python UDF gives trouble | Validate emails in Python on the distinct list, join the result back |
| XML parsing surprises | stdlib `xml.etree` instead of `pd.read_xml` (no lxml dependency) |
| Ambiguous requirement | Write the assumption in the README and move on |
| Agent output bloated | Revert, re-prompt for one function, or write it by hand |
| Running out of time | Drop M3, then the dashboard. Never drop: M1, a clean run, the README |

## 11. Be ready to explain

Why pandas? Why a left join from CRM? Why null rather than 0 for missing values? What happens on rerun? What happens if data is 1000× bigger (chunking, DuckDB/Spark, partitioning by city or registration month)? How do I know the numbers are right (reconciliation in section 2)? Why a reader class hierarchy (open/closed principle: a new format means a new class)? AWS mapping: S3 raw → Glue/Lambda → S3 curated/Redshift → QuickSight. Cron becomes EventBridge.

## 12. End-buffer checklist

- [ ] Delete `output/`, run from scratch, numbers match section 2
- [ ] Rerun gives identical files
- [ ] Tests green
- [ ] Dashboard launches
- [ ] README commands copy-paste clean
- [ ] Committed
- [ ] Rehearse a 2-min walkthrough and the section 11 answers
