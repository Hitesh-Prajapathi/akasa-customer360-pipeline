# Customer 360 Pipeline (Akasa Air DE Assessment)

*This file is kept up to date as the work happens, in plain language. The author's own notes are in `readme_by_me`.*

**Live dashboard:** https://akasa-customer360-pipeline-yleacgfug2v7qqljs5qdbp.streamlit.app/  
**Code:** https://github.com/Hitesh-Prajapathi/akasa-customer360-pipeline

## The problem, in one breath
An online store keeps its customers in three separate systems that never talked to each other. The CRM holds who people are, the order system holds what they spent, and the support desk holds how often they complained. Each system names things differently and stores them in a different format: JSON, CSV and XML. The job is to pull all three together into one clean profile per customer, catch the bad data along the way, and answer a few business questions:
- Who are the top spenders?
- How do customers split into Gold, Silver and Bronze?
- Who keeps contacting support?
- How does each city perform?

## What we found when we looked at the data
Before writing any code, we opened every file and counted.

- **CRM:** 501 records but only 500 real customers. One customer (C0105) appears twice, word for word. Three customers have email addresses with no "@" in them.
- **Orders:** 500 rows. One customer (C0310) has no order count. Another (C0420) shows a lifetime value of **minus 5,000**, which can't be right.
- **Support:** 500 records, but one of them has a blank customer ID. Its 8 tickets can't be tied to anyone, and customer C0450 ends up with no support history at all.
- The data is small (about 150 KB), so speed isn't the concern here. Correctness is.
- The sample names in the problem document (Amit Sharma and others) aren't in the real files. We treat them as illustrations only.

We wrote down the exact numbers we expect the quality report to show. When the pipeline runs, we can check its output against them instead of trusting it blindly.

## Assumptions
- "Revenue" means the customer's lifetime value from the order system.
- The CRM is the master list of customers. Orders and tickets are attached to it, not the other way round.
- The pipeline's internal stages are called **raw, clean and curated**. The usual names, bronze, silver and gold, are already taken by the problem's customer segments, and reusing them would confuse everyone.

## Can the broken values be repaired? (EDA)
We looked before deciding. The notebook is `notebooks/01_eda.ipynb`.
- **C0310's missing order count:** we checked whether a customer's spend says anything about how many orders they placed. It doesn't. The correlation is 0.003, essentially zero. Any "smart" fill would just guess the average (15) and pretend it was data. With one missing value out of 500, we leave it blank and flag it.
- **C0420's minus 5,000:** this could be a refund, a typo, or a sign error, and nothing in the data tells us which. Blank plus a flag, and it's left out of revenue totals.
- **The support record with no customer ID:** it sits between C0449 and C0451 in the file, and C0450 is the only customer with no support record. So it is very probably C0450's. We still don't attach it automatically. A guess that turns out wrong corrupts data silently. Instead it goes to quarantine with a note saying "probably C0450, please confirm".
- **When imputation *would* make sense:** with real volumes and columns that predict each other, the order of preference would be median or mode for a few gaps, then KNN or regression when related columns carry signal, then MICE or MissForest for many interacting gaps. None of that is true here.

## Decisions made (and what we turned down)
- **DuckDB instead of plain pandas.** The data is small today, but the realistic scenario is millions of rows and terabytes a day. Writing the logic as SQL means the same queries move to Spark, Athena or Redshift almost unchanged. pandas stays only where it's needed: reading the XML and feeding the dashboard.
- **Three stages on disk: raw, clean, curated.** Every stage can be opened and checked, and any stage can be rebuilt from the one before it.
- **Parquet inside the pipeline, JSON at the end.** Parquet is the standard compact, typed format for data lakes. The final customer file is also written as JSON, because that's the shape the problem asks for.
- **Bad data is handled by severity.** A row we can't use at all, such as a duplicate copy or a record with no ID, is moved to quarantine. A row with one broken field keeps the customer and blanks only that field, with a flag explaining why. Nothing is ever silently deleted.
- **Everyone is kept when merging.** All three systems are combined, and every customer carries three yes/no markers: is this person in the CRM, in orders, in support? A blank value then always has an explanation. Either there was no record, or the record was broken (and there's a flag saying so).
- **Duplicates are handled the way production pipelines do it.** Every row is stamped with where it came from and in what order it arrived, plus a fingerprint of its content. If a customer appears twice, the most recently loaded version wins and the other moves to quarantine. It's labelled an *exact* copy (same fingerprint, which typically means a batch was loaded twice) or a *conflict* (same ID, different details, which needs a human).
- **A proper email validator instead of a hand-written pattern**, run offline so results are fast and repeatable.
- **Segment thresholds live in the config file.** Exactly 50,000 and exactly 100,000 both count as Silver, and customers whose revenue is unknown are labelled "Unknown" rather than forced into Bronze.
- **Docker is the way to run it.** We don't know what machine the reviewers use. A container runs the same everywhere, and plain Python instructions remain as a backup.
- *Turned down:* pandas-only (doesn't tell the scale story), inner joins (would quietly lose customers), filling gaps with 0 or averages (invents data), and auto-fixing the blank ID (a silent guess).

## The less obvious things the data told us
A second, deeper look turned up issues that per-column checks never catch:
- **35 customers have zero support tickets but still have a "last ticket date".** Each value looks fine on its own; together they contradict each other. We keep both values and raise a warning, because we can't tell which one is wrong.
- **One of the top 10 spenders (C0250) has a broken email.** If we had thrown away every row with a bad email, the "Top 10 customers" report would quietly have been wrong. This is the clearest argument for blanking just the bad field.
- **The duplicate customer sits at the very end of the CRM file.** That's what a batch loaded twice by mistake looks like, which is exactly the case our "latest load wins" rule is designed for.
- **The orders file was saved on Windows** (different line endings). Harmless for us, but it would trip up a naive reader.
- **More than half of all customers have over 5 support tickets.** The ">5 tickets" list therefore isn't very selective. The threshold lives in the config so the business can tune it.

## How the pipeline is built
One command runs five steps, and each step only reads what the previous one wrote:

```
 CRM (JSON)   Orders (CSV)   Support (XML)
     │             │              │          1. read as-is, stamped with file + arrival order
     ▼             ▼              ▼
 output/raw/   (Parquet, untouched text)
     │                                       2. rename to one shared vocabulary (cust_id → customer_id)
     ▼                                       3. check every row, label every problem
 output/clean/       ← usable rows, broken values blanked
 output/quarantine/  ← unusable rows (duplicate copy, blank ID), kept for review
     │                                       4. join all three on customer_id, nobody dropped
     ▼
 output/curated/customer_360  (Parquet + JSON)
     │                                       5. business reports
     ▼
 output/reports/  dq_report, dq_issues, top 10, segments, high-support, city revenue & counts
```

- **Reading.** Each file format has its own small reader. They all hand back the same shape, so adding a fourth source (say, Parquet from a data lake) means writing one new reader and touching nothing else.
- **Naming.** The config file holds the translation table from each system's column names to ours. The code has no hardcoded column names.
- **Checking.** Every row gets a list of the problems found. Each problem is either *reject* (the row can't be used), *field* (one value is bad, so blank it) or *warning* (keep it, just report it).
- **Merging.** All three systems are combined. Each customer carries markers showing which systems know about them, and a list of every problem found in any of their records.
- **Reporting.** Each business question is one short SQL query.
- **Logging.** Everything is logged to the screen and to `logs/pipeline.log`.

## Different systems, different column names
Each system calls the customer something different: `customer_id`, `cust_id`, `customerId`, and tomorrow maybe `c_id` or `Customer ID`. We handle this in two layers, both in the config file rather than the code:
1. **Tidy, then compare.** Names are lowercased and stripped of underscores, dashes and spaces before comparing. `customerId`, `Customer_ID` and `customer id` all become the same thing and match `customer_id` with no extra rules.
2. **Known synonyms.** A shared list of aliases catches genuinely different words, such as `cust_id`, `c_id` or `client_id` for the customer, and `ltv` or `revenue` for lifetime value. A source can also pin an exact mapping if it needs to.

If two columns could both be the customer ID, we stop rather than guess. A wrong guess would mix up customers silently. Unknown extra columns are logged so someone notices the source changed.

## When a source changes shape: degrade, don't die
A pipeline that receives data continuously can't afford to stop because one feed changed. So:
- **A new column appears:** logged, ignored, and kept in the raw copy.
- **A non-essential column disappears:** filled with blanks, every row flagged, and processing carries on.
- **The customer ID column disappears:** that source's whole batch is set aside untouched, ready to replay once the mapping is fixed. The other sources still go through, and the report marks that source as failed. We tested it: with the ID column removed from the orders file, the run still finished, with 500 customers and orders marked failed.

In a real streaming setup this becomes a schema registry (which decides what changes are allowed), a dead-letter queue (where bad batches wait), and a per-source circuit breaker (which pauses only the broken feed).

## Newer additions (M3)
- **A chat assistant on every dashboard page.** At the bottom of each page you can ask about what you're looking at, for example "why is successful lower than total?" or "what does high value, high friction mean for the business?". It only sees that page's numbers and a glossary of the terms. It's told to explain what the numbers mean and to say so when something isn't on the page, never to make things up. Email addresses are stripped out in code before anything leaves the app. Customer names are only shared on the lookup page, where that customer is the whole point. Without an API key the chat politely says it's unavailable, and the rest of the page works as normal. It runs on Groq's `llama-3.3-70b-versatile`.
- **A small database (SQLite) as the final home of the Customer 360**, loaded with *insert-or-update*. Running the pipeline twice doesn't create duplicates: the second run reports 0 new, 0 changed, 500 unchanged. The whole load is one transaction, so a crash halfway leaves the database exactly as it was. Every load is recorded in an audit table.
- **New data arriving in small batches (a simulated stream).** A producer drops order-update files into `incoming/orders/`. The stream runner picks up only the files it hasn't seen yet, checks them with the same quality rules, sets bad rows aside, updates just the affected customers, and refreshes the reports and dashboard. A bookmark (a "watermark") records the last file processed. It's saved in the same transaction as the data, so a file is never skipped and never applied twice.
- **Automatic checks on every push (CI).** GitHub runs the lint, the pipeline, all the tests and the Docker build. The quality report from each run can be downloaded from the run page.
- *Known limit:* when a stream batch fixes a value (say C0310's missing order count), that customer's old quality flag isn't cleared until the next full batch run.

## How to run it
**With Docker (recommended, works on any OS):**
```
docker compose run --rm pipeline
```
Results appear in `output/`, logs in `logs/`.

**The dashboard:** `docker compose up`, then open http://localhost:8501. The pipeline runs first and the dashboard starts once it has finished. Without Docker, run `streamlit run dashboard/app.py`. If there are no results yet, the dashboard runs the pipeline itself, so a hosted copy is a live pipeline, not a static picture.

**The tests:** `pip install -r requirements-dev.txt`, then `pytest`. That runs 100 tests in about 2 seconds.

**The simulated stream** (after a normal run):
```
python -m stream.producer --batches 5 --size 20   # drop 5 batches of order updates
python run_stream.py --once                        # process only the new ones (omit --once to keep watching)
```
With Docker: `docker compose run --rm stream`.

**The chat assistant:** put `GROQ_API_KEY = "gsk_..."` in `.streamlit/secrets.toml` (this file is never committed), or in the app's Secrets on Streamlit Cloud.

The polished write-up for reviewers is in `DOCUMENTATION.md`.

**Without Docker (needs Python 3.12):**
```
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python run_pipeline.py                                # optional: --config path/to/other.json
```

## How we know the data is right
We wrote down the expected numbers *before* building, then checked the pipeline against them. They match exactly.

| Check | Expected | Pipeline |
|---|---|---|
| Records read (all three files) | 1501 | 1501 |
| Duplicates | 1 | 1 (C0105, exact copy) |
| Invalid emails | 3 | 3 (C0125, C0250, C0375) |
| Missing mandatory fields | 1 | 1 (C0310 order count) |
| Negative revenue | 1 | 1 (C0420) |
| Invalid customer IDs | 1 | 1 (blank support record, flagged "probably C0450") |
| Successful records | 1494 | 1494 |
| Customers in the 360 view | 500 | 500 |
| Warnings (0 tickets but a ticket date) | 35 | 35 |

- **Every input row is accounted for.** For each file, *rows read = clean rows + quarantined rows*. The pipeline stops with an error if that ever fails.
- **Running it twice gives byte-for-byte identical files.** We checked this with file hashes. The "arrival time" stamp is the file's own timestamp, not the clock, which is what keeps reruns identical.
- `output/reports/dq_issues.json` lists every problem row: what was wrong and what the pipeline did about it.

**What the problem's sample report doesn't say, and how we read it:** a row can have more than one problem, so the problem counts can overlap. "Successful records" counts rows with no real problem. Warnings don't count against a row.

## If this were in production
_How the local setup maps to cloud services._

## Timeline
| When | What happened |
|---|---|
| Start | Read the problem, profiled all three files, found 7 planted data issues, drafted the plan |
| Round 1 | All 15 design decisions settled. EDA notebook confirmed nothing can be safely imputed |
| M1 | Deeper EDA found 5 easy-to-miss issues. Baseline built (two helpers worked in parallel on the reports and the Docker setup). All expected numbers matched, reruns are identical, and it runs in Docker. Committed as `v1-baseline` |
| M2 | Added column-name matching and "degrade, don't die" for broken sources. Four helpers worked in parallel: 100 tests, the dashboard, the reviewer documentation, plus logging upgrades. The full Docker stack works. Committed as `v2-brownie` |
| M2+ | Dashboard grew from one page to four: Overview, Pipeline flow (row counts at every stage), Customer lookup (one customer across all three systems, raw records side by side), Quality & risk (issue drill-down, quarantine review, high-value customers with many tickets). Three helpers built the three new pages at the same time, one file each |
| M2.6 | Published the code on GitHub and deployed the dashboard on Streamlit Community Cloud. The hosted copy runs the full pipeline itself on first load |
| M3 | Three helpers in parallel, each on separate files: a page-aware chat assistant (Groq), CI on GitHub, then the SQLite insert-or-update load and the simulated stream. 124 tests pass, and CI passed on its first run |

## What we'd do next
_Filled in at the end._
