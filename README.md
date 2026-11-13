# Financial Data Pipeline

A small, production-minded batch pipeline that extracts U.S. 10-year Treasury
yields from two public sources, validates and normalizes them, loads them
idempotently into PostgreSQL, and analyzes them with SQL.

## Overview

Financial teams rarely trust a single feed. Market data arrives from multiple
providers, each with its own format, missing-value conventions, and revision
behavior, and downstream reports are only as reliable as the ingestion layer
beneath them.

This project demonstrates that ingestion layer at a deliberately small scale:

- **Extraction** from a JSON REST API (FRED) and an XML Atom feed (U.S. Treasury).
- **Validation** that separates source-reported missing values from malformed
  records instead of silently dropping or zero-filling either.
- **Idempotent loading** into a normalized relational model, with accurate
  inserted/updated/skipped counts and all-or-nothing transactions.
- **SQL analytics** — monthly aggregation, window-function moving averages, and
  a date-aligned cross-source reconciliation.

## Architecture

```
 FRED API (JSON)  ─┐
                   ├─► ingest.py ─► transform.py ─► database.py ─► PostgreSQL ─► sql/analysis.sql
 Treasury (XML)   ─┘   extract       validate &       transactional    sources
                                      normalize        upsert           series
                                                                        observations
                         └──────────── main.py: CLI orchestration + summary ────────────┘
```

| Module | Responsibility |
|---|---|
| `ingest.py` | HTTP requests and response parsing only. Returns records in each source's own shape. Treasury is fetched one month per request (the feed's finest filter). Errors never include request URLs, so the FRED API key cannot leak into logs or tracebacks. |
| `transform.py` | Validates records and sorts each into exactly one bucket: **observations**, **missing**, or **rejected** (with a reason). Holds the source/series metadata (`SeriesMeta`). |
| `database.py` | Connection handling and `load()`: registers sources and series, upserts observations, and returns per-source counts — all inside one transaction. |
| `main.py` | `argparse` CLI: validates the date range, fetches and normalizes both sources, loads them, flags weekday gaps, and prints the summary. |

## Technology

| Tool | Used for |
|---|---|
| Python 3.12 | Pipeline code |
| PostgreSQL 17 (Docker image `postgres:17`) | Storage and analytics |
| psycopg 3 | Database driver; parameterized SQL, explicit transactions |
| Docker Compose | Local PostgreSQL with a named volume and health check |
| pytest | Unit and integration tests |
| Standard library | `urllib.request` (HTTP), `json`, `xml.etree.ElementTree` (Atom/XML), `decimal`, `datetime`, `argparse`, `dataclasses` |

psycopg is the only runtime dependency.

## Data Sources

| | FRED `DGS10` | U.S. Treasury `BC_10YEAR` |
|---|---|---|
| Series | Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity | Daily Treasury Par Yield Curve Rate, 10-Year |
| Publisher | Federal Reserve Bank of St. Louis (FRED), from the Federal Reserve Board's H.15 release | U.S. Department of the Treasury |
| Links | [Series](https://fred.stlouisfed.org/series/DGS10) · [API docs](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | [Daily rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve) · [XML feed docs](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) |
| Format | JSON | XML (OData Atom feed) |
| Frequency / unit | Daily (business days) / Percent | Daily (business days) / Percent |
| Authentication | Free API key | None |
| Missing dates | Returned with the value `"."` (e.g. Labor Day) → counted as **missing** | Omitted from the feed entirely → not reported; surfaces only as a **weekday gap** |

**These are not independent indicators.** The H.15 constant-maturity yields that
FRED republishes are derived from Treasury's own yield curve, so the two series
should match. Comparing them is a **reconciliation check** on the pipeline —
a non-zero difference indicates a loading, parsing, or revision problem — not
an economic comparison.

## Data Model

Defined in [`sql/schema.sql`](sql/schema.sql) (safe to run repeatedly).

```
sources (1) ──< series (1) ──< observations
```

| Table | Key | Constraints |
|---|---|---|
| `sources` | `source_id` identity PK | `name` UNIQUE (`FRED`, `US_TREASURY`) |
| `series` | `series_id` identity PK | FK `source_id → sources`; UNIQUE `(source_id, external_id)` |
| `observations` | composite PK `(series_id, observation_date)` | FK `series_id → series`; `value NUMERIC NOT NULL`; `fetched_at TIMESTAMPTZ` |

- The natural keys (`sources.name`, `series(source_id, external_id)`) are the
  conflict targets for idempotent registration; surrogate ids are only used
  for joins.
- The composite observation key enforces **one value per series per day** at
  the database level, and is the conflict target for observation upserts.
- `fetched_at` records when the stored value was last written — it changes only
  when the value changes.

## Setup

Requires Python 3.12+ and Docker. Run all commands from the repository root.

```bash
# 1. Virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# 2. Dependencies (package in editable mode + pytest)
pip install -e ".[dev]"

# 3. Configuration: copy, then set a password and your FRED API key.
#    DATABASE_URL must use the same user/password/port/database as the POSTGRES_* values.
cp .env.example .env

# 4. Start PostgreSQL (Compose reads .env) and wait until it is healthy
docker compose up -d --wait db

# 5. Initialize the schema (uses psql inside the container; no host psql needed)
docker compose exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' < sql/schema.sql

# 6. Export variables for the Python process
set -a; source .env; set +a
python -m financial_pipeline.database   # optional: prints "Connected to PostgreSQL 17.x"

# 7. Run the pipeline for an inclusive date range
python -m financial_pipeline.main 2026-09-01 2026-09-30

# 8. Run the analytical queries
docker compose exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < sql/analysis.sql

# 9. Run the tests
pytest
```

Database tests run only when `DATABASE_URL` is set; otherwise they are skipped
and the unit tests still run. Each database test creates a throwaway schema and
drops it afterwards, so it never reads or modifies the pipeline's real tables.

`docker compose down` stops PostgreSQL and keeps the data volume.

## Pipeline Behavior

**Extract → Validate → Transform → Load.** Both sources are fully fetched and
normalized *before* the database is touched. If either source fails, the run
exits with status 1, prints `No data was written.`, and the database is
unchanged. Invalid arguments (bad dates, start after end, end in the future)
exit with status 2.

**Missing vs. rejected.** Every record ends up in exactly one bucket:

- *missing* — the source explicitly reported no value for that date (FRED `"."`,
  a null or absent Treasury rate). Expected; nothing is stored.
- *rejected* — malformed: unparseable or non-midnight dates, non-numeric or
  non-finite values (`NaN`, `Infinity`), missing fields, or duplicate dates
  (every record for a duplicated date is rejected, since the correct value is
  ambiguous). Rejected records are never stored and are listed with a reason.

Values are parsed as `Decimal` from their source strings and never coerced to
zero.

**Idempotent upserts.** For each observation:

| Stored state | Action | Counted as |
|---|---|---|
| Not present | Insert | `inserted` |
| Present, same value | No write; `fetched_at` unchanged | `skipped` |
| Present, different value | Update value and `fetched_at` | `updated` |

Observations a source stops returning are never deleted. Each series is loaded
with one set-based statement (see `database.py`):

```sql
INSERT ... ON CONFLICT (series_id, observation_date) DO UPDATE
    SET value = EXCLUDED.value, fetched_at = now()
    WHERE observations.value IS DISTINCT FROM EXCLUDED.value
RETURNING ...
```

The `WHERE` turns an unchanged value into a no-op. Counts come from a CTE that
joins the returned keys against the pre-statement snapshot of `observations`:
returned keys that already existed were updated, the rest were inserted, and
anything not returned was skipped.

**Transactions.** `load()` wraps every source, series, and observation write for
the run in a single transaction. Any exception rolls back all of it, so a
failed run never leaves partial data (covered by a test that forces a
`NOT NULL` violation mid-load).

**Data-quality summary.** Each run prints, per source: `fetched`, `valid`,
`missing`, `rejected`, `inserted`, `updated`, `skipped`, plus the missing dates,
rejection reasons, rows outside the requested window, and weekday gaps.

**Weekday gap warnings.** A weekday with neither a value nor a missing marker
is listed as a gap. This is informational only: without a trading-holiday
calendar the pipeline cannot tell a market holiday (Treasury omits these) or
not-yet-published data from a genuine data problem. Gaps are never filled.

### Verified September 2026 run

First run against an empty database:

```
Pipeline summary 2026-09-01 .. 2026-09-30
  FRED         DGS10      fetched=22 valid=21 missing=1 rejected=0 inserted=21 updated=0 skipped=0
    source-reported missing: 2026-09-07
  US_TREASURY  BC_10YEAR  fetched=21 valid=21 missing=0 rejected=0 inserted=21 updated=0 skipped=0
    weekday gaps (may be holidays/unpublished): 2026-09-07
```

Second run of the same range — nothing inserted or updated, `fetched_at` untouched:

```
Pipeline summary 2026-09-01 .. 2026-09-30
  FRED         DGS10      fetched=22 valid=21 missing=1 rejected=0 inserted=0 updated=0 skipped=21
    source-reported missing: 2026-09-07
  US_TREASURY  BC_10YEAR  fetched=21 valid=21 missing=0 rejected=0 inserted=0 updated=0 skipped=21
    weekday gaps (may be holidays/unpublished): 2026-09-07
```

Labor Day (2026-09-07) shows both missing-date conventions: FRED returns it as
`"."` (counted as missing), while Treasury omits it, so it appears only as a
weekday gap. Resulting tables: 2 sources, 2 series, 42 observations.

## SQL Analytics

[`sql/analysis.sql`](sql/analysis.sql) contains three queries. Series are
selected by `(sources.name, series.external_id)`, never by surrogate id.

**1. Monthly average yield per series** — `JOIN` across all three tables,
`date_trunc('month', …)`, `GROUP BY`, `AVG`, `COUNT`.

```
   month    |   source    |  series   | avg_yield_pct | observation_count | first_date | last_date
 2026-09-01 | FRED        | DGS10     |        4.9890 |                21 | 2026-09-01 | 2026-09-30
 2026-09-01 | US_TREASURY | BC_10YEAR |        4.9890 |                21 | 2026-09-01 | 2026-09-30
```

**2. Five-observation moving average (FRED DGS10)** — window function with a
named `WINDOW`, `PARTITION BY series_id ORDER BY observation_date ROWS BETWEEN
4 PRECEDING AND CURRENT ROW`. Because the frame is `ROWS`, it spans five stored
observations, not five calendar days; `calendar_days_spanned` makes that
visible. Rows with fewer than five observations show `NULL` rather than a
partial average.

```
 observation_date | yield_pct | moving_avg_5obs | obs_in_window | window_start | calendar_days_spanned
 2026-09-04       |      4.78 |                 |             4 | 2026-09-01   |                     4
 2026-09-08       |      4.80 |          4.7860 |             5 | 2026-09-01   |                     8
 2026-09-18       |      5.01 |          4.9860 |             5 | 2026-09-14   |                     5
 2026-09-30       |      5.29 |          5.2280 |             5 | 2026-09-24   |                     7
```

The 2026-09-08 window spans eight calendar days because of the Labor Day
weekend.

**3. Date-aligned reconciliation (FRED vs Treasury)** — CTEs and an `INNER JOIN`
on `observation_date`, so only dates present in both sources are compared;
signed and absolute differences in percentage points.

```
 observation_date | fred_dgs10_pct | treasury_bc_10year_pct | difference_pp | abs_difference_pp
 2026-09-01       |           4.79 |                   4.79 |          0.00 |              0.00
 2026-09-02       |           4.79 |                   4.79 |          0.00 |              0.00
 ...                (21 rows, every difference 0.00)
 2026-09-30       |           5.29 |                   5.29 |          0.00 |              0.00
```

All 21 September trading days reconcile exactly. Weekends and Labor Day have no
rows in either source, and nothing is fabricated for them.

## Engineering Decisions

| Decision | Rationale | Tradeoff |
|---|---|---|
| psycopg, no ORM | The core logic *is* SQL (`ON CONFLICT`, snapshot-based counting, window functions); writing it directly keeps it visible and reviewable. | Hand-written SQL; no model layer or migrations tooling. |
| Standard-library HTTP | Two GET endpoints don't justify a dependency; `urllib` gives full control over timeouts and error messages (no key leakage). | More verbose than `requests`; no built-in retries or sessions. |
| Three-table model | Sources and series are metadata registered once; observations are a narrow fact table. Adding a series needs no schema change. | One extra join compared with a wide table. |
| `Decimal` / `NUMERIC` | Yields are published as decimal strings; binary floats would introduce representation error and make equality-based change detection unreliable. | Slightly slower than `float`/`double precision` — irrelevant at this scale. |
| Composite PK `(series_id, observation_date)` | Encodes the business rule "one value per series per day" in the database, and doubles as the upsert conflict target and the index for date-ordered queries. | Intraday or vintage (revision history) data would need a different key. |
| Single transaction per run | All-or-nothing loads make reruns the only recovery step ever needed. | One unavailable source blocks loading the other. |
| `argparse` CLI, no orchestrator | A two-source batch job is fully served by a CLI that cron or any scheduler can call. Airflow/Dagster would add more infrastructure than pipeline. | Scheduling, retries, and alerting are left to whatever invokes the CLI. |

## Limitations

- **No scheduling or orchestration** — runs are manual (or via an external scheduler).
- **No automated retries** — a network or HTTP failure aborts the run; rerunning is safe because loads are idempotent.
- **No trading-calendar integration** — weekday gaps cannot be distinguished from market holidays or publication lag.
- **Limited history** — the demonstration loads September 2026 only (21 trading days per series). Larger ranges work but are not part of the verified run.
- **Concurrent runs** — data stays correct under concurrent loads (`ON CONFLICT` is safe), but inserted/updated counts assume a single writer; a row inserted by a concurrent run between snapshots can be reported as inserted rather than updated.
- **No revision history** — an updated value overwrites the previous one; prior vintages are not kept.
