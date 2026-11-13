-- Financial data pipeline: analytical queries.
--
-- Run:  psql -U <user> -d <db> -f sql/analysis.sql
--
-- Series are selected by (sources.name, series.external_id), never by
-- surrogate id. Only stored observations are used: weekends, holidays, and
-- source-reported missing values are absent rows, and nothing is filled in.
-- Values are in percent; differences are in percentage points (pp).


-- ---------------------------------------------------------------------------
-- Query 1: Monthly average yield per series
--
-- One row per (calendar month, source, series). observation_count is the
-- number of trading days that actually have a stored value in that month.
-- ---------------------------------------------------------------------------
SELECT
    date_trunc('month', o.observation_date)::date AS month,
    src.name                                      AS source,
    s.external_id                                 AS series,
    round(avg(o.value), 4)                        AS avg_yield_pct,
    count(*)                                      AS observation_count,
    min(o.observation_date)                       AS first_date,
    max(o.observation_date)                       AS last_date
FROM observations AS o
JOIN series       AS s   ON s.series_id   = o.series_id
JOIN sources      AS src ON src.source_id = s.source_id
GROUP BY month, src.name, s.external_id
ORDER BY month, source, series;


-- ---------------------------------------------------------------------------
-- Query 2: Five-observation moving average of FRED DGS10
--
-- The frame is ROWS, not RANGE: it covers the current observation and the
-- four stored observations before it, which can span more than five calendar
-- days when weekends or holidays intervene (see calendar_days_spanned).
-- The average is reported only once the window holds a full five
-- observations; earlier rows show NULL rather than a partial average.
-- ---------------------------------------------------------------------------
WITH dgs10 AS (
    SELECT o.series_id, o.observation_date, o.value
    FROM observations AS o
    JOIN series       AS s   ON s.series_id   = o.series_id
    JOIN sources      AS src ON src.source_id = s.source_id
    WHERE src.name = 'FRED' AND s.external_id = 'DGS10'
)
SELECT
    observation_date,
    round(value, 2)                                     AS yield_pct,
    CASE WHEN count(*) OVER w = 5
         THEN round(avg(value) OVER w, 4) END           AS moving_avg_5obs,
    count(*) OVER w                                     AS obs_in_window,
    first_value(observation_date) OVER w                AS window_start,
    observation_date - first_value(observation_date) OVER w + 1
                                                        AS calendar_days_spanned
FROM dgs10
WINDOW w AS (
    PARTITION BY series_id
    ORDER BY observation_date
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
)
ORDER BY observation_date;


-- ---------------------------------------------------------------------------
-- Query 3: Date-aligned comparison of FRED DGS10 vs Treasury BC_10YEAR
--
-- FRED republishes Treasury's 10-year constant-maturity yield, so these
-- should agree; this is a cross-source consistency check. The INNER JOIN on
-- observation_date keeps only dates present in both sources.
-- difference_pp = FRED - Treasury.
-- ---------------------------------------------------------------------------
WITH tagged AS (
    SELECT src.name AS source, s.external_id AS series, o.observation_date, o.value
    FROM observations AS o
    JOIN series       AS s   ON s.series_id   = o.series_id
    JOIN sources      AS src ON src.source_id = s.source_id
),
fred AS (
    SELECT observation_date, value FROM tagged
    WHERE source = 'FRED' AND series = 'DGS10'
),
treasury AS (
    SELECT observation_date, value FROM tagged
    WHERE source = 'US_TREASURY' AND series = 'BC_10YEAR'
)
SELECT
    f.observation_date,
    round(f.value, 2)            AS fred_dgs10_pct,
    round(t.value, 2)            AS treasury_bc_10year_pct,
    f.value - t.value            AS difference_pp,
    abs(f.value - t.value)       AS abs_difference_pp
FROM fred     AS f
JOIN treasury AS t ON t.observation_date = f.observation_date
ORDER BY f.observation_date;
