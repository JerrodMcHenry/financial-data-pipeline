"""PostgreSQL connection handling and idempotent loading.

Usage:
    with connect() as conn:
        results = load(conn, [(FRED_DGS10, fred.observations)])

The connection context manager commits on success, rolls back on error,
and always closes the connection. load() additionally wraps all of its work
in a single transaction, so a failure leaves no partially written data.
"""

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import psycopg

from financial_pipeline.transform import Observation, SeriesMeta


class DatabaseConfigError(RuntimeError):
    """Raised when required database configuration is missing."""


@dataclass(frozen=True, slots=True)
class LoadResult:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise DatabaseConfigError(
            "DATABASE_URL is not set. Copy .env.example to .env, fill in values, "
            "and export it (e.g. `set -a; source .env; set +a`)."
        )
    return url


def connect(**kwargs) -> psycopg.Connection:
    """Open a new PostgreSQL connection. Use it as a context manager."""
    return psycopg.connect(get_database_url(), **kwargs)


_UPSERT_SOURCE = """
    INSERT INTO sources (name, base_url)
    VALUES (%s, %s)
    ON CONFLICT (name) DO UPDATE SET base_url = EXCLUDED.base_url
    RETURNING source_id
"""

_UPSERT_SERIES = """
    INSERT INTO series (source_id, external_id, name, unit, frequency)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (source_id, external_id) DO UPDATE
        SET name = EXCLUDED.name, unit = EXCLUDED.unit, frequency = EXCLUDED.frequency
    RETURNING series_id
"""

# One statement per series. The WHERE clause on DO UPDATE turns an unchanged
# value into a no-op: the row is not rewritten and fetched_at is preserved.
#
# Counting: every statement in a WITH query runs against the same snapshot, so
# the outer SELECT sees `observations` as it was *before* the upsert. A
# returned key that already existed was updated; one that did not was inserted.
# Keys that were neither returned were skipped (value unchanged).
_UPSERT_OBSERVATIONS = """
    WITH incoming AS (
        SELECT %(series_id)s::integer AS series_id, d AS observation_date, v AS value
        FROM unnest(%(dates)s::date[], %(values)s::numeric[]) AS t(d, v)
    ),
    upserted AS (
        INSERT INTO observations (series_id, observation_date, value)
        SELECT series_id, observation_date, value FROM incoming
        ON CONFLICT (series_id, observation_date) DO UPDATE
            SET value = EXCLUDED.value, fetched_at = now()
            WHERE observations.value IS DISTINCT FROM EXCLUDED.value
        RETURNING series_id, observation_date
    )
    SELECT
        count(*) FILTER (WHERE prior.series_id IS NULL)     AS inserted,
        count(*) FILTER (WHERE prior.series_id IS NOT NULL) AS updated
    FROM upserted u
    LEFT JOIN observations prior USING (series_id, observation_date)
"""


def _upsert_series(cur: psycopg.Cursor, meta: SeriesMeta) -> int:
    (source_id,) = cur.execute(_UPSERT_SOURCE, (meta.source, meta.source_base_url)).fetchone()
    (series_id,) = cur.execute(
        _UPSERT_SERIES, (source_id, meta.series_id, meta.name, meta.unit, meta.frequency)
    ).fetchone()
    return series_id


def _upsert_observations(
    cur: psycopg.Cursor, series_id: int, observations: Sequence[Observation]
) -> LoadResult:
    if not observations:
        return LoadResult()
    params = {
        "series_id": series_id,
        "dates": [o.observation_date for o in observations],
        "values": [o.value for o in observations],
    }
    inserted, updated = cur.execute(_UPSERT_OBSERVATIONS, params).fetchone()
    return LoadResult(inserted, updated, len(observations) - inserted - updated)


def load(
    conn: psycopg.Connection, datasets: Iterable[tuple[SeriesMeta, Sequence[Observation]]]
) -> dict[str, LoadResult]:
    """Register sources/series and upsert observations in one transaction.

    Returns a LoadResult per source name. Existing observations that are absent
    from `datasets` are never deleted. Any exception rolls back every write.
    """
    results: dict[str, LoadResult] = {}
    with conn.transaction(), conn.cursor() as cur:
        for meta, observations in datasets:
            mismatched = [o for o in observations
                          if (o.source, o.series_id) != (meta.source, meta.series_id)]
            if mismatched:
                raise ValueError(f"{meta.source}: observation does not belong to "
                                 f"series {meta.series_id}: {mismatched[0]!r}")
            series_id = _upsert_series(cur, meta)
            results[meta.source] = _upsert_observations(cur, series_id, observations)
    return results


if __name__ == "__main__":
    with connect() as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
    print(f"Connected to PostgreSQL {version}")
