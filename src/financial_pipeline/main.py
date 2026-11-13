"""Run the pipeline: fetch -> normalize -> load into PostgreSQL -> summarize.

    python -m financial_pipeline.main 2026-09-01 2026-09-30

Both sources are fetched and normalized before the database is touched, then
loaded in a single transaction. Any fetch or load failure exits non-zero and
writes nothing.
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg

from financial_pipeline import database, ingest
from financial_pipeline.database import LoadResult
from financial_pipeline.transform import (
    FRED_DGS10,
    TREASURY_PAR_10Y,
    NormalizationResult,
    SeriesMeta,
    normalize_fred,
    normalize_treasury,
)

SOURCES = (
    (FRED_DGS10, ingest.fetch_fred, normalize_fred),
    (TREASURY_PAR_10Y, ingest.fetch_treasury, normalize_treasury),
)


@dataclass
class SourceReport:
    meta: SeriesMeta
    fetched: int
    result: NormalizationResult
    gaps: list[date]
    load: LoadResult | None = None


def weekday_gaps(start: date, end: date, result: NormalizationResult) -> list[date]:
    """Weekdays in [start, end] with neither a value nor a source-reported missing marker.

    Informational only: without a market-holiday calendar, a gap may be a
    legitimate closure (e.g. Treasury omits holidays rather than marking them),
    or data the source has not yet published. Nothing is filled in.
    """
    seen = {o.observation_date for o in result.observations} | set(result.missing)
    days = (start + timedelta(n) for n in range((end - start).days + 1))
    return [d for d in days if d.weekday() < 5 and d not in seen]


def run(start: date, end: date) -> list[SourceReport]:
    reports = []
    for meta, fetch, normalize in SOURCES:
        raw = fetch(start, end)
        result = normalize(raw, start, end)
        reports.append(SourceReport(meta, len(raw), result, weekday_gaps(start, end, result)))

    # autocommit: load()'s explicit transaction is then the only transaction.
    with database.connect(autocommit=True) as conn:
        loaded = database.load(conn, [(r.meta, r.result.observations) for r in reports])
    for r in reports:
        r.load = loaded[r.meta.source]
    return reports


def format_summary(start: date, end: date, reports: list[SourceReport]) -> str:
    lines = [f"Pipeline summary {start} .. {end}"]
    for r in reports:
        res, ld = r.result, r.load
        lines.append(
            f"  {r.meta.source:<12} {r.meta.series_id:<10} "
            f"fetched={r.fetched} valid={len(res.observations)} missing={len(res.missing)} "
            f"rejected={len(res.rejected)} inserted={ld.inserted} updated={ld.updated} "
            f"skipped={ld.skipped}"
        )
        outside = r.fetched - len(res.observations) - len(res.missing) - len(res.rejected)
        if outside:
            lines.append(f"    outside requested window: {outside}")
        if res.missing:
            lines.append(f"    source-reported missing: {', '.join(map(str, res.missing))}")
        for rej in res.rejected:
            lines.append(f"    rejected: {rej.reason}")
        if r.gaps:
            lines.append(f"    weekday gaps (may be holidays/unpublished): "
                         f"{', '.join(map(str, r.gaps))}")
    return "\n".join(lines)


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO date (YYYY-MM-DD): {text!r}") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="financial_pipeline.main", description=__doc__.split("\n")[0])
    parser.add_argument("start", type=_iso_date, help="first date, YYYY-MM-DD")
    parser.add_argument("end", type=_iso_date, help="last date, YYYY-MM-DD (inclusive)")
    args = parser.parse_args(argv)
    if args.start > args.end:
        parser.error("start must be on or before end")
    if args.end > date.today():
        parser.error("end must not be in the future")

    try:
        reports = run(args.start, args.end)
    except (ingest.IngestError, database.DatabaseConfigError, psycopg.Error) as exc:
        print(f"error: {exc}\nNo data was written.", file=sys.stderr)
        return 1
    print(format_summary(args.start, args.end, reports))
    return 0


if __name__ == "__main__":
    sys.exit(main())
