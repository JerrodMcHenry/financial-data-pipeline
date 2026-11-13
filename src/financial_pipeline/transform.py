"""Validate and normalize raw source records into a shared representation.

Every input record ends up in exactly one bucket:
  - observations: valid, normalized records
  - missing:      dates the source explicitly reported as having no value
  - rejected:     malformed records, kept with the reason they were rejected

Duplicate policy: if a date appears more than once in a response, every record
for that date is rejected. Which value is correct is ambiguous, so none is chosen.
"""

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True, slots=True)
class SeriesMeta:
    source: str
    source_base_url: str
    series_id: str  # the source's own identifier for the series
    name: str
    unit: str
    frequency: str


FRED_DGS10 = SeriesMeta(
    source="FRED",
    source_base_url="https://api.stlouisfed.org",
    series_id="DGS10",
    name="Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
    unit="Percent",
    frequency="Daily",
)

TREASURY_PAR_10Y = SeriesMeta(
    source="US_TREASURY",
    source_base_url="https://home.treasury.gov",
    series_id="BC_10YEAR",
    name="Daily Treasury Par Yield Curve Rate, 10-Year",
    unit="Percent",
    frequency="Daily",
)


@dataclass(frozen=True, slots=True)
class Observation:
    source: str
    series_id: str
    observation_date: date
    value: Decimal
    unit: str
    frequency: str


@dataclass(frozen=True, slots=True)
class Rejection:
    record: Any
    reason: str


@dataclass
class NormalizationResult:
    observations: list[Observation] = field(default_factory=list)
    missing: list[date] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)


def _parse_decimal(raw: Any) -> Decimal:
    if not isinstance(raw, str):
        raise ValueError(f"expected a numeric string, got {type(raw).__name__}")
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"not a number: {raw!r}") from None
    if not value.is_finite():
        raise ValueError(f"non-finite number: {raw!r}")
    return value


def _parse_iso_date(raw: str) -> date:
    return date.fromisoformat(raw)


def _parse_iso_midnight(raw: str) -> date:
    """Treasury dates are ISO datetimes at midnight, e.g. 2026-09-01T00:00:00."""
    parsed = datetime.fromisoformat(raw)
    if parsed.time() != time(0) or parsed.tzinfo is not None:
        raise ValueError(f"expected a midnight datetime: {raw!r}")
    return parsed.date()


def _normalize(
    records: Iterable[Any],
    meta: SeriesMeta,
    *,
    date_field: str,
    value_field: str,
    parse_date: Callable[[str], date],
    missing_marker: Any,
    start: date | None,
    end: date | None,
) -> NormalizationResult:
    result = NormalizationResult()
    dated: list[tuple[date, Mapping]] = []

    for record in records:
        if not isinstance(record, Mapping):
            result.rejected.append(Rejection(record, "record is not an object"))
            continue
        absent = [f for f in (date_field, value_field) if f not in record]
        if absent:
            result.rejected.append(Rejection(record, f"missing field(s): {', '.join(absent)}"))
            continue
        try:
            obs_date = parse_date(record[date_field])
        except (TypeError, ValueError):
            result.rejected.append(Rejection(record, f"invalid date: {record[date_field]!r}"))
            continue
        if (start and obs_date < start) or (end and obs_date > end):
            continue  # outside the requested window; not an error
        dated.append((obs_date, record))

    counts = Counter(obs_date for obs_date, _ in dated)
    for obs_date, record in dated:
        if counts[obs_date] > 1:
            result.rejected.append(Rejection(record, f"duplicate date: {obs_date}"))
            continue
        raw_value = record[value_field]
        if raw_value == missing_marker:
            result.missing.append(obs_date)
            continue
        try:
            value = _parse_decimal(raw_value)
        except ValueError as exc:
            result.rejected.append(Rejection(record, f"invalid value: {exc}"))
            continue
        result.observations.append(
            Observation(meta.source, meta.series_id, obs_date, value, meta.unit, meta.frequency)
        )

    return result


def normalize_fred(
    records: Iterable[Any], start: date | None = None, end: date | None = None
) -> NormalizationResult:
    """FRED reports missing observations (e.g. market holidays) as the value "."."""
    return _normalize(
        records,
        FRED_DGS10,
        date_field="date",
        value_field="value",
        parse_date=_parse_iso_date,
        missing_marker=".",
        start=start,
        end=end,
    )


def normalize_treasury(
    records: Iterable[Any], start: date | None = None, end: date | None = None
) -> NormalizationResult:
    """Treasury omits unavailable rates; ingest represents that as None."""
    return _normalize(
        records,
        TREASURY_PAR_10Y,
        date_field="NEW_DATE",
        value_field="BC_10YEAR",
        parse_date=_parse_iso_midnight,
        missing_marker=None,
        start=start,
        end=end,
    )
