import io
import json
import traceback
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError, URLError

import psycopg
import pytest

from financial_pipeline import database, ingest, main
from financial_pipeline.database import LoadResult, load
from financial_pipeline.ingest import IngestError, fetch_fred, fetch_treasury
from financial_pipeline.transform import (
    FRED_DGS10,
    TREASURY_PAR_10Y,
    Observation,
    normalize_fred,
    normalize_treasury,
)

FAKE_KEY = "abcdefghijklmnopqrstuvwxyz012345"


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body, self.status = body, status

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def mock_urlopen(monkeypatch, outcome):
    """Patch urlopen to return a body (bytes) or raise an exception."""

    def fake(request, timeout):
        assert timeout == ingest.TIMEOUT_SECONDS
        if isinstance(outcome, Exception):
            raise outcome
        return FakeResponse(outcome)

    monkeypatch.setattr(ingest, "urlopen", fake)


def treasury_entry(new_date: str | None, rate_xml: str) -> str:
    date_xml = f'<d:NEW_DATE m:type="Edm.DateTime">{new_date}</d:NEW_DATE>' if new_date else ""
    return f"<entry><content><m:properties>{date_xml}{rate_xml}</m:properties></content></entry>"


def treasury_feed(*entries: str) -> bytes:
    return (
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" '
        'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
        + "".join(entries)
        + "</feed>"
    ).encode()


# --- Normalization -----------------------------------------------------------


def test_fred_valid_and_missing(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", FAKE_KEY)
    body = {"observations": [
        {"date": "2026-09-04", "value": "4.72"},
        {"date": "2026-09-07", "value": "."},  # Labor Day: source-reported missing
    ]}
    mock_urlopen(monkeypatch, json.dumps(body).encode())

    result = normalize_fred(fetch_fred(date(2026, 9, 1), date(2026, 9, 30)))

    assert result.observations == [
        Observation("FRED", "DGS10", date(2026, 9, 4), Decimal("4.72"), "Percent", "Daily")
    ]
    assert result.missing == [date(2026, 9, 7)]
    assert result.rejected == []


def test_treasury_valid_missing_and_window(monkeypatch):
    rate = '<d:BC_10YEAR m:type="Edm.Double">{}</d:BC_10YEAR>'
    feed = treasury_feed(
        treasury_entry("2026-09-01T00:00:00", rate.format("4.79")),
        treasury_entry("2026-09-02T00:00:00", ""),  # rate omitted by source
        treasury_entry("2026-09-03T00:00:00", '<d:BC_10YEAR m:type="Edm.Double" m:null="true" />'),
        treasury_entry("2026-09-30T00:00:00", rate.format("4.60")),  # outside window
    )
    mock_urlopen(monkeypatch, feed)

    start, end = date(2026, 9, 1), date(2026, 9, 15)
    result = normalize_treasury(fetch_treasury(start, end), start, end)

    assert result.observations == [
        Observation("US_TREASURY", "BC_10YEAR", date(2026, 9, 1), Decimal("4.79"), "Percent", "Daily")
    ]
    assert result.missing == [date(2026, 9, 2), date(2026, 9, 3)]
    assert result.rejected == []


@pytest.mark.parametrize("bad_date", ["2026-13-01", "09/01/2026", "", None])
def test_malformed_date_rejected(bad_date):
    result = normalize_fred([{"date": bad_date, "value": "4.1"}])
    assert result.observations == []
    assert "invalid date" in result.rejected[0].reason


def test_treasury_non_midnight_datetime_rejected():
    result = normalize_treasury([{"NEW_DATE": "2026-09-01T15:30:00", "BC_10YEAR": "4.1"}])
    assert "invalid date" in result.rejected[0].reason


@pytest.mark.parametrize("bad_value", ["abc", "", "NaN", "Infinity", None, 4.1])
def test_invalid_numeric_rejected_not_zeroed(bad_value):
    result = normalize_fred([{"date": "2026-09-01", "value": bad_value}])
    assert result.observations == [] and result.missing == []
    assert "invalid value" in result.rejected[0].reason


@pytest.mark.parametrize(
    "record, expected",
    [({"value": "4.1"}, "date"), ({"date": "2026-09-01"}, "value"), ("not-an-object", "not an object")],
)
def test_missing_required_fields_rejected(record, expected):
    result = normalize_fred([record])
    assert expected in result.rejected[0].reason


def test_treasury_entry_without_date_rejected():
    rows = ingest._parse_treasury_feed(treasury_feed(treasury_entry(None, "")))
    assert "missing field(s): NEW_DATE" in normalize_treasury(rows).rejected[0].reason


def test_duplicate_dates_all_rejected():
    records = [
        {"date": "2026-09-01", "value": "4.10"},
        {"date": "2026-09-01", "value": "4.20"},
        {"date": "2026-09-02", "value": "4.30"},
    ]
    result = normalize_fred(records)
    assert [o.observation_date for o in result.observations] == [date(2026, 9, 2)]
    assert [r.reason for r in result.rejected] == ["duplicate date: 2026-09-01"] * 2


# --- HTTP and response failures ---------------------------------------------


@pytest.mark.parametrize(
    "outcome, message",
    [
        (HTTPError(ingest.FRED_URL, 400, "Bad Request", {},
                   io.BytesIO(b'{"error_message": "Bad api_key"}')), "HTTP 400: Bad api_key"),
        (HTTPError(ingest.FRED_URL, 503, "Unavailable", {}, io.BytesIO(b"<html>")), "HTTP 503"),
        (URLError("connection refused"), "network error"),
        (TimeoutError("timed out"), "network error"),
        (b"<html>not json</html>", "not valid JSON"),
        (b'{"error": "no observations key"}', "missing an 'observations' list"),
    ],
)
def test_fred_failures_raise_without_leaking_key(monkeypatch, outcome, message):
    monkeypatch.setenv("FRED_API_KEY", FAKE_KEY)
    mock_urlopen(monkeypatch, outcome)
    with pytest.raises(IngestError, match=message) as exc_info:
        fetch_fred(date(2026, 9, 1), date(2026, 9, 30))
    assert FAKE_KEY not in "".join(traceback.format_exception(exc_info.value))


def test_fred_requires_api_key(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(IngestError, match="FRED_API_KEY is not set"):
        fetch_fred(date(2026, 9, 1), date(2026, 9, 30))


@pytest.mark.parametrize(
    "body, message", [(b"<html><unclosed>", "not valid XML"), (b"<html></html>", "not an Atom feed")]
)
def test_treasury_malformed_response(monkeypatch, body, message):
    mock_urlopen(monkeypatch, body)
    with pytest.raises(IngestError, match=message):
        fetch_treasury(date(2026, 9, 1), date(2026, 9, 30))


def test_treasury_requests_one_feed_per_month(monkeypatch):
    requested = []

    def fake(request, timeout):
        requested.append(request.full_url)
        return FakeResponse(treasury_feed())

    monkeypatch.setattr(ingest, "urlopen", fake)
    fetch_treasury(date(2026, 11, 20), date(2027, 1, 5))
    assert [url[-6:] for url in requested] == ["202611", "202612", "202701"]


# --- PostgreSQL loading (integration; needs DATABASE_URL) ---------------------
#
# Each test runs in a throwaway schema that is dropped afterwards, so the real
# tables in the target database are never read or modified.

SCHEMA_SQL = (Path(__file__).parents[1] / "sql" / "schema.sql").read_text()


@pytest.fixture
def db_connect(monkeypatch):
    """Return a connect(**kw) factory bound to a fresh schema with the pipeline tables."""
    try:
        url = database.get_database_url()
        admin = psycopg.connect(url, autocommit=True)
    except (database.DatabaseConfigError, psycopg.OperationalError) as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
    schema = f"test_{uuid.uuid4().hex[:12]}"
    admin.execute(f'CREATE SCHEMA "{schema}"')

    def connect(**kwargs):
        return psycopg.connect(url, options=f"-c search_path={schema}", **kwargs)

    try:
        with connect(autocommit=True) as conn:
            conn.execute(SCHEMA_SQL)
        monkeypatch.setattr(database, "connect", connect)
        yield connect
    finally:
        admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        admin.close()


def obs(meta, day, value):
    return Observation(meta.source, meta.series_id, date(2026, 9, day), Decimal(value),
                       meta.unit, meta.frequency)


def table_counts(conn):
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in ("sources", "series", "observations")}


def test_load_first_run_inserts(db_connect):
    fred = [obs(FRED_DGS10, 1, "4.10"), obs(FRED_DGS10, 2, "4.20")]
    tsy = [obs(TREASURY_PAR_10Y, 1, "4.11")]
    with db_connect(autocommit=True) as conn:
        result = load(conn, [(FRED_DGS10, fred), (TREASURY_PAR_10Y, tsy)])
        assert result == {"FRED": LoadResult(2, 0, 0), "US_TREASURY": LoadResult(1, 0, 0)}
        assert table_counts(conn) == {"sources": 2, "series": 2, "observations": 3}
        orphans = conn.execute("""
            SELECT count(*) FROM observations o
            LEFT JOIN series s USING (series_id)
            LEFT JOIN sources src ON src.source_id = s.source_id
            WHERE src.source_id IS NULL""").fetchone()[0]
        assert orphans == 0


def test_load_repeated_run_is_idempotent(db_connect):
    data = [(FRED_DGS10, [obs(FRED_DGS10, 1, "4.10"), obs(FRED_DGS10, 2, "4.20")])]
    with db_connect(autocommit=True) as conn:
        load(conn, data)
        before = conn.execute("SELECT * FROM observations ORDER BY 1, 2").fetchall()
        assert load(conn, data) == {"FRED": LoadResult(0, 0, 2)}
        after = conn.execute("SELECT * FROM observations ORDER BY 1, 2").fetchall()
        assert after == before  # no duplicates; fetched_at untouched
        assert table_counts(conn) == {"sources": 1, "series": 1, "observations": 2}


def test_load_changed_value_updates_and_keeps_omitted_rows(db_connect):
    with db_connect(autocommit=True) as conn:
        load(conn, [(FRED_DGS10, [obs(FRED_DGS10, 1, "4.10"), obs(FRED_DGS10, 2, "4.20"),
                                  obs(FRED_DGS10, 3, "4.30")])])
        old = dict(conn.execute("SELECT observation_date, fetched_at FROM observations").fetchall())

        # Day 1 revised, day 2 unchanged, day 3 omitted by the source, day 4 new.
        result = load(conn, [(FRED_DGS10, [obs(FRED_DGS10, 1, "4.15"), obs(FRED_DGS10, 2, "4.20"),
                                           obs(FRED_DGS10, 4, "4.40")])])
        assert result == {"FRED": LoadResult(inserted=1, updated=1, skipped=1)}

        rows = {d: (v, f) for d, v, f in conn.execute(
            "SELECT observation_date, value, fetched_at FROM observations")}
        assert rows[date(2026, 9, 1)][0] == Decimal("4.15")
        assert rows[date(2026, 9, 1)][1] > old[date(2026, 9, 1)]   # refreshed
        assert rows[date(2026, 9, 2)][1] == old[date(2026, 9, 2)]  # untouched
        assert rows[date(2026, 9, 3)][0] == Decimal("4.30")        # not deleted
        assert len(rows) == 4


def test_load_failure_rolls_back_everything(db_connect):
    with db_connect(autocommit=True) as conn:
        load(conn, [(FRED_DGS10, [obs(FRED_DGS10, 1, "4.10")])])
        before = conn.execute("SELECT * FROM observations").fetchall()

        # FRED is written first (a revision plus a new row), then Treasury
        # violates NOT NULL on value. Nothing from this load may survive.
        bad = Observation("US_TREASURY", "BC_10YEAR", date(2026, 9, 1), None, "Percent", "Daily")
        with pytest.raises(psycopg.errors.NotNullViolation):
            load(conn, [(FRED_DGS10, [obs(FRED_DGS10, 1, "9.99"), obs(FRED_DGS10, 2, "4.20")]),
                        (TREASURY_PAR_10Y, [bad])])

        assert conn.execute("SELECT * FROM observations").fetchall() == before
        assert table_counts(conn) == {"sources": 1, "series": 1, "observations": 1}


def test_cli_summary_counts_across_runs(db_connect, monkeypatch, capsys):
    fred_raw = [{"date": "2026-09-01", "value": "4.10"}, {"date": "2026-09-02", "value": "4.20"},
                {"date": "2026-09-03", "value": "."}, {"date": "2026-09-04", "value": "abc"}]
    tsy_raw = [{"NEW_DATE": "2026-09-01T00:00:00", "BC_10YEAR": "4.11"},
               {"NEW_DATE": "2026-09-02T00:00:00", "BC_10YEAR": None},
               {"NEW_DATE": "2026-09-30T00:00:00", "BC_10YEAR": "4.50"}]  # outside window

    def run_cli():
        monkeypatch.setattr(main, "SOURCES", (
            (FRED_DGS10, lambda s, e: list(fred_raw), normalize_fred),
            (TREASURY_PAR_10Y, lambda s, e: list(tsy_raw), normalize_treasury),
        ))
        assert main.main(["2026-09-01", "2026-09-04"]) == 0
        return capsys.readouterr().out

    out = run_cli()
    assert ("FRED         DGS10      fetched=4 valid=2 missing=1 rejected=1 "
            "inserted=2 updated=0 skipped=0") in out
    assert ("US_TREASURY  BC_10YEAR  fetched=3 valid=1 missing=1 rejected=0 "
            "inserted=1 updated=0 skipped=0") in out
    assert "outside requested window: 1" in out
    assert "weekday gaps (may be holidays/unpublished): 2026-09-03, 2026-09-04" in out

    fred_raw[1]["value"] = "4.25"  # source revises one value
    out = run_cli()
    assert "fetched=4 valid=2 missing=1 rejected=1 inserted=0 updated=1 skipped=1" in out
    assert "fetched=3 valid=1 missing=1 rejected=0 inserted=0 updated=0 skipped=1" in out


@pytest.mark.parametrize("argv", [["2026-09-30", "2026-09-01"], ["2026-09-01", "09/30/2026"],
                                  ["2026-09-01", "2999-01-01"]])
def test_cli_rejects_bad_date_range(argv):
    with pytest.raises(SystemExit) as exc_info:
        main.main(argv)
    assert exc_info.value.code == 2
