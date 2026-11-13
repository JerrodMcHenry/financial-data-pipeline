"""Extract raw observations from FRED and the U.S. Treasury.

This module only performs HTTP and response parsing. Records are returned in
their source shape; validation and normalization live in transform.py.
Error messages never include request URLs, so API keys cannot leak into logs.
"""

import json
import os
import xml.etree.ElementTree as ET
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
# Documented at https://home.treasury.gov/treasury-daily-interest-rate-xml-feed
TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
TIMEOUT_SECONDS = 30

_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}


class IngestError(RuntimeError):
    """Raised when a source cannot be retrieved or its response is unusable."""


def _http_get(url: str, params: dict[str, str], source: str) -> bytes:
    request = Request(f"{url}?{urlencode(params)}", headers={"User-Agent": "financial-pipeline/0.1"})
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise IngestError(f"{source}: unexpected HTTP status {response.status}")
            return response.read()
    except HTTPError as exc:
        detail = ""
        try:  # FRED returns {"error_message": "..."} on failures
            detail = f": {json.loads(exc.read())['error_message']}"
        except Exception:
            pass
        raise IngestError(f"{source}: HTTP {exc.code}{detail}") from None
    except (URLError, TimeoutError) as exc:
        reason = getattr(exc, "reason", exc)
        raise IngestError(f"{source}: network error: {reason}") from None


def fetch_fred(start: date, end: date, series_id: str = "DGS10") -> list[dict]:
    """Return FRED observation dicts ({"date": ..., "value": ...}) for [start, end]."""
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise IngestError("FRED: FRED_API_KEY is not set")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
    }
    body = _http_get(FRED_URL, params, "FRED")
    try:
        payload = json.loads(body)
    except ValueError:
        raise IngestError("FRED: response is not valid JSON") from None
    if not isinstance(payload, dict) or not isinstance(payload.get("observations"), list):
        raise IngestError("FRED: response is missing an 'observations' list")
    return payload["observations"]


def _months(start: date, end: date) -> list[str]:
    months, year, month = [], start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(f"{year}{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def _parse_treasury_feed(body: bytes) -> list[dict]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        raise IngestError("US_TREASURY: response is not valid XML") from None
    if root.tag != f"{{{_NS['atom']}}}feed":
        raise IngestError("US_TREASURY: response is not an Atom feed")

    rows = []
    for entry in root.findall("atom:entry", _NS):
        props = entry.find("atom:content/m:properties", _NS)
        if props is None:
            rows.append({})  # transform rejects this as missing fields
            continue
        row = {}
        date_el = props.find("d:NEW_DATE", _NS)
        if date_el is not None:
            row["NEW_DATE"] = date_el.text
        # Treasury omits a rate element when no value exists; treat that, or an
        # explicit m:null="true", as a source-reported missing value (None).
        rate_el = props.find("d:BC_10YEAR", _NS)
        is_null = rate_el is None or rate_el.get(f"{{{_NS['m']}}}null") == "true"
        row["BC_10YEAR"] = None if is_null else (rate_el.text or "")
        rows.append(row)
    return rows


def fetch_treasury(start: date, end: date) -> list[dict]:
    """Return {"NEW_DATE", "BC_10YEAR"} rows for every month touching [start, end].

    The feed's finest filter is a calendar month, so callers should pass the
    same window to normalize_treasury() to trim rows outside [start, end].
    """
    rows = []
    for month in _months(start, end):
        params = {"data": "daily_treasury_yield_curve", "field_tdr_date_value_month": month}
        rows.extend(_parse_treasury_feed(_http_get(TREASURY_URL, params, "US_TREASURY")))
    return rows


if __name__ == "__main__":
    import sys

    from financial_pipeline.transform import normalize_fred, normalize_treasury

    start, end = (date.fromisoformat(arg) for arg in sys.argv[1:3])
    for name, fetch, normalize in (
        ("FRED DGS10", fetch_fred, normalize_fred),
        ("Treasury BC_10YEAR", fetch_treasury, normalize_treasury),
    ):
        raw = fetch(start, end)
        result = normalize(raw, start, end)
        print(
            f"{name}: retrieved={len(raw)} valid={len(result.observations)} "
            f"missing={len(result.missing)} rejected={len(result.rejected)}"
        )
        for d in result.missing:
            print(f"  missing: {d}")
        for r in result.rejected:
            print(f"  rejected: {r.reason} -> {r.record!r}")
        for obs in result.observations[:3]:
            print(f"  {obs!r}")
