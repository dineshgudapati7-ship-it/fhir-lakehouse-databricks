"""HAPI FHIR search client: retrying session, _lastUpdated windows and Bundle pagination."""
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FHIR_NS = {"f": "http://hl7.org/fhir"}
ACCEPT_HEADERS = {"json": "application/fhir+json", "xml": "application/fhir+xml"}


@dataclass
class Page:
    page_number: int          # 1-based
    request_url: str          # final URL including query string
    http_status: int
    text: str                 # response body exactly as received
    entries: int
    next_url: str | None
    max_last_updated: datetime | None
    call_started_at: datetime
    call_ended_at: datetime


class FhirFetchError(Exception):
    """Raised when a page cannot be fetched; carries enough context for the call log."""

    def __init__(self, page_number, request_url, call_started_at, cause, http_status=None):
        super().__init__(f"page {page_number} failed ({request_url}): {cause}")
        self.page_number = page_number
        self.request_url = request_url
        self.call_started_at = call_started_at
        self.http_status = http_status
        self.cause = cause


def utcnow():
    return datetime.now(timezone.utc)


def build_session(api_cfg):
    session = requests.Session()
    retry = Retry(total=api_cfg["max_retries"], backoff_factor=api_cfg["backoff_factor"],
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"],
                  respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def to_fhir_instant(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_search_params(start, end, page_size, fmt):
    """List of tuples so _lastUpdated is sent twice (ge + lt), which is how FHIR expresses a range."""
    params = [("_count", page_size), ("_sort", "_lastUpdated"), ("_format", fmt)]
    if start:
        params.append(("_lastUpdated", f"ge{to_fhir_instant(start)}"))
    if end:
        params.append(("_lastUpdated", f"lt{to_fhir_instant(end)}"))
    return params


def _parse_instant(value):
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_bundle(text, fmt):
    """Return (entry_count, next_url, max meta.lastUpdated) for a searchset Bundle."""
    if fmt == "json":
        bundle = json.loads(text)
        entries = bundle.get("entry", [])
        next_url = next((l.get("url") for l in bundle.get("link", [])
                         if l.get("relation") == "next"), None)
        updated = [_parse_instant(e.get("resource", {}).get("meta", {}).get("lastUpdated"))
                   for e in entries]
    else:
        root = ET.fromstring(text)
        entries = root.findall("f:entry", FHIR_NS)
        next_url = None
        for link in root.findall("f:link", FHIR_NS):
            relation = link.find("f:relation", FHIR_NS)
            if relation is not None and relation.get("value") == "next":
                next_url = link.find("f:url", FHIR_NS).get("value")
                break
        updated = [_parse_instant(el.get("value"))
                   for el in root.findall("f:entry/f:resource/*/f:meta/f:lastUpdated", FHIR_NS)]
    updated = [u for u in updated if u]
    return len(entries), next_url, max(updated, default=None)


def fetch_pages(session, base_url, resource, params, fmt, max_pages, timeout):
    """Yield one Page per Bundle page, following link[relation=next] until exhausted or max_pages.

    Pages are fetched sequentially: HAPI's _getpages tokens expire, so don't parallelize.
    """
    url = f"{base_url.rstrip('/')}/{resource}"
    page_number = 1
    headers = {"Accept": ACCEPT_HEADERS[fmt]}
    while url and page_number <= max_pages:
        started = utcnow()
        try:
            resp = session.get(url, params=params if page_number == 1 else None,
                               headers=headers, timeout=timeout)
            resp.raise_for_status()
            entries, next_url, max_updated = parse_bundle(resp.text, fmt)
        except requests.HTTPError as e:
            raise FhirFetchError(page_number, e.response.url, started, e, e.response.status_code) from e
        except (requests.RequestException, ValueError, ET.ParseError) as e:
            raise FhirFetchError(page_number, url, started, e) from e
        yield Page(page_number, resp.url, resp.status_code, resp.text, entries, next_url,
                   max_updated, started, utcnow())
        url, page_number = next_url, page_number + 1
