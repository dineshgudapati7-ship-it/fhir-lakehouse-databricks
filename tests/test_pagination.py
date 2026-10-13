import json
import os
import sys
from datetime import datetime, timezone

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fhir_lib import api_client, audit, config, raw_writer  # noqa: E402

BASE = "https://example.org/fhir"


def json_bundle(ids, next_url=None, last_updated="2026-10-10T08:00:00.000+00:00"):
    bundle = {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": [{"relation": "self", "url": "self"}],
        "entry": [{"resource": {"resourceType": "Patient", "id": i,
                                "meta": {"versionId": "1", "lastUpdated": last_updated}}}
                  for i in ids],
    }
    if next_url:
        bundle["link"].append({"relation": "next", "url": next_url})
    return json.dumps(bundle)


class FakeResponse:
    def __init__(self, url, text, status=200):
        self.url, self.text, self.status_code = url, text, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)


class FakeSession:
    def __init__(self, responses):
        self.responses = responses   # url -> (text, status)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        text, status = self.responses[url]
        return FakeResponse(url, text, status)


def fetch(session, max_pages=10, fmt="json"):
    params = api_client.build_search_params(None, None, 2, fmt)
    return list(api_client.fetch_pages(session, BASE, "Patient", params, fmt, max_pages, 30))


def test_single_page_without_next_link():
    session = FakeSession({f"{BASE}/Patient": (json_bundle(["a", "b"]), 200)})
    pages = fetch(session)
    assert len(pages) == 1
    assert pages[0].page_number == 1
    assert pages[0].entries == 2
    assert pages[0].next_url is None


def test_follows_next_links_and_sends_params_only_on_first_page():
    session = FakeSession({
        f"{BASE}/Patient": (json_bundle(["a", "b"], next_url="p2"), 200),
        "p2": (json_bundle(["c", "d"], next_url="p3"), 200),
        "p3": (json_bundle(["e"]), 200),
    })
    pages = fetch(session)
    assert [p.page_number for p in pages] == [1, 2, 3]
    assert sum(p.entries for p in pages) == 5
    assert session.calls[0][1] is not None
    assert all(params is None for _, params in session.calls[1:])


def test_stops_at_max_pages_and_keeps_next_link():
    session = FakeSession({
        f"{BASE}/Patient": (json_bundle(["a"], next_url="p2"), 200),
        "p2": (json_bundle(["b"], next_url="p3"), 200),
    })
    pages = fetch(session, max_pages=2)
    assert len(pages) == 2
    assert pages[-1].next_url == "p3"   # caller uses this to flag TRUNCATED


def test_empty_bundle():
    session = FakeSession({f"{BASE}/Patient": (json_bundle([]), 200)})
    pages = fetch(session)
    assert pages[0].entries == 0
    assert pages[0].max_last_updated is None


def test_http_error_raises_fetch_error_with_context():
    session = FakeSession({
        f"{BASE}/Patient": (json_bundle(["a"], next_url="p2"), 200),
        "p2": ("gone", 410),
    })
    with pytest.raises(api_client.FhirFetchError) as exc:
        fetch(session)
    assert exc.value.page_number == 2
    assert exc.value.http_status == 410
    row = audit.call_log_row("r1", "Patient", None, status="FAILED", error=exc.value)
    assert row["status"] == "FAILED" and row["page_number"] == 2 and row["error_message"]


def test_max_last_updated_across_entries():
    text = json.dumps({"resourceType": "Bundle", "entry": [
        {"resource": {"meta": {"lastUpdated": "2026-10-10T08:00:00Z"}}},
        {"resource": {"meta": {"lastUpdated": "2026-10-10T09:30:00.123+00:00"}}},
        {"resource": {}},
    ]})
    _, _, max_updated = api_client.parse_bundle(text, "json")
    assert max_updated == datetime(2026, 10, 10, 9, 30, 0, 123000, tzinfo=timezone.utc)


def test_parse_xml_bundle():
    text = """<Bundle xmlns="http://hl7.org/fhir">
      <type value="searchset"/>
      <link><relation value="self"/><url value="self"/></link>
      <link><relation value="next"/><url value="https://x/next"/></link>
      <entry><resource><Patient><id value="a"/>
        <meta><lastUpdated value="2026-10-10T08:00:00Z"/></meta></Patient></resource></entry>
      <entry><resource><Patient><id value="b"/>
        <meta><lastUpdated value="2026-10-11T08:00:00Z"/></meta></Patient></resource></entry>
    </Bundle>"""
    entries, next_url, max_updated = api_client.parse_bundle(text, "xml")
    assert entries == 2
    assert next_url == "https://x/next"
    assert max_updated == datetime(2026, 10, 11, 8, tzinfo=timezone.utc)


def test_search_params_express_lastupdated_range():
    start = datetime(2026, 10, 10, tzinfo=timezone.utc)
    end = datetime(2026, 10, 11, tzinfo=timezone.utc)
    params = api_client.build_search_params(start, end, 100, "json")
    assert ("_lastUpdated", "ge2026-10-10T00:00:00Z") in params
    assert ("_lastUpdated", "lt2026-10-11T00:00:00Z") in params
    assert ("_sort", "_lastUpdated") in params


def test_raw_file_path_layout():
    path = raw_writer.raw_file_path("/Volumes/fhir/raw/landing", "Patient", "json",
                                    "2026-10-10", "123", 7)
    assert path == "/Volumes/fhir/raw/landing/Patient/json/load_date=2026-10-10/run_id=123/page_0007.json"


def test_config_catalog_override():
    cfg = config.load_config(catalog="dev_fhir")
    assert cfg["storage"]["raw_volume_path"] == "/Volumes/dev_fhir/raw/landing"
    assert config.get_resource(cfg, "patient")["name"] == "Patient"
    with pytest.raises(ValueError):
        config.resolve_format(cfg, "csv")
