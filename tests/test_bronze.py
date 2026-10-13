import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fhir_lib import bronze  # noqa: E402

XML_BUNDLE = """<Bundle xmlns="http://hl7.org/fhir">
  <type value="searchset"/>
  <entry><resource><Patient><id value="a"/>
    <meta><versionId value="2"/><lastUpdated value="2026-10-10T08:00:00Z"/></meta>
    <gender value="female"/></Patient></resource></entry>
  <entry><resource><OperationOutcome><id value="oo"/></OperationOutcome></resource></entry>
</Bundle>"""


def test_split_xml_bundle_extracts_each_resource():
    records = bronze.split_xml_bundle(XML_BUNDLE)
    assert [r[0] for r in records] == ["Patient", "OperationOutcome"]
    rtype, rid, version, updated, raw = records[0]
    assert (rid, version, updated) == ("a", "2", "2026-10-10T08:00:00Z")
    assert raw.startswith('<Patient xmlns="http://hl7.org/fhir">')   # no ns0: prefixes
    assert '<gender value="female" />' in raw
    assert records[1][2] is None and records[1][3] is None


def test_split_xml_bundle_empty():
    assert bronze.split_xml_bundle("") == []
    assert bronze.split_xml_bundle('<Bundle xmlns="http://hl7.org/fhir"/>') == []


def test_bronze_table_name_and_run_id_guard():
    assert bronze.bronze_table("fhir", "Observation") == "fhir.bronze.observation"
    assert bronze.validate_run_id("123456") == "123456"
    with pytest.raises(ValueError):
        bronze.validate_run_id("1' OR '1'='1")
