import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "lib"))

import config  # noqa: E402
import silver  # noqa: E402


@pytest.mark.parametrize("resource", config.resource_names(config.load_config()))
def test_every_configured_resource_has_a_valid_mapping(resource):
    mapping = silver.load_silver_mapping(resource)
    assert mapping["key"] in mapping["columns"]


def test_mapping_validation(tmp_path):
    bad = tmp_path / "m.yaml"
    bad.write_text("Patient:\n  key: patient_id\n  columns:\n    patient_id: {expr: id, type: money}\n")
    with pytest.raises(ValueError, match="unknown type"):
        silver.load_silver_mapping("Patient", bad)
    bad.write_text("Patient:\n  key: nope\n  columns:\n    patient_id: {expr: id}\n")
    with pytest.raises(ValueError, match="must be one of its columns"):
        silver.load_silver_mapping("Patient", bad)


def test_clean_sql_types_and_rules():
    assert silver.clean_sql("gender", "lower") == "lower(nullif(trim(cast((gender) AS STRING)), ''))"
    assert "try_cast" in silver.clean_sql("birthDate", "date")
    assert "regexp_extract" in silver.clean_sql("subject.reference", "reference")
    sql = silver.clean_sql("birthDate", "date", "not_future")
    assert sql.startswith("CASE WHEN") and "current_date()" in sql
    assert "current_timestamp()" in silver.clean_sql("onsetDateTime", "timestamp", "not_future")
