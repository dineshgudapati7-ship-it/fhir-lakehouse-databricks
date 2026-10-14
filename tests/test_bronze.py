import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "lib"))

import bronze  # noqa: E402


def test_bronze_table_name():
    assert bronze.bronze_table("fhir", "Observation") == "fhir.bronze.observation"


def test_run_id_guard():
    assert bronze.validate_run_id("123456") == "123456"
    with pytest.raises(ValueError):
        bronze.validate_run_id("1' OR '1'='1")
