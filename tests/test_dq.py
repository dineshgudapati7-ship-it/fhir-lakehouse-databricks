import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "lib"))

import config  # noqa: E402
import dq  # noqa: E402
import silver  # noqa: E402

CFG = config.load_config()
KEYS = {r: silver.load_silver_mapping(r)["key"] for r in config.resource_names(CFG)}
CHECKS = dq.dq_checks("fhir", "123", KEYS, CFG["data_quality"])


def test_check_names_unique_per_table():
    ids = [(c["table_name"], c["check_name"]) for c in CHECKS]
    assert len(ids) == len(set(ids))


def test_every_resource_has_structural_checks_and_facts_have_gold_checks():
    for resource in KEYS:
        table = f"fhir.silver.{resource.lower()}"
        names = {c["check_name"] for c in CHECKS if c["table_name"] == table}
        assert {"duplicate_current_versions", "invalid_validity_window", "silver_keys_missing_in_bronze"} <= names
    for fact in dq.FACT_TABLES.values():
        names = {c["check_name"] for c in CHECKS if c["table_name"] == f"fhir.gold.{fact}"}
        assert names == {"gold_vs_silver_row_diff", "unknown_patient_rate"}


def test_null_rate_checks_come_from_config_and_only_warn():
    null_checks = [c for c in CHECKS if c["check_name"].startswith("null_rate:")]
    assert len(null_checks) == sum(len(v) for v in CFG["data_quality"]["max_null_rate"].values())
    assert all(c["severity"] == "warn" for c in null_checks)


def test_status():
    err = {"threshold": 0.0, "severity": "error"}
    warn = {"threshold": 0.1, "severity": "warn"}
    assert dq.dq_status(0, err) == "PASS"
    assert dq.dq_status(1, err) == "FAIL"
    assert dq.dq_status(0.05, warn) == "PASS"
    assert dq.dq_status(0.2, warn) == "WARN"
