# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/dq
# MAGIC Data quality checks as SQL returning one number each, compared with `metric <= threshold`.
# MAGIC
# MAGIC Severity `error` = the pipeline itself is wrong (broken SCD2, lost or duplicated rows): FAIL stops
# MAGIC the run. Severity `warn` = the source data is incomplete: recorded as WARN, run continues.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/dq`.

# COMMAND ----------

FACT_TABLES = {"Encounter": "fact_encounter", "Observation": "fact_observation", "Condition": "fact_condition"}


def _check(name, table, sql, threshold, severity, description):
    return {"check_name": name, "table_name": table, "sql": sql, "threshold": float(threshold),
            "severity": severity, "description": description}


def dq_checks(catalog, run_id, keys, dq_cfg):
    """Build the check list. keys: {resource: silver business key}; dq_cfg: config['data_quality'].
    run_id must already be validated (it is embedded in SQL)."""
    checks = []
    for resource, key in keys.items():
        silver = f"{catalog}.silver.{resource.lower()}"
        bronze = f"{catalog}.bronze.{resource.lower()}"

        checks.append(_check(
            "duplicate_current_versions", silver,
            f"SELECT count(*) FROM (SELECT `{key}` FROM {silver} WHERE is_current GROUP BY 1 HAVING count(*) > 1)",
            0, "error", "keys with more than one is_current row"))
        checks.append(_check(
            "invalid_validity_window", silver,
            f"SELECT count_if(effective_to IS NOT NULL AND effective_to < effective_from) FROM {silver}",
            0, "error", "SCD2 versions that end before they start"))
        checks.append(_check(
            "silver_keys_missing_in_bronze", silver,
            f"SELECT count(*) FROM (SELECT DISTINCT `{key}` AS k FROM {silver}) s "
            f"LEFT ANTI JOIN (SELECT DISTINCT id FROM {bronze}) b ON s.k = b.id",
            0, "error", "Silver keys with no Bronze record (lineage broken)"))
        checks.append(_check(
            "api_vs_bronze_row_diff", bronze,
            f"SELECT abs(coalesce((SELECT sum(entries_in_page) FROM {catalog}.ops.api_call_log "
            f"WHERE run_id = '{run_id}' AND resource = '{resource}' AND status IN ('SUCCESS', 'TRUNCATED')), 0) "
            f"- (SELECT count(*) FROM {bronze} WHERE _run_id = '{run_id}'))",
            0, "error", "records returned by the API in this run vs rows landed in Bronze"))

        for column, max_rate in dq_cfg.get("max_null_rate", {}).get(resource, {}).items():
            checks.append(_check(
                f"null_rate:{column}", silver,
                f"SELECT coalesce(avg(CASE WHEN `{column}` IS NULL THEN 1.0 ELSE 0.0 END), 0) "
                f"FROM {silver} WHERE is_current",
                max_rate, "warn", f"share of current rows with NULL {column}"))

        fact = FACT_TABLES.get(resource)
        if fact:
            gold_fact = f"{catalog}.gold.{fact}"
            checks.append(_check(
                "gold_vs_silver_row_diff", gold_fact,
                f"SELECT abs((SELECT count(*) FROM {gold_fact}) - "
                f"(SELECT count(*) FROM {silver} WHERE is_current))",
                0, "error", "fact rows vs current Silver rows (patient join must not drop or duplicate)"))
            checks.append(_check(
                "unknown_patient_rate", gold_fact,
                f"SELECT coalesce(avg(CASE WHEN patient_sk = -1 THEN 1.0 ELSE 0.0 END), 0) FROM {gold_fact}",
                dq_cfg.get("max_unknown_patient_rate", 0.1), "warn",
                "share of facts referencing a patient that was never loaded"))
    return checks


def dq_status(metric, check):
    if metric <= check["threshold"]:
        return "PASS"
    return "FAIL" if check["severity"] == "error" else "WARN"
