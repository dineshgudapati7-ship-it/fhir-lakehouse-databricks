# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality
# MAGIC Runs the checks from `lib/dq` across Bronze, Silver and Gold and writes one row per check to
# MAGIC `ops.dq_results`. Any FAIL (pipeline correctness) fails the task, so the run is marked FAILED and
# MAGIC the watermark is not advanced. WARN (incomplete source data) is recorded only.

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/bronze

# COMMAND ----------

# MAGIC %run ./lib/silver

# COMMAND ----------

# MAGIC %run ./lib/dq

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("run_id", "")

cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
run_id = validate_run_id(dbutils.widgets.get("run_id").strip()
                         or f"manual_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}")
keys = {r: load_silver_mapping(r)["key"] for r in resource_names(cfg)}

# COMMAND ----------

results = []
for check in dq_checks(catalog, run_id, keys, cfg.get("data_quality", {})):
    try:
        metric = float(spark.sql(check["sql"]).first()[0] or 0)
        status, details = dq_status(metric, check), check["description"]
    except Exception as e:   # e.g. a table not built yet: record it rather than hide it
        metric = None
        status = "FAIL" if check["severity"] == "error" else "WARN"
        details = f"check could not run: {e}"[:1000]
    results.append({"run_id": run_id, "check_name": check["check_name"], "table_name": check["table_name"],
                    "status": status, "metric_value": metric, "threshold": check["threshold"],
                    "details": details, "checked_at": datetime.now(timezone.utc)})

# Rerunning DQ for the same run replaces its results.
spark.sql(f"DELETE FROM {catalog}.ops.dq_results WHERE run_id = :run_id", args={"run_id": run_id})
(spark.createDataFrame(results, "run_id STRING, check_name STRING, table_name STRING, status STRING, "
                                "metric_value DOUBLE, threshold DOUBLE, details STRING, checked_at TIMESTAMP")
 .write.mode("append").saveAsTable(f"{catalog}.ops.dq_results"))

# COMMAND ----------

display(spark.table(f"{catalog}.ops.dq_results").filter(f"run_id = '{run_id}'")
        .orderBy("status", "table_name", "check_name"))

failed = [f"{r['table_name']}.{r['check_name']}" for r in results if r["status"] == "FAIL"]
warned = sum(r["status"] == "WARN" for r in results)
print(f"{len(results)} checks: {len(failed)} FAIL, {warned} WARN")
if failed:
    raise RuntimeError(f"Data quality checks failed: {failed}")
