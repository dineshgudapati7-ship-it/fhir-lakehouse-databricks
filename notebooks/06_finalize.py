# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Finalize
# MAGIC Last task of the job, in one of two modes:
# MAGIC - `outcome = success` (runs only if every task succeeded): advance `ops.watermark` per resource to
# MAGIC   the newest `meta.lastUpdated` loaded in this run, and mark the run SUCCEEDED.
# MAGIC - `outcome = failed` (runs if any task failed): mark the run FAILED. Watermarks stay put, so the
# MAGIC   next incremental run re-reads the same window.

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/audit

# COMMAND ----------

# MAGIC %run ./lib/bronze

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("outcome", "success", ["success", "failed"])

cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
run_id = validate_run_id(dbutils.widgets.get("run_id").strip())
outcome = dbutils.widgets.get("outcome")

# COMMAND ----------

if outcome == "success":
    for resource in resource_names(cfg):
        table = bronze_table(catalog, resource)
        if not spark.catalog.tableExists(table):
            continue
        max_ts = spark.sql(f"SELECT max(try_cast(meta.lastUpdated AS TIMESTAMP)) FROM {table} WHERE _run_id = :run_id",
                           args={"run_id": run_id}).first()[0]
        update_watermark(spark, catalog, resource, max_ts, run_id)
        print(f"{resource}: newest lastUpdated in this run = {max_ts}")
    finish_pipeline_run(spark, catalog, run_id, "SUCCEEDED")
else:
    finish_pipeline_run(spark, catalog, run_id, "FAILED")

# COMMAND ----------

display(spark.table(f"{catalog}.ops.watermark"))
display(spark.table(f"{catalog}.ops.pipeline_run").filter(f"run_id = '{run_id}'"))
