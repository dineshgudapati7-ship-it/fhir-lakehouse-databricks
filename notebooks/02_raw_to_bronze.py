# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Raw to Bronze
# MAGIC Loads the JSON pages saved by `01_ingest_raw` for one `run_id` + `resource` into
# MAGIC `bronze.<resource>`: one row per FHIR resource, its fields as columns (nested parts stay as
# MAGIC structs/arrays), plus lineage columns prefixed with `_`.
# MAGIC
# MAGIC Bronze keeps every version across runs; rerunning the same `run_id` replaces that run's rows.

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/bronze

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("resource", "Patient")
dbutils.widgets.text("run_id", "")

cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
resource = get_resource(cfg, dbutils.widgets.get("resource").strip())["name"]
run_id = validate_run_id(dbutils.widgets.get("run_id").strip())
table = bronze_table(catalog, resource)

call_log = run_call_log(spark, catalog, resource, run_id)
files = [r.raw_file_path for r in call_log.select("raw_file_path").collect()
         if r.raw_file_path.endswith(".json")]
print(f"run_id={run_id} resource={resource} files={len(files)} -> {table}")

# COMMAND ----------

resources = read_resources(spark, files, resource) if files else None
if resources is None:
    dbutils.jobs.taskValues.set(key="bronze_rows", value=0)
    dbutils.notebook.exit("no records for this run")

df = add_lineage(resources, call_log)
if df.filter(F.col("_run_id").isNull()).limit(1).count():
    raise RuntimeError("Some rows could not be matched to ops.api_call_log by file path")

write_bronze(spark, df, table, run_id)

# COMMAND ----------

loaded = spark.table(table).filter(F.col("_run_id") == run_id)
bronze_rows = loaded.count()
api_entries = call_log.agg(F.sum("entries_in_page")).first()[0] or 0
print(f"api_entries={api_entries} bronze_rows={bronze_rows}")
dbutils.jobs.taskValues.set(key="bronze_rows", value=bronze_rows)

display(loaded.limit(20))
