# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Raw to Bronze
# MAGIC Loads the raw pages saved by `01_ingest_raw` for one `run_id` + `resource` into
# MAGIC `bronze.<resource>`. The file list comes from `ops.api_call_log`, which is also the
# MAGIC source of lineage (extraction timestamp, request URL, page number).
# MAGIC
# MAGIC Bronze is append-only across runs; rerunning the same `run_id` replaces that run's rows.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from fhir_lib import bronze, config

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("resource", "Patient")
dbutils.widgets.text("run_id", "")

cfg = config.load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
resource = config.get_resource(cfg, dbutils.widgets.get("resource").strip())["name"]
run_id = bronze.validate_run_id(dbutils.widgets.get("run_id").strip())
table = bronze.bronze_table(catalog, resource)
print(f"run_id={run_id} resource={resource} -> {table}")

# COMMAND ----------

call_log = bronze.run_call_log(spark, catalog, resource, run_id).cache()
files = [r.raw_file_path for r in call_log.select("raw_file_path").collect()]
json_files = [f for f in files if f.endswith(".json")]
xml_files = [f for f in files if f.endswith(".xml")]
print(f"files: json={len(json_files)} xml={len(xml_files)}")

bronze.ensure_bronze_table(spark, table)

if not files:
    print("No raw files for this run; nothing to load.")
    dbutils.jobs.taskValues.set(key="bronze_rows", value=0)
    dbutils.notebook.exit("no files")

# COMMAND ----------

parts = []
if json_files:
    parts.append(bronze.parse_json_files(spark, json_files))
if xml_files:
    parts.append(bronze.parse_xml_files(spark, xml_files))
entries = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])

bronze_df = bronze.build_bronze(entries, call_log, resource).cache()

# Every row must trace back to a logged API call; an unmatched file path means lineage is broken.
unmatched = bronze_df.filter(F.col("run_id").isNull()).select("source_file").distinct().limit(5).collect()
if unmatched:
    raise RuntimeError(f"Bronze rows without a matching api_call_log entry, e.g. {[r[0] for r in unmatched]}")

bronze_rows = bronze_df.count()
bronze.write_bronze(bronze_df, table, run_id)

# COMMAND ----------

# Reconciliation: entries reported by the API vs rows landed in Bronze (non-matching types such as
# OperationOutcome are dropped, so Bronze can be lower; the DQ notebook records this formally).
api_entries = call_log.agg(F.sum("entries_in_page")).first()[0] or 0
null_ids = bronze_df.filter(F.col("resource_id").isNull()).count()
print(f"api_entries={api_entries} bronze_rows={bronze_rows} null_resource_ids={null_ids}")
if bronze_rows != api_entries:
    print(f"WARNING: {api_entries - bronze_rows} entries were not loaded as {resource}")

dbutils.jobs.taskValues.set(key="bronze_rows", value=bronze_rows)

display(spark.sql(f"""
    SELECT load_date, source_format, count(*) AS rows, count(DISTINCT resource_id) AS ids
    FROM {table} WHERE run_id = '{run_id}' GROUP BY ALL ORDER BY ALL
"""))
