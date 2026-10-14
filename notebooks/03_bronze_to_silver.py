# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze to Silver
# MAGIC For one `run_id` + `resource`: flatten Bronze using `conf/silver_mappings.yaml`, clean types,
# MAGIC quarantine rows without a business key, keep the latest version per key, then SCD Type 2
# MAGIC merge into `silver.<resource>` and record new/changed/unchanged counts in
# MAGIC `ops.load_change_summary`.
# MAGIC
# MAGIC Run each run_id in load order: SCD2 history depends on the order batches arrive.
# MAGIC
# MAGIC `rebuild = true` drops the Silver table and replays every Bronze run in load-date order.
# MAGIC Use it after changing `silver_mappings.yaml`, or if runs were loaded out of order.

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/audit

# COMMAND ----------

# MAGIC %run ./lib/bronze

# COMMAND ----------

# MAGIC %run ./lib/silver

# COMMAND ----------

# MAGIC %run ./lib/scd2

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("resource", "Patient")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("rebuild", "false", ["false", "true"])

cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
resource = get_resource(cfg, dbutils.widgets.get("resource").strip())["name"]
rebuild = dbutils.widgets.get("rebuild") == "true"

mapping = load_silver_mapping(resource)
key = mapping["key"]
business_columns = [c for c in mapping["columns"] if c != key]
target = silver_table(catalog, resource)
quarantine = f"{target}_quarantine"
source = bronze_table(catalog, resource)

# COMMAND ----------


def process_run(run_id):
    """Flatten, quarantine, dedupe and SCD2-merge one Bronze run. Returns the change counts."""
    batch = spark.table(source).filter(F.col("_run_id") == run_id)
    if batch.limit(1).count() == 0:
        print(f"[{run_id}] no bronze rows")
        return None

    flat, missing = flatten_resource(batch, mapping)
    if missing:
        print(f"[{run_id}] fields not present in this batch (loaded as NULL): {missing}")

    good, bad = split_quarantine(flat, key)
    if bad.limit(1).count():
        write_quarantine(spark, bad, quarantine, run_id)

    updates = prepare_scd2(add_record_hash(dedupe_latest(good, key), business_columns))
    ensure_scd2_table(spark, updates, target)
    counts = scd2_merge(spark, updates, target, key)

    load_date = batch.agg(F.max("_load_date")).first()[0]
    write_change_summary(spark, catalog, run_id, load_date, resource, counts)
    print(f"[{run_id}] load_date={load_date} {counts}")
    return counts


# COMMAND ----------

if rebuild:
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(f"DROP TABLE IF EXISTS {quarantine}")
    spark.sql(f"DELETE FROM {catalog}.ops.load_change_summary WHERE resource = '{resource}'")
    run_ids = [r._run_id for r in spark.table(source)
               .groupBy("_run_id").agg(F.min("_load_date").alias("d"), F.min("_extracted_at").alias("t"))
               .orderBy("d", "t").collect()]
    print(f"Rebuilding {target} from {len(run_ids)} bronze runs")
else:
    run_ids = [validate_run_id(dbutils.widgets.get("run_id").strip())]

counts = None
for rid in run_ids:
    counts = process_run(rid)

if counts:
    dbutils.jobs.taskValues.set(key="silver_new", value=counts["new_records"])
    dbutils.jobs.taskValues.set(key="silver_changed", value=counts["changed_records"])

# COMMAND ----------

if not spark.catalog.tableExists(target):
    dbutils.notebook.exit("no silver table yet")

# Must always be 0: more than one current version per key means SCD2 is broken.
dupes = spark.sql(f"SELECT `{key}` FROM {target} WHERE is_current GROUP BY 1 HAVING count(*) > 1").count()
if dupes:
    raise RuntimeError(f"{dupes} keys have more than one current version in {target}")

display(spark.sql(f"""
    SELECT is_current, count(*) AS rows, count(DISTINCT `{key}`) AS keys FROM {target} GROUP BY 1
"""))
