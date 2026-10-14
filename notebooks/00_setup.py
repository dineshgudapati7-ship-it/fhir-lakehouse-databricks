# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Idempotent: creates the catalog schemas, the raw landing volume and the ops (audit) tables.
# MAGIC Safe to rerun; also runs as the first task of the pipeline job, where it records the run as
# MAGIC RUNNING in `ops.pipeline_run` (closed by the `finalize` / `mark_failed` tasks).

# COMMAND ----------

# MAGIC %run ./lib/audit

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("format", "")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

for schema in ["raw", "bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing COMMENT 'FHIR API responses stored as received'")

# COMMAND ----------

ops_tables = {
    "api_call_log": """
        run_id STRING,
        resource STRING,
        page_number INT,
        request_url STRING,
        request_params STRING,
        http_status INT,
        call_started_at TIMESTAMP,
        call_ended_at TIMESTAMP,
        entries_in_page INT,
        next_link STRING,
        raw_file_path STRING,
        file_saved_at TIMESTAMP,
        status STRING,
        error_message STRING
    """,
    "watermark": """
        resource STRING,
        last_max_lastupdated TIMESTAMP,
        last_successful_run_id STRING,
        updated_at TIMESTAMP
    """,
    "load_change_summary": """
        run_id STRING,
        load_date DATE,
        resource STRING,
        records_in_batch BIGINT,
        new_records BIGINT,
        changed_records BIGINT,
        unchanged_records BIGINT,
        closed_versions BIGINT
    """,
    "pipeline_run": """
        run_id STRING,
        job_run_id STRING,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        status STRING,
        params STRING
    """,
    "dq_results": """
        run_id STRING,
        check_name STRING,
        table_name STRING,
        status STRING,
        metric_value DOUBLE,
        threshold DOUBLE,
        details STRING,
        checked_at TIMESTAMP
    """,
}

for name, cols in ops_tables.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.ops.{name} ({cols}) USING DELTA")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.ops"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.raw"))

# COMMAND ----------

run_id = dbutils.widgets.get("run_id").strip()
if run_id:   # only inside a job run; interactive setup runs aren't pipeline runs
    params = {k: dbutils.widgets.get(k) for k in ("catalog", "start_date", "end_date", "format")}
    start_pipeline_run(spark, catalog, run_id, dbutils.widgets.get("job_run_id").strip() or run_id, params)
    print(f"pipeline run {run_id} started with {params}")
