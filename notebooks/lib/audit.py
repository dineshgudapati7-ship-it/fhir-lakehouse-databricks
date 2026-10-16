# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/audit
# MAGIC Ops/audit table helpers: API call log, pipeline runs, watermarks and change summaries.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/audit`.

# COMMAND ----------

import json

API_CALL_LOG_SCHEMA = """
    run_id STRING, resource STRING, page_number INT, request_url STRING, request_params STRING,
    http_status INT, call_started_at TIMESTAMP, call_ended_at TIMESTAMP, entries_in_page INT,
    next_link STRING, raw_file_path STRING, file_saved_at TIMESTAMP, status STRING,
    error_message STRING
"""


def call_log_row(run_id, resource, params, page=None, raw_file_path=None, file_saved_at=None,
                 status="SUCCESS", error=None):
    """Build one api_call_log row from a Page (success) or a FhirFetchError (failure)."""
    src = page or error
    return {
        "run_id": run_id,
        "resource": resource,
        "page_number": src.page_number,
        "request_url": src.request_url,
        "request_params": json.dumps(params) if params else None,
        "http_status": src.http_status,
        "call_started_at": src.call_started_at,
        "call_ended_at": page.call_ended_at if page else None,
        "entries_in_page": page.entries if page else None,
        "next_link": page.next_url if page else None,
        "raw_file_path": raw_file_path,
        "file_saved_at": file_saved_at,
        "status": status,
        "error_message": str(error.cause)[:4000] if error else None,
    }


def write_call_log(spark, catalog, rows):
    if rows:
        (spark.createDataFrame(rows, API_CALL_LOG_SCHEMA)
         .write.mode("append").saveAsTable(f"{catalog}.ops.api_call_log"))


def read_watermark(spark, catalog, resource):
    """Return the last successfully loaded max meta.lastUpdated for a resource, or None."""
    row = (spark.table(f"{catalog}.ops.watermark")
           .filter(f"resource = '{resource}'")
           .select("last_max_lastupdated")
           .first())
    return row[0] if row else None


LOAD_CHANGE_SUMMARY_SCHEMA = """
    run_id STRING, load_date DATE, resource STRING, records_in_batch BIGINT, new_records BIGINT,
    changed_records BIGINT, unchanged_records BIGINT, closed_versions BIGINT
"""


def write_change_summary(spark, catalog, run_id, load_date, resource, counts):
    """One row per run and resource; rerunning a run_id replaces its row."""
    table = f"{catalog}.ops.load_change_summary"
    spark.sql(f"DELETE FROM {table} WHERE run_id = '{run_id}' AND resource = '{resource}'")
    row = {"run_id": run_id, "load_date": load_date, "resource": resource,
           **{k: counts[k] for k in ("records_in_batch", "new_records", "changed_records",
                                     "unchanged_records", "closed_versions")}}
    spark.createDataFrame([row], LOAD_CHANGE_SUMMARY_SCHEMA).write.mode("append").saveAsTable(table)


def start_pipeline_run(spark, catalog, run_id, job_run_id, params):
    """Record the run as RUNNING (a repaired/re-run job run_id is reset to RUNNING)."""
    spark.sql(f"""
        MERGE INTO {catalog}.ops.pipeline_run t
        USING (SELECT :run_id AS run_id, :job_run_id AS job_run_id, :params AS params) s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET job_run_id = s.job_run_id, started_at = current_timestamp(),
                                     ended_at = NULL, status = 'RUNNING', params = s.params
        WHEN NOT MATCHED THEN INSERT (run_id, job_run_id, started_at, ended_at, status, params)
                              VALUES (s.run_id, s.job_run_id, current_timestamp(), NULL, 'RUNNING', s.params)
    """, args={"run_id": run_id, "job_run_id": job_run_id, "params": json.dumps(params)})


def finish_pipeline_run(spark, catalog, run_id, status):
    spark.sql(f"""
        UPDATE {catalog}.ops.pipeline_run SET ended_at = current_timestamp(), status = :status
        WHERE run_id = :run_id
    """, args={"run_id": run_id, "status": status})


def update_watermark(spark, catalog, resource, max_last_updated, run_id):
    """Advance the watermark to max_last_updated; never moves it backwards (e.g. an older backfill)."""
    if max_last_updated is None:
        return
    spark.sql(f"""
        MERGE INTO {catalog}.ops.watermark t
        USING (SELECT :resource AS resource, CAST(:ts AS TIMESTAMP) AS ts, :run_id AS run_id) s
        ON t.resource = s.resource
        WHEN MATCHED AND s.ts > t.last_max_lastupdated THEN
            UPDATE SET last_max_lastupdated = s.ts, last_successful_run_id = s.run_id,
                       updated_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (resource, last_max_lastupdated, last_successful_run_id, updated_at)
            VALUES (s.resource, s.ts, s.run_id, current_timestamp())
    """, args={"resource": resource, "ts": str(max_last_updated), "run_id": run_id})
