"""Ops/audit table helpers: API call log and watermarks."""
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
