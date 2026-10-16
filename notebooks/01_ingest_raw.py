# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest Raw
# MAGIC Pulls one FHIR resource from the HAPI server for a `_lastUpdated` window, follows Bundle
# MAGIC pagination, writes every page to the raw Volume as received and logs each call to
# MAGIC `ops.api_call_log`.
# MAGIC
# MAGIC **Window:** `start_date` / `end_date` (`YYYY-MM-DD` or ISO datetime, end exclusive).
# MAGIC If `start_date` is blank, runs incrementally from `ops.watermark` minus the configured
# MAGIC overlap up to now. If `end_date` is blank and `start_date` is given, the window is one day.
# MAGIC
# MAGIC **Outputs (task values):** `raw_files`, `pages`, `entries`, `max_last_updated`, `load_date`.

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/api_client

# COMMAND ----------

# MAGIC %run ./lib/audit

# COMMAND ----------

# MAGIC %run ./lib/raw_writer

# COMMAND ----------

from datetime import datetime, timedelta, timezone

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
dbutils.widgets.text("resource", "Patient")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("format", "")
dbutils.widgets.text("run_id", "")

cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]
api_cfg = cfg["api"]
resource = get_resource(cfg, dbutils.widgets.get("resource").strip())["name"]
fmt = resolve_format(cfg, dbutils.widgets.get("format").strip() or None)
run_id = dbutils.widgets.get("run_id").strip() or f"manual_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"


def parse_ts(value):
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


start_raw, end_raw = dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
if start_raw.strip():
    window_start = parse_ts(start_raw)
    window_end = parse_ts(end_raw) if end_raw.strip() else window_start + timedelta(days=1)
    load_date = window_start.date().isoformat()   # backfill: the day being loaded
else:
    watermark = read_watermark(spark, catalog, resource)
    if watermark is None:
        raise ValueError(f"No watermark for {resource}; pass start_date for the initial load.")
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    window_start = watermark - timedelta(minutes=api_cfg.get("watermark_overlap_minutes", 0))
    window_end = parse_ts(end_raw) if end_raw.strip() else datetime.now(timezone.utc)
    load_date = datetime.now(timezone.utc).date().isoformat()   # incremental: the day it ran

if window_start >= window_end:
    raise ValueError(f"Empty window: {window_start} >= {window_end}")

# Raw files are partitioned by load_date: the day loaded for a backfill (so each backfill day gets
# its own folder), the run date for an incremental load (its window spans the previous run to now).
print(f"run_id={run_id} resource={resource} format={fmt} window=[{window_start}, {window_end}) load_date={load_date}")

# COMMAND ----------

params = build_search_params(window_start, window_end, api_cfg["page_size"], fmt)
session = build_session(api_cfg)

log_rows, raw_files = [], []
total_entries, max_last_updated, last_page = 0, None, None

try:
    for page in fetch_pages(session, api_cfg["base_url"], resource, params, fmt,
                            api_cfg["max_pages_per_window"], api_cfg["timeout_seconds"]):
        path = raw_file_path(cfg["storage"]["raw_volume_path"], resource, fmt,
                             load_date, run_id, page.page_number)
        write_raw(path, page.text)
        log_rows.append(call_log_row(run_id, resource, params if page.page_number == 1 else None,
                                     page=page, raw_file_path=path,
                                     file_saved_at=datetime.now(timezone.utc)))
        raw_files.append(path)
        total_entries += page.entries
        if page.max_last_updated and (max_last_updated is None or page.max_last_updated > max_last_updated):
            max_last_updated = page.max_last_updated
        last_page = page

    if last_page and last_page.next_url:
        # Hit max_pages_per_window with more data available: flag it rather than failing.
        log_rows[-1]["status"] = "TRUNCATED"
        print(f"WARNING: stopped at max_pages_per_window={api_cfg['max_pages_per_window']}; more pages exist.")
except FhirFetchError as e:
    log_rows.append(call_log_row(run_id, resource, params if e.page_number == 1 else None,
                                 status="FAILED", error=e))
    raise
finally:
    write_call_log(spark, catalog, log_rows)

print(f"pages={len(raw_files)} entries={total_entries} max_last_updated={max_last_updated}")

# COMMAND ----------

# The watermark itself is only advanced at the end of a fully successful pipeline run;
# here we just hand the values downstream.
dbutils.jobs.taskValues.set(key="raw_files", value=raw_files)
dbutils.jobs.taskValues.set(key="pages", value=len(raw_files))
dbutils.jobs.taskValues.set(key="entries", value=total_entries)
dbutils.jobs.taskValues.set(key="max_last_updated", value=max_last_updated.isoformat() if max_last_updated else None)
dbutils.jobs.taskValues.set(key="load_date", value=load_date)
