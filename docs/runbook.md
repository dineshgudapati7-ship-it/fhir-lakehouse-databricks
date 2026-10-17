# Runbook

## Deploy

```bash
databricks bundle validate
databricks bundle deploy          # target dev (default environment)
```

## Initial backfill

Run one window per day, **oldest first**. SCD2 history is built in load order, and the job allows only
one run at a time.

```bash
databricks bundle run fhir_pipeline --params start_date=2026-10-10
databricks bundle run fhir_pipeline --params start_date=2026-10-11
databricks bundle run fhir_pipeline --params start_date=2026-10-12
```

Each successful run advances `ops.watermark` to the newest `meta.lastUpdated` it loaded. Watermarks
never move backwards.

## Daily incremental load

Run without `start_date`. The window is `[watermark − 10 min, now)`.

```bash
databricks bundle run fhir_pipeline
```

The job has a daily 06:00 UTC schedule that ships **paused**. Unpause it in the Jobs UI (or set
`pause_status: UNPAUSED` in `resources/fhir_pipeline.job.yml` and redeploy) once the backfill is done.

## Checking a run

```sql
SELECT * FROM fhir.ops.pipeline_run ORDER BY started_at DESC;
SELECT * FROM fhir.gold.vw_pipeline_health ORDER BY started_at DESC;
SELECT * FROM fhir.ops.dq_results WHERE run_id = '<run_id>' ORDER BY status, table_name;
SELECT * FROM fhir.ops.api_call_log WHERE status <> 'SUCCESS' ORDER BY call_started_at DESC;
```

## When a run fails

`mark_failed` sets the run to FAILED in `ops.pipeline_run`, and watermarks stay where they were. The
next incremental run re-reads the same window, and Silver absorbs the overlap.

| Failing task | Look at | Typical cause / action |
|---|---|---|
| `ingest_*` | `ops.api_call_log` rows with `status = 'FAILED'` | HAPI down or rate-limiting after retries. Repair the run, or rerun later. |
| `bronze_*` | task output | "rows could not be matched to ops.api_call_log": a raw file path mismatch. Check `_metadata.file_path` vs `raw_file_path`. |
| `silver_*` | task output | invalid mapping (the loader names the column) or a SCD2 duplicate-current check |
| `data_quality` | `ops.dq_results` with `status = 'FAIL'` | a correctness check failed. Fix the cause, then repair the run from `data_quality`. |

A repaired run reuses the same `run_id`. Raw files are overwritten in place, Bronze replaces that
run's rows, and the Silver merge is idempotent for already-applied data.

`TRUNCATED` in `api_call_log` means a window hit `max_pages_per_window`. Raise the cap in
`conf/pipeline_config.yaml`, or load that day in smaller windows (`start_date` / `end_date` with times).

## Changing the Silver mapping

1. Edit `conf/silver_mappings.yaml`. Quote every `expr`, because inside `{...}` an unquoted comma ends the value.
2. `uv run pytest tests/test_silver.py` (validates every mapping), then deploy.
3. Run `notebooks/03_bronze_to_silver` with `rebuild = true` for each changed resource. Rebuild drops the
   Silver table and replays every Bronze run in load-date order. Without it, existing rows keep the old
   values: the same `lastUpdated` with a different hash is treated as a late arrival and skipped.
4. Run `notebooks/04_gold` (or the whole job).

## Tests

```bash
uv run pytest                      # local: API client, pagination, mappings, Gold and DQ definitions
```

In the workspace, run `notebooks/tests/test_scd2`. It applies synthetic batches (insert; change +
no-change + new; late arrival) to a scratch table, asserts versions and counts, then drops the table.
