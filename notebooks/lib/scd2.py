# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/scd2
# MAGIC Reusable SCD Type 2 merge for any Silver table keyed by a single business key.
# MAGIC
# MAGIC Expects incoming rows with `record_hash` and `effective_from` (one row per key). Changed keys get
# MAGIC their current version closed (`effective_to`, `is_current = false`) and a new version inserted;
# MAGIC unchanged and late-arriving (older than current) records are ignored.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/scd2`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def prepare_scd2(df):
    """Add SCD2 columns to a deduplicated batch. effective_from is source business time when known."""
    return (df.withColumn("effective_from", F.coalesce("source_last_updated", "_extracted_at"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("_silver_loaded_at", F.current_timestamp()))


def ensure_scd2_table(spark, df, table):
    """Create the table from the batch schema, or add columns newly introduced in the mapping."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)
        return
    existing = {f.name.lower() for f in spark.table(table).schema}
    new_cols = [f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema if f.name.lower() not in existing]
    if new_cols:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(new_cols)})")


def scd2_merge(spark, updates, table, key):
    """Apply one batch; returns counts for ops.load_change_summary."""
    cols = updates.columns
    current = (spark.table(table).filter("is_current")
               .select(F.col(key).alias("_t_key"), F.col("record_hash").alias("_t_hash"),
                       F.col("effective_from").alias("_t_from")))
    joined = updates.join(current, updates[key] == current["_t_key"], "left")

    new = joined.filter("_t_key IS NULL").select(cols)
    changed = joined.filter("_t_key IS NOT NULL AND record_hash <> _t_hash AND effective_from > _t_from").select(cols)
    late = joined.filter("_t_key IS NOT NULL AND record_hash <> _t_hash AND effective_from <= _t_from")

    # Counts must be taken before the merge changes which rows are current.
    counts = {"records_in_batch": updates.count(), "new_records": new.count(),
              "changed_records": changed.count(), "late_records": late.count()}
    counts["unchanged_records"] = counts["records_in_batch"] - counts["new_records"] - counts["changed_records"]
    counts["closed_versions"] = counts["changed_records"]

    # Changed keys appear twice: with merge key = key (matches and closes the current version) and with
    # a NULL merge key (never matches, so the new version is inserted). New keys are inserted directly.
    staged = (new.withColumn("_merge_key", F.col(key))
              .unionByName(changed.withColumn("_merge_key", F.col(key)))
              .unionByName(changed.withColumn("_merge_key", F.lit(None).cast(updates.schema[key].dataType))))

    (DeltaTable.forName(spark, table).alias("t")
     .merge(staged.alias("s"), f"t.`{key}` = s._merge_key AND t.is_current = true")
     .whenMatchedUpdate(set={"is_current": "false", "effective_to": "s.effective_from"})
     .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in cols})
     .execute())
    return counts
