# Databricks notebook source
# MAGIC %md
# MAGIC # Test - SCD2 merge
# MAGIC Runs `lib/scd2` on a scratch table with synthetic batches (insert, change + no-change + new,
# MAGIC late-arriving older version) and asserts versions and counts. Drops the table afterwards.

# COMMAND ----------

# MAGIC %run ../lib/silver

# COMMAND ----------

# MAGIC %run ../lib/scd2

# COMMAND ----------

from datetime import date, datetime

dbutils.widgets.text("catalog", "fhir")
table = f"{dbutils.widgets.get('catalog')}.silver._test_scd2"
spark.sql(f"DROP TABLE IF EXISTS {table}")

SCHEMA = ("id STRING, val STRING, source_last_updated TIMESTAMP, source_version_id STRING, "
          "_run_id STRING, _extracted_at TIMESTAMP, _load_date DATE")


def batch(run_id, rows):
    data = [(k, v, ts, ver, run_id, ts, ts.date()) for k, v, ts, ver in rows]
    return prepare_scd2(add_record_hash(dedupe_latest(spark.createDataFrame(data, SCHEMA), "id"), ["val"]))


def apply(run_id, rows):
    updates = batch(run_id, rows)
    ensure_scd2_table(spark, updates, table)
    return scd2_merge(spark, updates, table, "id")


t1, t2, t15 = datetime(2026, 10, 10, 8), datetime(2026, 10, 11, 8), datetime(2026, 10, 10, 20)

# COMMAND ----------

c1 = apply("r1", [("k1", "a", t1, "1"), ("k2", "b", t1, "1")])
assert (c1["new_records"], c1["changed_records"], c1["unchanged_records"]) == (2, 0, 0), c1

# k1 changes, k2 re-fetched unchanged (newer timestamp, same data), k3 new; duplicate k3 in batch -> latest wins
c2 = apply("r2", [("k1", "a2", t2, "2"), ("k2", "b", t2, "2"), ("k3", "c-old", t1, "1"), ("k3", "c", t2, "2")])
assert (c2["new_records"], c2["changed_records"], c2["unchanged_records"]) == (1, 1, 1), c2

# late-arriving older version of k1 must not overwrite the newer current one
c3 = apply("r3", [("k1", "a-late", t15, "9")])
assert (c3["new_records"], c3["changed_records"], c3["late_records"]) == (0, 0, 1), c3

# COMMAND ----------

rows = {(r.id, r.is_current): r for r in spark.table(table).collect()}
assert spark.table(table).count() == 4
assert rows[("k1", True)].val == "a2" and rows[("k1", True)].effective_from == t2
assert rows[("k1", False)].val == "a" and rows[("k1", False)].effective_to == t2
assert rows[("k2", True)].effective_from == t1          # unchanged: original version kept
assert rows[("k3", True)].val == "c"
assert spark.sql(f"SELECT id FROM {table} WHERE is_current GROUP BY id HAVING count(*) > 1").count() == 0

spark.sql(f"DROP TABLE IF EXISTS {table}")
print("SCD2 tests passed")
