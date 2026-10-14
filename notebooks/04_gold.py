# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold
# MAGIC Rebuilds the Gold star schema and reporting views from the current Silver state
# MAGIC (definitions in `lib/gold`). Full rebuild every run: idempotent, and cheap at this volume.
# MAGIC
# MAGIC | Object | Grain |
# MAGIC |---|---|
# MAGIC | `dim_patient` | one row per patient version (SCD2), plus Unknown (`patient_sk = -1`) |
# MAGIC | `dim_code` | one row per code system + code, most-used display |
# MAGIC | `dim_date` | one row per calendar day covering all event dates |
# MAGIC | `fact_encounter` / `fact_observation` / `fact_condition` | one row per current resource |
# MAGIC | `vw_patient_360`, `vw_daily_clinical_activity`, `vw_top_conditions`, `vw_patient_vitals`, `vw_pipeline_health` | reporting views |

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/gold

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]

# COMMAND ----------

for name, kind, cluster_by, select_sql in gold_objects(f"{catalog}.silver.", f"{catalog}.gold.", f"{catalog}.ops."):
    target = f"{catalog}.gold.{name}"
    if kind == "table":
        cluster = f"CLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
        spark.sql(f"CREATE OR REPLACE TABLE {target} {cluster} AS {select_sql}")
    else:
        spark.sql(f"CREATE OR REPLACE VIEW {target} AS {select_sql}")
    print(f"{kind:5s} {target}")

# COMMAND ----------

# How facts were linked to patients: point_in_time (SCD2 version valid at event time),
# current (event predates the versions we hold) or unknown (patient never loaded).
display(spark.sql(f"""
    SELECT 'encounter' AS fact, patient_match, count(*) AS rows FROM {catalog}.gold.fact_encounter GROUP BY ALL
    UNION ALL SELECT 'observation', patient_match, count(*) FROM {catalog}.gold.fact_observation GROUP BY ALL
    UNION ALL SELECT 'condition', patient_match, count(*) FROM {catalog}.gold.fact_condition GROUP BY ALL
    ORDER BY 1, 2
"""))
