# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold
# MAGIC Rebuilds the Gold star schema and reporting views from the current Silver rows
# MAGIC (definitions in `lib/gold`). Full rebuild every run: idempotent, and cheap at this volume.
# MAGIC
# MAGIC | Object | Grain |
# MAGIC |---|---|
# MAGIC | `dim_patient` | one row per current patient, with age and age band |
# MAGIC | `fact_encounter` / `fact_observation` / `fact_condition` | one row per current resource, joined to `dim_patient` on `patient_id` |
# MAGIC | `vw_daily_clinical_activity` | one row per day: encounters, observations, conditions |
# MAGIC | `vw_top_conditions` | one row per condition code |
# MAGIC | `vw_pipeline_health` | one row per run and resource, from the ops tables |

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %run ./lib/gold

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir")
cfg = load_config(catalog=dbutils.widgets.get("catalog").strip() or None)
catalog = cfg["storage"]["catalog"]

# COMMAND ----------

for name, kind, select_sql in gold_objects(f"{catalog}.silver.", f"{catalog}.gold.", f"{catalog}.ops."):
    target = f"{catalog}.gold.{name}"
    spark.sql(f"CREATE OR REPLACE {'TABLE' if kind == 'table' else 'VIEW'} {target} AS {select_sql}")
    print(f"{kind:5s} {target}")

# COMMAND ----------

# Facts whose patient was never loaded (the reference points outside the windows we pulled).
display(spark.sql(f"""
    SELECT fact, count(*) AS rows, count_if(p.patient_id IS NULL) AS without_patient
    FROM (
        SELECT 'encounter' AS fact, patient_id FROM {catalog}.gold.fact_encounter
        UNION ALL SELECT 'observation', patient_id FROM {catalog}.gold.fact_observation
        UNION ALL SELECT 'condition', patient_id FROM {catalog}.gold.fact_condition
    ) f
    LEFT JOIN {catalog}.gold.dim_patient p USING (patient_id)
    GROUP BY fact ORDER BY fact
"""))
