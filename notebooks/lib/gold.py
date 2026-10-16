# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/gold
# MAGIC Gold star schema and reporting views, defined as ordered SELECT statements over the current
# MAGIC Silver rows. Facts join to `dim_patient` on `patient_id`.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/gold`.

# COMMAND ----------


def _code_display(s, table):
    """Most-used display per code: FHIR sources spell the same code's display differently."""
    return f"""
        SELECT code, max_by(code_display, n) AS code_display FROM (
            SELECT code, code_display, count(*) AS n FROM {s}{table}
            WHERE is_current AND code IS NOT NULL AND code_display IS NOT NULL GROUP BY ALL
        ) GROUP BY code"""


def gold_objects(s, g, o):
    """Ordered (name, kind, select_sql). s/g/o are table-name prefixes for silver/gold/ops, e.g.
    'fhir.silver.'. Each object only references objects defined before it."""
    return [
        ("dim_patient", "table", f"""
            SELECT *,
                   CASE WHEN age IS NULL THEN 'Unknown'
                        WHEN age < 18 THEN '0-17' WHEN age < 35 THEN '18-34'
                        WHEN age < 50 THEN '35-49' WHEN age < 65 THEN '50-64'
                        ELSE '65+' END AS age_band,
                   -- sort key for age_band in BI tools (Unknown last)
                   CASE WHEN age IS NULL THEN 6
                        WHEN age < 18 THEN 1 WHEN age < 35 THEN 2
                        WHEN age < 50 THEN 3 WHEN age < 65 THEN 4
                        ELSE 5 END AS age_band_order
            FROM (
                SELECT patient_id, given_name, family_name,
                       coalesce(gender, 'unknown') AS gender,
                       birth_date,
                       CAST(floor(months_between(coalesce(CAST(deceased_datetime AS DATE), current_date()),
                                                 birth_date) / 12) AS INT) AS age,
                       city, state, country, active, deceased_flag
                FROM {s}patient WHERE is_current
            )"""),

        ("fact_encounter", "table", f"""
            SELECT encounter_id, patient_id,
                   CAST(period_start AS DATE) AS encounter_date,
                   period_start, period_end,
                   CASE WHEN period_end >= period_start
                        THEN round((unix_timestamp(period_end) - unix_timestamp(period_start)) / 3600, 2)
                   END AS duration_hours,
                   status, class_code, type_display
            FROM {s}encounter WHERE is_current"""),

        ("fact_observation", "table", f"""
            SELECT o.observation_id, o.patient_id, o.encounter_id,
                   CAST(o.effective_datetime AS DATE) AS observation_date,
                   o.effective_datetime, o.status, o.category,
                   o.code, coalesce(d.code_display, o.code) AS code_display,
                   o.value_quantity, o.value_unit, o.value_code,
                   o.bp_systolic, o.bp_diastolic
            FROM {s}observation o
            LEFT JOIN ({_code_display(s, 'observation')}) d ON d.code = o.code
            WHERE o.is_current"""),

        ("fact_condition", "table", f"""
            SELECT c.condition_id, c.patient_id, c.encounter_id,
                   CAST(c.onset_datetime AS DATE) AS onset_date,
                   c.recorded_date,
                   c.code, coalesce(d.code_display, c.code) AS code_display,
                   c.clinical_status, c.verification_status, c.category
            FROM {s}condition c
            LEFT JOIN ({_code_display(s, 'condition')}) d ON d.code = c.code
            WHERE c.is_current"""),

        ("vw_daily_clinical_activity", "view", f"""
            SELECT activity_date,
                   count_if(kind = 'encounter') AS encounters,
                   count_if(kind = 'observation') AS observations,
                   count_if(kind = 'condition') AS conditions
            FROM (
                SELECT encounter_date AS activity_date, 'encounter' AS kind FROM {g}fact_encounter
                UNION ALL SELECT observation_date, 'observation' FROM {g}fact_observation
                UNION ALL SELECT coalesce(recorded_date, onset_date), 'condition' FROM {g}fact_condition
            )
            WHERE activity_date IS NOT NULL
            GROUP BY activity_date"""),

        ("vw_top_conditions", "view", f"""
            SELECT code, code_display,
                   count(*) AS occurrences,
                   count(DISTINCT patient_id) AS patients,
                   count_if(clinical_status = 'active') AS active_occurrences
            FROM {g}fact_condition
            WHERE code IS NOT NULL
            GROUP BY code, code_display"""),

        ("vw_pipeline_health", "view", f"""
            SELECT l.run_id, l.resource, s.load_date,
                   min(l.call_started_at) AS started_at, max(l.file_saved_at) AS finished_at,
                   count(*) AS api_calls,
                   count_if(l.status = 'FAILED') AS failed_calls,
                   count_if(l.status = 'TRUNCATED') AS truncated_windows,
                   sum(l.entries_in_page) AS records_fetched,
                   max(s.new_records) AS new_records,
                   max(s.changed_records) AS changed_records,
                   max(s.unchanged_records) AS unchanged_records
            FROM {o}api_call_log l
            LEFT JOIN {o}load_change_summary s ON s.run_id = l.run_id AND s.resource = l.resource
            GROUP BY l.run_id, l.resource, s.load_date"""),
    ]
