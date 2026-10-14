# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/gold
# MAGIC Gold star schema and reporting views, defined as ordered SELECT statements.
# MAGIC
# MAGIC Facts are linked to the patient version valid at event time (SCD2 point-in-time join). If the
# MAGIC event happened before the first version we hold, they fall back to the current version, and
# MAGIC to the Unknown patient (`patient_sk = -1`) when the patient was never loaded.
# MAGIC `patient_match` records which of the three applied.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/gold`.

# COMMAND ----------

LOINC_VITALS = {
    "8867-4": "heart_rate",
    "29463-7": "body_weight",
    "8302-2": "body_height",
    "39156-5": "bmi",
    "4548-4": "hba1c",
    "2339-0": "glucose",
    "41653-7": "glucose",
    "8480-6": "bp_systolic",
    "8462-4": "bp_diastolic",
}


def _code_sk(prefix=""):
    return (f"CASE WHEN {prefix}code IS NULL THEN -1 "
            f"ELSE xxhash64(coalesce({prefix}code_system, ''), {prefix}code) END")


def _date_key(col):
    return f"CAST(date_format({col}, 'yyyyMMdd') AS INT)"


def _patient_joins(g):
    """Point-in-time patient version, else current version, else Unknown (-1). Expects alias f + f.event_ts."""
    select = """
        coalesce(pit.patient_sk, cur.patient_sk, -1) AS patient_sk,
        CASE WHEN pit.patient_sk IS NOT NULL THEN 'point_in_time'
             WHEN cur.patient_sk IS NOT NULL THEN 'current'
             ELSE 'unknown' END AS patient_match"""
    joins = f"""
    LEFT JOIN {g}dim_patient pit
      ON pit.patient_id = f.patient_id
     AND f.event_ts >= pit.effective_from
     AND (f.event_ts < pit.effective_to OR pit.effective_to IS NULL)
    LEFT JOIN {g}dim_patient cur
      ON cur.patient_id = f.patient_id AND cur.is_current"""
    return select, joins


def gold_objects(s, g, o):
    """Ordered (name, kind, cluster_by, select_sql). s/g/o are table-name prefixes for silver/gold/ops,
    e.g. 'fhir.silver.' — each object only references objects defined before it."""
    patient_select, patient_joins = _patient_joins(g)
    vital_case = "CASE code " + " ".join(f"WHEN '{c}' THEN '{v}'" for c, v in LOINC_VITALS.items()) + " END"
    vitals = sorted(set(LOINC_VITALS.values()))

    return [
        ("dim_patient", "table", None, f"""
            SELECT *,
                   CASE WHEN age IS NULL THEN 'Unknown'
                        WHEN age < 18 THEN '0-17' WHEN age < 35 THEN '18-34'
                        WHEN age < 50 THEN '35-49' WHEN age < 65 THEN '50-64'
                        ELSE '65+' END AS age_band
            FROM (
                SELECT xxhash64(patient_id, CAST(effective_from AS STRING)) AS patient_sk,
                       patient_id, given_name, family_name,
                       coalesce(gender, 'unknown') AS gender,
                       birth_date,
                       CAST(floor(months_between(coalesce(CAST(deceased_datetime AS DATE), current_date()),
                                                 birth_date) / 12) AS INT) AS age,
                       city, state, postal_code, country, marital_status,
                       active, deceased_flag, deceased_datetime,
                       effective_from, effective_to, is_current
                FROM {s}patient
                UNION ALL
                SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', 'unknown', CAST(NULL AS DATE), CAST(NULL AS INT),
                       NULL, NULL, NULL, NULL, NULL, CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN),
                       CAST(NULL AS TIMESTAMP), TIMESTAMP'1900-01-01', CAST(NULL AS TIMESTAMP), true
            )"""),

        # One row per code; FHIR sources spell the same code's display differently, so keep the most used.
        ("dim_code", "table", None, f"""
            WITH c AS (
                SELECT code_system, code, code_display AS display, 'Observation' AS source
                FROM {s}observation WHERE is_current AND code IS NOT NULL
                UNION ALL
                SELECT code_system, code, code_display, 'Condition'
                FROM {s}condition WHERE is_current AND code IS NOT NULL
            ), counted AS (
                SELECT code_system, code, display, source, count(*) AS n FROM c GROUP BY ALL
            )
            SELECT {_code_sk()} AS code_sk, code_system, code,
                   coalesce(max_by(display, CASE WHEN display IS NOT NULL THEN n END), code) AS display,
                   array_join(array_sort(collect_set(source)), ',') AS used_in,
                   sum(n) AS usage_count
            FROM counted GROUP BY code_system, code
            UNION ALL
            SELECT -1, NULL, NULL, 'Unknown', NULL, 0"""),

        ("fact_encounter", "table", ["patient_sk", "date_key"], f"""
            WITH f AS (SELECT *, period_start AS event_ts FROM {s}encounter WHERE is_current)
            SELECT f.encounter_id, {patient_select},
                   f.patient_id,
                   {_date_key('f.period_start')} AS date_key,
                   f.status, f.class_code, f.type_code, f.type_display,
                   f.period_start, f.period_end,
                   CASE WHEN f.period_end >= f.period_start
                        THEN round((unix_timestamp(f.period_end) - unix_timestamp(f.period_start)) / 3600, 2)
                   END AS duration_hours,
                   f.service_provider, f._load_date
            FROM f {patient_joins}"""),

        ("fact_observation", "table", ["patient_sk", "date_key"], f"""
            WITH f AS (SELECT *, effective_datetime AS event_ts FROM {s}observation WHERE is_current)
            SELECT f.observation_id, {patient_select},
                   f.patient_id, f.encounter_id,
                   {_code_sk('f.')} AS code_sk,
                   {_date_key('f.effective_datetime')} AS date_key,
                   f.effective_datetime, f.status, f.category, f.code,
                   f.value_quantity, f.value_unit, f.value_string, f.value_code,
                   f.bp_systolic, f.bp_diastolic, f._load_date
            FROM f {patient_joins}"""),

        ("fact_condition", "table", ["patient_sk", "onset_date_key"], f"""
            WITH f AS (SELECT *, coalesce(onset_datetime, CAST(recorded_date AS TIMESTAMP)) AS event_ts
                       FROM {s}condition WHERE is_current)
            SELECT f.condition_id, {patient_select},
                   f.patient_id, f.encounter_id,
                   {_code_sk('f.')} AS code_sk,
                   {_date_key('f.onset_datetime')} AS onset_date_key,
                   {_date_key('f.recorded_date')} AS recorded_date_key,
                   f.clinical_status, f.verification_status, f.category, f.code,
                   f.onset_datetime, f.recorded_date, f._load_date
            FROM f {patient_joins}"""),

        # Calendar covering every event date in the facts, up to today.
        ("dim_date", "table", None, f"""
            WITH bounds AS (
                SELECT least(min(d), current_date()) AS lo, greatest(max(d), current_date()) AS hi FROM (
                    SELECT CAST(period_start AS DATE) AS d FROM {g}fact_encounter
                    UNION ALL SELECT CAST(effective_datetime AS DATE) FROM {g}fact_observation
                    UNION ALL SELECT CAST(onset_datetime AS DATE) FROM {g}fact_condition
                    UNION ALL SELECT recorded_date FROM {g}fact_condition)
            )
            SELECT {_date_key('d')} AS date_key, d AS date,
                   year(d) AS year, quarter(d) AS quarter, month(d) AS month,
                   date_format(d, 'MMMM') AS month_name, day(d) AS day,
                   dayofweek(d) AS day_of_week, date_format(d, 'EEEE') AS day_name,
                   dayofweek(d) IN (1, 7) AS is_weekend
            FROM bounds LATERAL VIEW explode(sequence(lo, hi, INTERVAL 1 DAY)) days AS d"""),

        ("vw_patient_360", "view", None, f"""
            SELECT p.patient_id, p.given_name, p.family_name, p.gender, p.birth_date, p.age, p.age_band,
                   p.city, p.state, p.country,
                   coalesce(e.encounters, 0) AS encounters, e.last_encounter_at,
                   coalesce(o.observations, 0) AS observations, o.last_observation_at,
                   coalesce(c.conditions, 0) AS conditions, coalesce(c.active_conditions, 0) AS active_conditions
            FROM {g}dim_patient p
            LEFT JOIN (SELECT patient_id, count(*) AS encounters, max(period_start) AS last_encounter_at
                       FROM {g}fact_encounter GROUP BY 1) e ON e.patient_id = p.patient_id
            LEFT JOIN (SELECT patient_id, count(*) AS observations, max(effective_datetime) AS last_observation_at
                       FROM {g}fact_observation GROUP BY 1) o ON o.patient_id = p.patient_id
            LEFT JOIN (SELECT patient_id, count(*) AS conditions,
                              count_if(clinical_status = 'active') AS active_conditions
                       FROM {g}fact_condition GROUP BY 1) c ON c.patient_id = p.patient_id
            WHERE p.is_current AND p.patient_sk <> -1"""),

        ("vw_daily_clinical_activity", "view", None, f"""
            SELECT d.date, d.day_name, d.is_weekend,
                   coalesce(e.n, 0) AS encounters,
                   coalesce(o.n, 0) AS observations,
                   coalesce(c.n, 0) AS conditions_onset
            FROM {g}dim_date d
            LEFT JOIN (SELECT date_key, count(*) AS n FROM {g}fact_encounter GROUP BY 1) e ON e.date_key = d.date_key
            LEFT JOIN (SELECT date_key, count(*) AS n FROM {g}fact_observation GROUP BY 1) o ON o.date_key = d.date_key
            LEFT JOIN (SELECT onset_date_key AS date_key, count(*) AS n FROM {g}fact_condition GROUP BY 1) c
                   ON c.date_key = d.date_key
            WHERE coalesce(e.n, 0) + coalesce(o.n, 0) + coalesce(c.n, 0) > 0"""),

        ("vw_top_conditions", "view", None, f"""
            SELECT dc.code, dc.code_system, dc.display,
                   count(*) AS occurrences,
                   count(DISTINCT f.patient_id) AS patients,
                   count_if(f.clinical_status = 'active') AS active_occurrences
            FROM {g}fact_condition f JOIN {g}dim_code dc ON dc.code_sk = f.code_sk
            GROUP BY dc.code, dc.code_system, dc.display"""),

        # Latest value per patient for common LOINC vitals; BP panels contribute their components.
        ("vw_patient_vitals", "view", None, f"""
            WITH v AS (
                SELECT patient_id, effective_datetime, {vital_case} AS vital, value_quantity AS value
                FROM {g}fact_observation WHERE value_quantity IS NOT NULL
                UNION ALL
                SELECT patient_id, effective_datetime, 'bp_systolic', bp_systolic
                FROM {g}fact_observation WHERE bp_systolic IS NOT NULL
                UNION ALL
                SELECT patient_id, effective_datetime, 'bp_diastolic', bp_diastolic
                FROM {g}fact_observation WHERE bp_diastolic IS NOT NULL
            )
            SELECT patient_id,
                   {", ".join(f"max_by(value, effective_datetime) FILTER (WHERE vital = '{v}') AS {v}" for v in vitals)},
                   max(effective_datetime) AS last_vitals_at
            FROM v WHERE vital IS NOT NULL AND patient_id IS NOT NULL
            GROUP BY patient_id"""),

        ("vw_pipeline_health", "view", None, f"""
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
