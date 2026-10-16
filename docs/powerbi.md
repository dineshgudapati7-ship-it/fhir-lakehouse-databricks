# Power BI report

A one-page report on the Gold layer, built in Power BI Desktop. The finished file is committed as
`powerbi/fhir_clinical_overview.pbix`, with a PDF export next to it.

## 1. Connect

1. Find the warehouse connection details in Databricks: **SQL Warehouses → Serverless Starter
   Warehouse → Connection details**.
   - Server hostname: `dbc-5311cbfd-5d84.cloud.databricks.com`
   - HTTP path: `/sql/1.0/warehouses/75f7692daa198097`
2. In Power BI Desktop: **Get data → Databricks**. The workspace runs on AWS, so use the
   *Databricks* connector, not *Azure Databricks*.
3. Paste the hostname and HTTP path. Choose **Import**: the data is small, and Import avoids waking
   the warehouse on every click.
4. Sign in with your Databricks account (OAuth), or use a personal access token from **User settings →
   Developer → Access tokens**.
5. In the navigator, open `fhir` → `gold` and select:
   `dim_patient`, `fact_encounter`, `fact_observation`, `fact_condition`,
   `vw_daily_clinical_activity`, `vw_top_conditions`, `vw_pipeline_health`.

## 2. Model

In **Model view**, check these relationships. Power BI usually detects them from the `patient_id`
column names.

| From (one) | To (many) | Cardinality | Cross-filter |
|---|---|---|---|
| `dim_patient[patient_id]` | `fact_encounter[patient_id]` | 1 : * | single |
| `dim_patient[patient_id]` | `fact_observation[patient_id]` | 1 : * | single |
| `dim_patient[patient_id]` | `fact_condition[patient_id]` | 1 : * | single |

The three views stand alone, with no relationships. Delete any relationship Power BI creates between
the views and the other tables.

Sort the age bands in order: select `dim_patient[age_band]`, then **Column tools → Sort by column →
`age_band_order`**. `age_band_order` comes from Gold. Don't create it as a DAX calculated column
instead: a calculated column derived from `age_band` can't be its sort key, and Power BI reports a
circular dependency.

## 3. Measures

Create a table `_Measures` (**Enter data**, one empty column) to hold them:

```dax
Patients            = COUNTROWS ( dim_patient )
Encounters          = COUNTROWS ( fact_encounter )
Observations        = COUNTROWS ( fact_observation )
Conditions          = COUNTROWS ( fact_condition )
Active Conditions   = CALCULATE ( [Conditions], fact_condition[clinical_status] = "active" )
Avg Encounter Hours = AVERAGE ( fact_encounter[duration_hours] )
Records Fetched     = SUM ( vw_pipeline_health[records_fetched] )
Failed API Calls    = SUM ( vw_pipeline_health[failed_calls] )
```

## 4. Page layout — "FHIR Clinical Overview"

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ FHIR Clinical Overview          [Gender slicer]   [Age band slicer]          │
├────────────┬────────────┬────────────┬────────────┬──────────────────────────┤
│ Patients   │ Encounters │Observations│ Conditions │ Active conditions        │  cards
├────────────┴────────────┴────────────┴────────────┴──────────────────────────┤
│ Daily clinical activity (line: encounters / observations / conditions)       │
├──────────────────────────────────────┬───────────────────────────────────────┤
│ Top 10 conditions (bar)              │ Patients by gender (donut)            │
│                                      ├───────────────────────────────────────┤
│                                      │ Patients by age band (column)         │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ Encounters by class (bar)            │ Pipeline health (table)               │
└──────────────────────────────────────┴───────────────────────────────────────┘
```

| Visual | Fields | Settings |
|---|---|---|
| Cards | `Patients`, `Encounters`, `Observations`, `Conditions`, `Active Conditions` | — |
| Slicers | `dim_patient[gender]`, `dim_patient[age_band]` | dropdown; they filter the cards and patient-based visuals |
| Daily clinical activity | X: `vw_daily_clinical_activity[activity_date]`; Y: `encounters`, `observations`, `conditions` | filter `activity_date` to the load period (e.g. last 30 days). Older dates exist (historic events) but are sparse. |
| Top 10 conditions | Y: `fact_condition[code_display]`; X: `Conditions` | Top N filter: 10 by `Conditions` |
| Patients by gender | Legend: `dim_patient[gender]`; Values: `Patients` | — |
| Patients by age band | X: `dim_patient[age_band]`; Y: `Patients` | sorted by `age_band_order` |
| Encounters by class | Y: `fact_encounter[class_code]`; X: `Encounters` | tooltip: `Avg Encounter Hours` |
| Pipeline health | `run_id`, `resource`, `load_date`, `api_calls`, `records_fetched`, `new_records`, `changed_records`, `failed_calls` | sort by `started_at` descending |

`Top 10 conditions` uses `fact_condition` rather than `vw_top_conditions`, so the gender and age band
slicers filter it. `vw_top_conditions` is there for SQL users and quick checks.

## 5. Sanity check

After a refresh, compare the cards with SQL:

```sql
SELECT (SELECT count(*) FROM fhir.gold.dim_patient)      AS patients,
       (SELECT count(*) FROM fhir.gold.fact_encounter)   AS encounters,
       (SELECT count(*) FROM fhir.gold.fact_observation) AS observations,
       (SELECT count(*) FROM fhir.gold.fact_condition)   AS conditions;
```

## 6. Save and export

- Save as `powerbi/fhir_clinical_overview.pbix`.
- **File → Export → Export to PDF** → `powerbi/fhir_clinical_overview.pdf`.
- Commit both. The `.pbix` holds imported data (a few thousand rows of public test data), so that's
  fine to commit here.
