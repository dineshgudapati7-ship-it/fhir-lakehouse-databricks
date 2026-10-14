# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/silver
# MAGIC Bronze -> Silver: config-driven flattening, cleaning, quarantine, dedup and change hash.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/silver`.

# COMMAND ----------

from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    SILVER_MAPPINGS_PATH = Path(__file__).resolve().parents[2] / "conf" / "silver_mappings.yaml"
except NameError:   # pulled in with %run from notebooks/
    SILVER_MAPPINGS_PATH = Path.cwd().parent / "conf" / "silver_mappings.yaml"

SILVER_TYPES = {"string", "lower", "upper", "date", "timestamp", "double", "int", "boolean", "reference"}
SILVER_RULES = {"not_future"}
LINEAGE_COLUMNS = ["_run_id", "_extracted_at", "_load_date"]


def silver_table(catalog, resource):
    return f"{catalog}.silver.{resource.lower()}"


def load_silver_mapping(resource, path=None):
    with open(path or SILVER_MAPPINGS_PATH, encoding="utf-8") as f:
        mappings = yaml.safe_load(f)
    if resource not in mappings:
        raise ValueError(f"no silver mapping for {resource}")
    mapping = mappings[resource]
    columns = mapping.get("columns") or {}
    if mapping.get("key") not in columns:
        raise ValueError(f"{resource}: key '{mapping.get('key')}' must be one of its columns")
    for name, spec in columns.items():
        if "expr" not in spec:
            raise ValueError(f"{resource}.{name}: missing expr")
        if spec.get("type", "string") not in SILVER_TYPES:
            raise ValueError(f"{resource}.{name}: unknown type '{spec['type']}'")
        if spec.get("rule") and spec["rule"] not in SILVER_RULES:
            raise ValueError(f"{resource}.{name}: unknown rule '{spec['rule']}'")
    return mapping


def clean_sql(expr, type_="string", rule=None):
    """SQL for one silver column: trim, blank -> NULL, then cast/normalize. Casts never fail (try_cast)."""
    base = f"nullif(trim(cast(({expr}) AS STRING)), '')"
    sql = {
        "string": base,
        "lower": f"lower({base})",
        "upper": f"upper({base})",
        "date": f"try_cast({base} AS DATE)",
        "timestamp": f"try_cast({base} AS TIMESTAMP)",
        "double": f"try_cast({base} AS DOUBLE)",
        "int": f"try_cast({base} AS INT)",
        "boolean": f"try_cast({base} AS BOOLEAN)",
        "reference": f"nullif(regexp_extract({base}, '([^/]+)$', 1), '')",
    }[type_]
    if rule == "not_future":
        limit = "current_date()" if type_ == "date" else "current_timestamp()"
        sql = f"CASE WHEN {sql} > {limit} THEN NULL ELSE {sql} END"
    return sql


def _resolves(df, expr):
    try:
        df.select(F.expr(expr)).schema   # forces analysis (also on Spark Connect / serverless)
        return True
    except Exception:
        return False


def flatten_resource(df, mapping):
    """Return (flat_df, missing_columns). Paths absent from this batch's Bronze schema become NULL."""
    selects, missing = [], []
    for name, spec in mapping["columns"].items():
        expr = spec["expr"]
        if not _resolves(df, expr):
            missing.append(name)
            expr = "NULL"
        selects.append(f"{clean_sql(expr, spec.get('type', 'string'), spec.get('rule'))} AS `{name}`")
    meta = [
        "meta.versionId AS source_version_id" if _resolves(df, "meta.versionId")
        else "CAST(NULL AS STRING) AS source_version_id",
        "try_cast(meta.lastUpdated AS TIMESTAMP) AS source_last_updated" if _resolves(df, "meta.lastUpdated")
        else "CAST(NULL AS TIMESTAMP) AS source_last_updated",
    ]
    return df.selectExpr(*selects, *meta, *LINEAGE_COLUMNS), missing


def split_quarantine(df, key):
    """Rows without a business key can't be tracked over time; keep them aside instead of dropping."""
    bad = (df.filter(F.col(key).isNull())
           .withColumn("_quarantine_reason", F.lit(f"missing {key}"))
           .withColumn("_quarantined_at", F.current_timestamp()))
    return df.filter(F.col(key).isNotNull()), bad


def write_quarantine(spark, df, table, run_id):
    """Idempotent per run: rerunning a run_id replaces that run's quarantined rows."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
    else:
        (df.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"_run_id = '{run_id}'").option("mergeSchema", "true")
         .saveAsTable(table))


def dedupe_latest(df, key):
    """One row per key: the latest source version in this batch."""
    w = Window.partitionBy(key).orderBy(F.col("source_last_updated").desc_nulls_last(),
                                        F.expr("try_cast(source_version_id AS INT)").desc_nulls_last(),
                                        F.col("_extracted_at").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_record_hash(df, business_columns):
    """sha256 over business columns only, so re-fetching an unchanged record doesn't create a version."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in business_columns]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *parts), 256))
