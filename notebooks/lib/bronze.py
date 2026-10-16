# Databricks notebook source
# MAGIC %md
# MAGIC ## lib/bronze
# MAGIC Raw Volume JSON pages -> Bronze Delta tables, one row per FHIR resource with its fields as columns.
# MAGIC
# MAGIC Shared helper notebook: include it with `%run ./lib/bronze`.

# COMMAND ----------

import re

from pyspark.sql import functions as F

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")


def bronze_table(catalog, resource):
    return f"{catalog}.bronze.{resource.lower()}"


def validate_run_id(run_id):
    if not run_id or not RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"invalid run_id '{run_id}'")
    return run_id


def run_call_log(spark, catalog, resource, run_id):
    """Pages successfully saved for this run (one row per file)."""
    return (spark.table(f"{catalog}.ops.api_call_log")
            .filter((F.col("run_id") == run_id) & (F.col("resource") == resource)
                    & F.col("status").isin("SUCCESS", "TRUNCATED"))
            .dropDuplicates(["raw_file_path"]))


def read_resources(spark, paths, resource):
    """Explode Bundle pages into one row per resource, keeping FHIR fields as native columns.

    primitivesAsString keeps every leaf value as a string, so a field inferred as a number one day
    and a decimal the next can't break the schema; Silver casts to proper types.
    """
    bundles = (spark.read.option("multiLine", True).option("primitivesAsString", True).json(paths)
               .withColumn("_source_file", F.regexp_replace("_metadata.file_path", "^dbfs:", "")))
    if "entry" not in bundles.columns:   # every page in this run was empty
        return None
    return (bundles.select("_source_file", F.explode("entry.resource").alias("r"))
            .filter(F.col("r.resourceType") == resource)   # skip OperationOutcome etc.
            .select("r.*", "_source_file"))


def add_lineage(df, call_log):
    log = call_log.select(F.col("raw_file_path").alias("_source_file"),
                          F.col("run_id").alias("_run_id"),
                          F.col("call_started_at").alias("_extracted_at"),
                          F.col("request_url").alias("_api_url"))
    return (df.join(F.broadcast(log), "_source_file", "left")
            .withColumn("_load_date",
                        F.to_date(F.regexp_extract("_source_file", r"load_date=(\d{4}-\d{2}-\d{2})", 1)))
            .withColumn("_loaded_at", F.current_timestamp()))


def write_bronze(spark, df, table, run_id):
    """Append with schema evolution; rerunning the same run_id replaces only that run's rows."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"_run_id = '{validate_run_id(run_id)}'")
     .option("mergeSchema", "true")
     .saveAsTable(table))
