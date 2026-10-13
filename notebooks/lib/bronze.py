"""Raw Volume files -> Bronze Delta tables: one row per FHIR resource, raw payload kept as received."""
import re
import xml.etree.ElementTree as ET

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

FHIR_NS_URI = "http://hl7.org/fhir"
FHIR_NS = {"f": FHIR_NS_URI}
ET.register_namespace("", FHIR_NS_URI)   # keep default xmlns when re-serializing resources

BRONZE_COLUMNS = """
    resource_id STRING,
    resource_type STRING,
    source_version_id STRING,
    source_last_updated TIMESTAMP,
    raw_resource STRING COMMENT 'resource exactly as returned by the API (JSON text or XML)',
    resource_variant VARIANT COMMENT 'parsed copy of raw_resource for querying; NULL for XML',
    source_format STRING,
    extraction_timestamp TIMESTAMP COMMENT 'when the API call that returned this record started',
    api_url_or_params STRING,
    run_id STRING,
    page_number INT,
    source_file STRING,
    bronze_loaded_at TIMESTAMP,
    load_date DATE
"""

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")

# Only the Bundle envelope gets a schema; typing `resource` as string makes Spark keep the
# nested object as its raw JSON text, so irregular FHIR schemas never break the read.
JSON_ENVELOPE = "struct<entry:array<struct<fullUrl:string,resource:string>>>"

XML_ENTRY_TYPE = T.ArrayType(T.StructType([
    T.StructField("resource_type", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("source_version_id", T.StringType()),
    T.StructField("source_last_updated", T.StringType()),
    T.StructField("raw_resource", T.StringType()),
]))


def bronze_table(catalog, resource):
    return f"{catalog}.bronze.{resource.lower()}"


def validate_run_id(run_id):
    if not run_id or not RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"invalid run_id '{run_id}'")
    return run_id


def ensure_bronze_table(spark, table):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} ({BRONZE_COLUMNS})
        USING DELTA
        PARTITIONED BY (load_date)
        COMMENT 'Append-only history of every FHIR resource version returned by the API'
    """)


def _normalize_path(col):
    # _metadata.file_path comes back as dbfs:/Volumes/...; the call log stores /Volumes/...
    return F.regexp_replace(col, r"^dbfs:", "")


def run_call_log(spark, catalog, resource, run_id):
    """Successfully saved pages for this run, one row per file (latest attempt wins on task retries)."""
    w = Window.partitionBy("raw_file_path").orderBy(F.col("file_saved_at").desc())
    return (spark.table(f"{catalog}.ops.api_call_log")
            .filter((F.col("run_id") == run_id) & (F.col("resource") == resource)
                    & F.col("status").isin("SUCCESS", "TRUNCATED")
                    & F.col("raw_file_path").isNotNull())
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))


def split_xml_bundle(text):
    """Split an XML searchset Bundle into one record per entry resource (pure Python, used as a UDF)."""
    if not text:
        return []
    root = ET.fromstring(text)
    records = []
    for res in root.findall("f:entry/f:resource/*", FHIR_NS):
        def value(path):
            el = res.find(path, FHIR_NS)
            return el.get("value") if el is not None else None
        records.append((res.tag.split("}")[-1], value("f:id"), value("f:meta/f:versionId"),
                        value("f:meta/f:lastUpdated"), ET.tostring(res, encoding="unicode")))
    return records


def parse_json_files(spark, paths):
    return (spark.read.text(paths, wholetext=True)
            .withColumn("source_file", _normalize_path(F.col("_metadata.file_path")))
            .withColumn("b", F.from_json("value", JSON_ENVELOPE))
            .select("source_file", F.explode("b.entry").alias("e"))
            .select("source_file", F.col("e.resource").alias("raw_resource"))
            .select("source_file",
                    F.get_json_object("raw_resource", "$.resourceType").alias("resource_type"),
                    F.get_json_object("raw_resource", "$.id").alias("resource_id"),
                    F.get_json_object("raw_resource", "$.meta.versionId").alias("source_version_id"),
                    F.get_json_object("raw_resource", "$.meta.lastUpdated").alias("source_last_updated"),
                    "raw_resource",
                    F.lit("json").alias("source_format")))


def parse_xml_files(spark, paths):
    split = F.udf(split_xml_bundle, XML_ENTRY_TYPE)
    return (spark.read.text(paths, wholetext=True)
            .withColumn("source_file", _normalize_path(F.col("_metadata.file_path")))
            .select("source_file", F.explode(split("value")).alias("e"))
            .select("source_file", "e.*", F.lit("xml").alias("source_format")))


def build_bronze(entries, call_log, resource):
    """Keep entries of the requested type and attach API-call lineage from ops.api_call_log."""
    log = call_log.select(
        F.col("raw_file_path").alias("source_file"),
        F.col("call_started_at").alias("extraction_timestamp"),
        F.col("request_url").alias("api_url_or_params"),
        "run_id", "page_number")
    return (entries
            .filter(F.col("resource_type") == resource)   # drops OperationOutcome etc.
            .join(log, "source_file", "left")
            .select(
                "resource_id", "resource_type", "source_version_id",
                F.expr("try_cast(source_last_updated AS TIMESTAMP)").alias("source_last_updated"),
                "raw_resource",
                F.when(F.col("source_format") == "json", F.expr("try_parse_json(raw_resource)"))
                 .alias("resource_variant"),
                "source_format", "extraction_timestamp", "api_url_or_params", "run_id",
                F.col("page_number").cast("int").alias("page_number"), "source_file",
                F.current_timestamp().alias("bronze_loaded_at"),
                F.to_date(F.regexp_extract("source_file", r"load_date=(\d{4}-\d{2}-\d{2})", 1))
                 .alias("load_date")))


def write_bronze(df, table, run_id):
    """Idempotent per run: rerunning a run_id replaces only that run's rows."""
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"run_id = '{validate_run_id(run_id)}'")
     .saveAsTable(table))
