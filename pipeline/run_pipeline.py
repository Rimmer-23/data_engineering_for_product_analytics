"""Run the Trino usage DWH pipeline: DDL, then transformations, optionally demo data and queries."""
import argparse
import logging
import time
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from generate_sample_data import SAMPLE_START, generate_sample_data

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"
WAREHOUSE = ROOT / "warehouse"
NAMESPACES = ["bronze", "ref", "silver", "gold"]

log = logging.getLogger("pipeline")


def build_spark():
    return (
        SparkSession.builder.appName("trino-usage-dwh")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE.as_uri())
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def hide_temp_dir_errors(spark):
    """On Windows Spark can't delete its temp dir at exit because the Iceberg jar is
    still locked. The error is harmless, so turn off the two loggers that report it."""
    log4j = spark._jvm.org.apache.logging.log4j
    for logger in ["org.apache.spark.util.ShutdownHookManager", "org.apache.spark.SparkEnv"]:
        log4j.core.config.Configurator.setLevel(logger, log4j.Level.toLevel("OFF"))


def target_table(sql_file):
    """'10_silver_queries.sql' -> 'silver.queries'."""
    layer, table = sql_file.stem.split("_", 1)[1].split("_", 1)
    return f"{layer}.{table}"


def run_sql_files(spark, folder, start_date):
    for sql_file in sorted((SQL_DIR / folder).glob("*.sql")):
        started = time.time()
        sql = sql_file.read_text(encoding="utf-8").replace("${start_date}", start_date)
        spark.sql(sql.strip().rstrip(";"))
        rows = spark.table(target_table(sql_file)).count()
        log.info("%-40s rows=%-6d %.1fs", sql_file.name, rows, time.time() - started)


def run_queries(spark):
    for sql_file in sorted((SQL_DIR / "queries").glob("*.sql")):
        print(f"\n=== {sql_file.name} ===")
        spark.sql(sql_file.read_text(encoding="utf-8").strip().rstrip(";")).show(truncate=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=date.fromisoformat,
                        help="YYYY-MM-DD, default: today - 3 days (with --demo: start of the sample data)")
    parser.add_argument("--demo", action="store_true", help="fill Bronze and Ref with sample data")
    parser.add_argument("--run-queries", action="store_true", help="run the example queries")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.start_date:
        start_date = args.start_date.isoformat()
    elif args.demo:
        start_date = SAMPLE_START.date().isoformat()
    else:
        start_date = (date.today() - timedelta(days=3)).isoformat()
    log.info("start_date=%s", start_date)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    hide_temp_dir_errors(spark)
    for namespace in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    run_sql_files(spark, "ddl", start_date)
    if args.demo:
        started = time.time()
        generate_sample_data(spark)
        log.info("%-40s rows=%-6d %.1fs", "generate_sample_data",
                 spark.table("bronze.trino_query_events").count(), time.time() - started)
    run_sql_files(spark, "transform", start_date)
    if args.run_queries:
        run_queries(spark)
    spark.stop()


if __name__ == "__main__":
    main()
