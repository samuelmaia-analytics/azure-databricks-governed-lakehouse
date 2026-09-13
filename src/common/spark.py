"""Local Spark session configured for Delta Lake."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def to_spark_path(path: str | Path) -> str:
    """Resolve a local path with forward slashes, preserving spaces without URI encoding."""
    return Path(path).resolve().as_posix()


def get_spark_session(app_name: str) -> SparkSession:
    """Create or reuse a local session; importing this module starts nothing."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
