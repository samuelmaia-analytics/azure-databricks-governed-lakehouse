"""Acquire a local Delta session or reuse the platform's Databricks session."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.common.paths import to_spark_path as to_spark_path
from src.common.runtime import RuntimeEnv, runtime_env


def get_spark_session(
    app_name: str, *, runtime: RuntimeEnv | None = None,
    existing: SparkSession | None = None,
) -> SparkSession:
    """Create or reuse a local session; importing this module starts nothing."""
    if existing is not None:
        return existing
    selected = RuntimeEnv(runtime) if runtime is not None else runtime_env()
    if selected == RuntimeEnv.DATABRICKS:
        active = SparkSession.getActiveSession()
        if active is None:
            raise RuntimeError("No active Databricks SparkSession; pass the platform spark explicitly")
        return active
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
