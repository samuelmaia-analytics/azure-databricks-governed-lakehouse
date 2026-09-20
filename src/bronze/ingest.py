"""Explicitly invoked CSV-to-Delta Bronze ingestion; Raw is read-only."""

import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from src.bronze.schemas import (
    AISLES_SCHEMA,
    DEPARTMENTS_SCHEMA,
    ORDER_PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
)
from src.common.spark import get_spark_session, to_spark_path
from src.common.paths import destination_path, is_remote_path, join_path, require_separate_paths
from src.common.runtime import RuntimeConfig, RuntimeEnv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dataset:
    filename: str
    schema: StructType


DATASETS = {
    "orders": Dataset("orders.csv", ORDERS_SCHEMA),
    "products": Dataset("products.csv", PRODUCTS_SCHEMA),
    "aisles": Dataset("aisles.csv", AISLES_SCHEMA),
    "departments": Dataset("departments.csv", DEPARTMENTS_SCHEMA),
    "order_products_prior": Dataset("order_products__prior.csv", ORDER_PRODUCTS_SCHEMA),
    "order_products_train": Dataset("order_products__train.csv", ORDER_PRODUCTS_SCHEMA),
}


def validate_source_file(source_file: str | Path) -> str | Path:
    """Validate local existence; remote existence/permissions are checked by Spark."""
    source = destination_path(source_file)
    if is_remote_path(source):
        return source
    if not source.is_file():
        raise FileNotFoundError(f"Source CSV file not found: {source}")
    return source


def read_csv(spark: SparkSession, source_file: str | Path, schema: StructType) -> DataFrame:
    """Read one CSV with the supplied schema and validate its header."""
    source = validate_source_file(source_file)
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", False)
        .option("inferSchema", False)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .csv(to_spark_path(source))
    )


def add_ingestion_metadata(
    dataframe: DataFrame, source_file: str | Path, batch_id: str
) -> DataFrame:
    """Record the source location, batch UUID and UTC ingestion timestamp."""
    return (
        dataframe.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(to_spark_path(source_file)))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def write_delta(dataframe: DataFrame, destination: str | Path) -> None:
    """Replace one dataset atomically through Delta's transaction log."""
    dataframe.write.format("delta").mode("overwrite").save(to_spark_path(destination))


def ingest_dataset(
    spark: SparkSession,
    dataset_name: str,
    raw_path: str | Path,
    bronze_path: str | Path,
    batch_id: str,
) -> str | Path:
    """Ingest one mapped dataset using the caller's session and batch."""
    dataset = DATASETS[dataset_name]
    source = validate_source_file(join_path(raw_path, dataset.filename))
    destination = destination_path(join_path(bronze_path, dataset_name))
    logger.info("Ingesting %s to %s (batch %s)", source, destination, batch_id)
    dataframe = read_csv(spark, source, dataset.schema)
    write_delta(add_ingestion_metadata(dataframe, source, batch_id), destination)
    logger.info("Completed %s (batch %s)", dataset_name, batch_id)
    return destination


def run_bronze_ingestion(
    raw_path: str | Path | None = None,
    bronze_path: str | Path | None = None,
    spark: SparkSession | None = None,
    config: RuntimeConfig | None = None,
) -> str:
    """Overwrite all six datasets with one UUID; return that batch ID.

    All sources are checked before writing. Each dataset commits separately;
    this function does not provide a transaction spanning all six datasets.
    A caller-supplied session remains open; an internally acquired one is stopped.
    """
    config = config or RuntimeConfig.from_env()
    raw = to_spark_path(raw_path if raw_path is not None else config.raw)
    bronze = to_spark_path(bronze_path if bronze_path is not None else config.bronze)
    require_separate_paths([raw, bronze])
    for dataset in DATASETS.values():
        validate_source_file(join_path(raw, dataset.filename))

    batch_id = str(uuid4())
    owns_session = spark is None and config.runtime == RuntimeEnv.LOCAL
    session = spark if spark is not None else get_spark_session(
        "Bronze ingestion", runtime=config.runtime)
    try:
        # Check remote headers/access before the first write; Spark owns remote I/O.
        if is_remote_path(raw):
            for dataset in DATASETS.values():
                read_csv(session, join_path(raw, dataset.filename), dataset.schema).limit(1).count()
        for dataset_name in DATASETS:
            ingest_dataset(session, dataset_name, raw, bronze, batch_id)
    finally:
        if owns_session:
            session.stop()
    return batch_id
