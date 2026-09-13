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

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


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


def validate_source_file(source_file: str | Path) -> Path:
    """Return an absolute path or fail before any Spark work."""
    source = Path(source_file).resolve()
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
    """Record the absolute local source path, batch UUID and UTC ingestion timestamp."""
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
) -> Path:
    """Ingest one mapped dataset using the caller's session and batch."""
    dataset = DATASETS[dataset_name]
    source = validate_source_file(Path(raw_path) / dataset.filename)
    destination = Path(bronze_path).resolve() / dataset_name
    logger.info("Ingesting %s to %s (batch %s)", source, destination, batch_id)
    dataframe = read_csv(spark, source, dataset.schema)
    write_delta(add_ingestion_metadata(dataframe, source, batch_id), destination)
    logger.info("Completed %s (batch %s)", dataset_name, batch_id)
    return destination


def run_bronze_ingestion(
    raw_path: str | Path = PROJECT_ROOT / "data" / "raw",
    bronze_path: str | Path = PROJECT_ROOT / "data" / "bronze",
    spark: SparkSession | None = None,
) -> str:
    """Overwrite all six datasets with one UUID; return that batch ID.

    All sources are checked before writing. Each dataset commits separately;
    this function does not provide a transaction spanning all six datasets.
    A caller-supplied session remains open; an internally acquired one is stopped.
    """
    raw = Path(raw_path).resolve()
    bronze = Path(bronze_path).resolve()
    if raw == bronze or raw in bronze.parents or bronze in raw.parents:
        raise ValueError("Raw and Bronze directories must be separate and non-overlapping")
    for dataset in DATASETS.values():
        validate_source_file(raw / dataset.filename)

    batch_id = str(uuid4())
    owns_session = spark is None
    session = spark if spark is not None else get_spark_session("Bronze ingestion")
    try:
        for dataset_name in DATASETS:
            ingest_dataset(session, dataset_name, raw, bronze, batch_id)
    finally:
        if owns_session:
            session.stop()
    return batch_id
