"""Explicit Delta persistence for caller-selected invalid rows."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from src.common.spark import to_spark_path
from src.quality.engine import DQ_COLUMNS

QUARANTINE_COLUMNS = ("_quarantined_at", "_quarantine_dataset", "_quarantine_batch_id")


@dataclass(frozen=True)
class QuarantineResult:
    dataset: str
    destination_path: Path
    batch_id: str
    row_count: int
    written: bool


def persist_quarantine(
    invalid_df: DataFrame, dataset_name: str, destination_path: str | Path, batch_id: str
) -> QuarantineResult:
    """Overwrite the exact destination; empty input is a no-op, including existing tables."""
    if not dataset_name.strip() or not batch_id.strip():
        raise ValueError("dataset_name and batch_id must be nonempty")
    required = {"_ingested_at", "_source_file", "_batch_id", *DQ_COLUMNS}
    if not required.issubset(invalid_df.columns):
        raise ValueError(f"Missing metadata: {sorted(required - set(invalid_df.columns))}")
    if len(invalid_df.columns) != len(set(invalid_df.columns)):
        raise ValueError("Duplicate input columns")
    if set(QUARANTINE_COLUMNS).intersection(invalid_df.columns):
        raise ValueError("Input already contains quarantine metadata")
    destination = Path(destination_path).resolve()
    count = invalid_df.count()
    if count:
        enriched = invalid_df.select(
            "*", F.lit(datetime.now(timezone.utc)).alias("_quarantined_at"),
            F.lit(dataset_name).alias("_quarantine_dataset"),
            F.lit(batch_id).alias("_quarantine_batch_id"),
        )
        enriched.write.format("delta").mode("overwrite").save(to_spark_path(destination))
    return QuarantineResult(dataset_name, destination, batch_id, count, bool(count))
