"""Explicit, gate-controlled Silver persistence for caller-selected valid rows."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from src.common.spark import to_spark_path
from src.common.paths import destination_path as resolve_destination
from src.quality.engine import DQ_COLUMNS
from src.quality.publication_gate import PublicationDecision, PublicationStatus

SILVER_COLUMNS = ("_published_at", "_publication_status", "_publication_reason")


@dataclass(frozen=True)
class SilverPublicationResult:
    dataset: str
    destination_path: str | Path
    status: PublicationStatus
    reason: str
    row_count: int
    written: bool


def publish_silver(
    approved_df: DataFrame,
    dataset_name: str,
    destination_path: str | Path,
    gate_result: PublicationDecision,
) -> SilverPublicationResult:
    """Overwrite a Delta destination only for APPROVED, nonempty input.

    Empty APPROVED input is a no-op, preserving any existing destination. The
    normal gate requires review for empty datasets; this also protects callers
    supplying an external approval. Refused decisions perform no Spark actions.
    The caller supplies stable valid_df and all applicable checks to the gate.
    Existing columns are preserved; reserved publication columns are rejected.
    """
    if not dataset_name.strip() or gate_result.dataset != dataset_name:
        raise ValueError("Gate dataset must match nonempty dataset_name")
    destination = resolve_destination(destination_path)
    status = PublicationStatus(gate_result.status)
    if status != PublicationStatus.APPROVED:
        return SilverPublicationResult(
            dataset_name, destination, status, gate_result.reason, 0, False,
        )
    required = {"_ingested_at", "_source_file", "_batch_id", *DQ_COLUMNS}
    if not required.issubset(approved_df.columns):
        raise ValueError(f"Missing metadata: {sorted(required - set(approved_df.columns))}")
    if len(approved_df.columns) != len(set(approved_df.columns)):
        raise ValueError("Duplicate input columns")
    if set(SILVER_COLUMNS).intersection(approved_df.columns):
        raise ValueError("Input already contains publication metadata")
    count = approved_df.count()
    if not count:
        return SilverPublicationResult(
            dataset_name, destination, status, "Empty input: publication skipped", 0, False,
        )
    enriched = approved_df.select(
        "*", F.lit(datetime.now(timezone.utc)).alias("_published_at"),
        F.lit(status.value).alias("_publication_status"),
        F.lit(gate_result.reason).alias("_publication_reason"),
    )
    enriched.write.format("delta").mode("overwrite").save(to_spark_path(destination))
    return SilverPublicationResult(
        dataset_name, destination, status, gate_result.reason, count, True,
    )
