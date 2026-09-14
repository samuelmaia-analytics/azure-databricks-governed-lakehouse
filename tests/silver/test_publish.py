from dataclasses import replace
from unittest.mock import Mock

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from src.common.spark import to_spark_path
from src.quality.engine import apply_quality_rules, build_quality_summary, split_valid_invalid
from src.quality.publication_gate import PublicationStatus, evaluate_publication_gate
from src.quality.rules import QualityRule
from src.silver.publish import SILVER_COLUMNS, publish_silver


@pytest.fixture
def approved(spark):
    source = spark.sql("""
        SELECT *, timestamp'2026-01-01 00:00:00' _ingested_at,
            'synthetic.csv' _source_file, 'bronze-batch' _batch_id
        FROM VALUES (1, 10), (2, 20) AS t(id, amount)
    """)
    checked = apply_quality_rules(source, "sample", [
        QualityRule("positive", "sample", "Positive", "amount > 0"),
    ])
    gate = evaluate_publication_gate(build_quality_summary(checked, "sample").first().asDict())
    return split_valid_invalid(checked)[0], gate


def test_approved_delta_roundtrip_preserves_all_columns(spark, tmp_path, approved):
    frame, gate = approved
    destination = tmp_path / "silver with spaces" / "sample"
    result = publish_silver(frame, "sample", destination, gate)
    assert result.written and result.row_count == 2
    assert result.status == PublicationStatus.APPROVED
    assert result.reason == gate.reason
    assert result.destination_path == destination.resolve()
    assert (destination / "_delta_log").is_dir()
    restored = spark.read.format("delta").load(to_spark_path(destination))
    assert restored.columns == frame.columns + list(SILVER_COLUMNS)
    assert restored.select(frame.columns).orderBy("id").collect() == frame.orderBy("id").collect()
    assert [(f.name, f.dataType) for f in restored.schema][:len(frame.columns)] == [
        (f.name, f.dataType) for f in frame.schema
    ]
    assert isinstance(restored.schema["_published_at"].dataType, TimestampType)
    for name in ("_publication_status", "_publication_reason"):
        assert isinstance(restored.schema[name].dataType, StringType)
    for row in restored.collect():
        assert row._published_at is not None
        assert row._publication_status == "APPROVED"
        assert row._publication_reason == gate.reason


@pytest.mark.parametrize("status", [PublicationStatus.NEEDS_REVIEW, PublicationStatus.BLOCKED])
def test_refused_without_dataframe_actions(tmp_path, approved, status):
    _, gate = approved
    frame = Mock()
    destination = tmp_path / "silver" / "sample"
    result = publish_silver(frame, "sample", destination, replace(gate, status=status))
    assert not result.written and result.row_count == 0
    assert result.status == status and result.reason == gate.reason
    assert not destination.parent.exists()
    assert not frame.mock_calls


def test_overwrite_empty_and_refused_preserve_existing(spark, tmp_path, approved):
    frame, gate = approved
    destination = tmp_path / "sample"
    publish_silver(frame, "sample", destination, gate)
    publish_silver(frame.limit(1), "sample", destination, gate)
    restored = spark.read.format("delta").load(to_spark_path(destination)).collect()
    assert len(restored) == 1
    log_before = sorted(p.name for p in (destination / "_delta_log").iterdir())
    empty = publish_silver(frame.limit(0), "sample", destination, gate)
    assert not empty.written and empty.row_count == 0 and "Empty" in empty.reason
    for status in (PublicationStatus.NEEDS_REVIEW, PublicationStatus.BLOCKED):
        assert not publish_silver(frame, "sample", destination, replace(gate, status=status)).written
    assert spark.read.format("delta").load(to_spark_path(destination)).collect() == restored
    assert sorted(p.name for p in (destination / "_delta_log").iterdir()) == log_before


def test_empty_approved_creates_no_destination(tmp_path, approved):
    frame, gate = approved
    destination = tmp_path / "silver" / "sample"
    result = publish_silver(frame.limit(0), "sample", destination, gate)
    assert not result.written and result.row_count == 0
    assert not destination.parent.exists()


def test_other_dataset_rejected(tmp_path, approved):
    frame, gate = approved
    with pytest.raises(ValueError, match="Gate dataset"):
        publish_silver(frame, "other", tmp_path / "out", gate)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("column", [
    "_ingested_at", "_source_file", "_batch_id", "_dq_failed_rules",
    "_dq_error_count", "_dq_warning_count", "_dq_checked_at",
])
def test_missing_metadata_rejected(tmp_path, approved, column):
    frame, gate = approved
    with pytest.raises(ValueError, match="Missing metadata"):
        publish_silver(frame.drop(column), "sample", tmp_path / "out", gate)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("column", SILVER_COLUMNS)
def test_reserved_columns_rejected(tmp_path, approved, column):
    frame, gate = approved
    with pytest.raises(ValueError, match="already contains"):
        publish_silver(frame.withColumn(column, F.lit("existing")), "sample", tmp_path / "out", gate)
    assert not (tmp_path / "out").exists()


def test_duplicate_columns_rejected(tmp_path, approved):
    frame, gate = approved
    with pytest.raises(ValueError, match="Duplicate"):
        publish_silver(frame.select("*", "id"), "sample", tmp_path / "out", gate)
    assert not (tmp_path / "out").exists()
