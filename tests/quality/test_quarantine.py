import pytest
from pyspark.sql.types import StringType, TimestampType

from src.common.spark import to_spark_path
from src.quality.engine import apply_quality_rules, build_quality_summary, split_valid_invalid
from src.quality.publication_gate import evaluate_publication_gate
from src.quality.quarantine import persist_quarantine
from src.quality.rules import QualityRule


def invalid_rows(spark):
    source = spark.sql("""
        SELECT *, timestamp'2026-01-01 00:00:00' _ingested_at,
            'synthetic.csv' _source_file, 'bronze-batch' _batch_id
        FROM VALUES (1, -1), (2, -2) AS t(id, amount)
    """)
    checked = apply_quality_rules(source, "sample", [
        QualityRule("positive", "sample", "Positivo", "amount > 0"),
    ])
    return split_valid_invalid(checked)[1]


def test_empty_creates_no_destination(spark, tmp_path):
    destination = tmp_path / "quarantine" / "sample"
    result = persist_quarantine(invalid_rows(spark).limit(0), "sample", destination, "batch")
    assert result.row_count == 0
    assert not result.written
    assert not destination.parent.exists()


@pytest.mark.parametrize("count", [1, 2])
def test_delta_roundtrip_metadata_and_overwrite(spark, tmp_path, count):
    frame = invalid_rows(spark).orderBy("id").limit(count)
    destination = tmp_path / "quarantine com espaços" / "sample"
    result = persist_quarantine(frame, "sample", destination, "first")
    assert result.written and result.row_count == count
    assert result.destination_path == destination.resolve()
    assert (destination / "_delta_log").is_dir()
    restored = spark.read.format("delta").load(to_spark_path(destination))
    assert restored.count() == count
    assert restored.select(frame.columns).orderBy("id").collect() == frame.orderBy("id").collect()
    assert [(f.name, f.dataType) for f in restored.schema][:len(frame.columns)] == [
        (f.name, f.dataType) for f in frame.schema
    ]
    assert isinstance(restored.schema["_quarantined_at"].dataType, TimestampType)
    assert isinstance(restored.schema["_quarantine_batch_id"].dataType, StringType)
    for row in restored.collect():
        assert row._quarantined_at is not None
        assert row._quarantine_dataset == "sample"
        assert row._quarantine_batch_id == "first"
        assert row._batch_id == "bronze-batch"
    persist_quarantine(frame.limit(1), "sample", destination, "second")
    updated = spark.read.format("delta").load(to_spark_path(destination))
    assert updated.count() == 1
    assert updated.first()._quarantine_batch_id == "second"
    empty = persist_quarantine(frame.limit(0), "sample", destination, "empty")
    assert not empty.written and empty.row_count == 0
    assert spark.read.format("delta").load(to_spark_path(destination)).first() == updated.first()
    summary = build_quality_summary(frame, "sample").first().asDict()
    assert evaluate_publication_gate(summary).status == "BLOCKED"


def test_missing_or_reserved_metadata(spark, tmp_path):
    with pytest.raises(ValueError, match="Missing metadata"):
        persist_quarantine(spark.sql("SELECT 1 id"), "sample", tmp_path / "out", "batch")
    from pyspark.sql import functions as F
    frame = invalid_rows(spark).withColumn("_quarantine_batch_id", F.lit("existing"))
    with pytest.raises(ValueError, match="already contains"):
        persist_quarantine(frame, "sample", tmp_path / "out", "batch")
    assert not (tmp_path / "out").exists()
