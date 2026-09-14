import pytest

from src.quality.engine import apply_quality_rules, build_quality_summary, split_valid_invalid
from src.quality.rules import QualityRule, Severity


def sample(spark):
    return spark.sql("""
        SELECT *, timestamp'2026-01-01 00:00:00' AS _ingested_at,
            'synthetic.csv' AS _source_file, 'synthetic-batch' AS _batch_id
        FROM VALUES (1, 10, 'ok'), (2, -1, 'ok'), (3, -1, ''), (4, 10, '')
        AS rows(id, amount, label)
    """)


def rules():
    return (
        QualityRule("positive", "sample", "Valor positivo", "amount > 0"),
        QualityRule("label_present", "sample", "Nome preenchido", "length(trim(label)) > 0",
                    Severity.WARNING),
    )


def test_errors_warnings_metadata_and_split(spark):
    original = sample(spark)
    checked = apply_quality_rules(original, "sample", rules())
    rows = checked.orderBy("id").collect()
    assert [(r._dq_failed_rules, r._dq_error_count, r._dq_warning_count) for r in rows] == [
        ([], 0, 0), (["positive"], 1, 0),
        (["positive", "label_present"], 1, 1), (["label_present"], 0, 1),
    ]
    assert checked.select(original.columns).orderBy("id").collect() == original.orderBy("id").collect()
    assert all(row._dq_checked_at is not None for row in rows)
    assert len({row._dq_checked_at for row in rows}) == 1
    valid, invalid = split_valid_invalid(checked)
    assert [r.id for r in valid.orderBy("id").collect()] == [1, 4]
    assert [r.id for r in invalid.orderBy("id").collect()] == [2, 3]
    summary = build_quality_summary(checked, "sample").first()
    assert (summary.dataset, summary.total_rows, summary.valid_rows, summary.invalid_rows,
            summary.error_rate, summary.warning_rows) == ("sample", 4, 2, 2, 0.5, 2)
    assert summary.checked_at == rows[0]._dq_checked_at


def test_multiple_errors(spark):
    selected = (*rules(), QualityRule("label_required", "sample", "Nome obrigatório", "label != ''"))
    row = apply_quality_rules(sample(spark).filter("id = 3"), "sample", selected).first()
    assert row._dq_failed_rules == ["positive", "label_present", "label_required"]
    assert row._dq_error_count == 2
    assert row._dq_warning_count == 1


def test_warning_only_rules(spark):
    checked = apply_quality_rules(sample(spark), "sample", [rules()[1]])
    valid, invalid = split_valid_invalid(checked)
    assert valid.count() == 4
    assert invalid.count() == 0
    summary = build_quality_summary(checked, "sample").first()
    assert summary.error_rate == 0.0
    assert summary.warning_rows == 2


def test_empty_dataframe(spark):
    checked = apply_quality_rules(sample(spark).limit(0), "sample", rules())
    valid, invalid = split_valid_invalid(checked)
    assert valid.count() == invalid.count() == 0
    assert valid.schema == invalid.schema == checked.schema
    row = build_quality_summary(checked, "sample").first()
    assert (row.total_rows, row.valid_rows, row.invalid_rows, row.warning_rows, row.error_rate) == (
        0, 0, 0, 0, 0.0,
    )
    assert row.checked_at is not None


def test_missing_rules(spark):
    with pytest.raises(ValueError, match="No quality rules configured"):
        apply_quality_rules(sample(spark), "unknown")
    with pytest.raises(ValueError, match="No quality rules configured"):
        apply_quality_rules(sample(spark), "sample", [])


def test_rule_configuration_errors(spark):
    with pytest.raises(ValueError, match="unique"):
        apply_quality_rules(sample(spark), "sample", [rules()[0], rules()[0]])
    with pytest.raises(ValueError, match="requested dataset"):
        apply_quality_rules(sample(spark), "different", rules())
    checked = apply_quality_rules(sample(spark), "sample", rules())
    with pytest.raises(ValueError, match="reserved quality columns"):
        apply_quality_rules(checked, "sample", rules())
