import pytest

from src.quality.checks import (
    REFERENTIAL_CHECKS, UNIQUENESS_CHECKS, CheckType, ReferentialCheck,
    check_referential_integrity, check_uniqueness, run_dataset_checks,
)
from tests.quality.test_rules import VALID_SQL


def synthetic_tables(spark):
    return {name: spark.sql(sql) for name, sql in VALID_SQL.items()}


@pytest.mark.parametrize("check", REFERENTIAL_CHECKS, ids=lambda c: f"{c.dataset}-{c.name}")
def test_valid_reference(spark, check):
    tables = synthetic_tables(spark)
    if check.dataset == "order_products_train":
        tables["orders"] = spark.sql("SELECT 1 order_id, 'train' eval_set")
    row = check_referential_integrity(
        tables[check.dataset], tables[check.reference_dataset], check
    ).first()
    assert (row.dataset, row.check_name, row.check_type, row.status, row.failed_rows,
            row.total_rows, row.failure_rate, row.severity) == (
        check.dataset, check.name, "REFERENTIAL_INTEGRITY", "PASS", 0, 1, 0.0, "ERROR",
    )
    assert row.checked_at is not None


@pytest.mark.parametrize("dataset,check_name,reference_sql", [
    ("products", "aisle_exists", "SELECT 99 aisle_id"),
    ("products", "department_exists", "SELECT 99 department_id"),
    ("order_products_prior", "product_exists", "SELECT 99 product_id"),
    ("order_products_train", "product_exists", "SELECT 99 product_id"),
    ("order_products_prior", "order_exists", "SELECT 99 order_id"),
    ("order_products_train", "order_exists", "SELECT 99 order_id"),
    ("order_products_prior", "order_eval_set_matches", "SELECT 1 order_id, 'train' eval_set"),
    ("order_products_train", "order_eval_set_matches", "SELECT 1 order_id, 'prior' eval_set"),
])
def test_invalid_reference(spark, dataset, check_name, reference_sql):
    check = next(c for c in REFERENTIAL_CHECKS if c.dataset == dataset and c.name == check_name)
    row = check_referential_integrity(
        spark.sql(VALID_SQL[dataset]), spark.sql(reference_sql), check
    ).first()
    assert (row.status, row.failed_rows, row.total_rows, row.failure_rate) == ("FAIL", 1, 1, 1.0)


@pytest.mark.parametrize("check", UNIQUENESS_CHECKS, ids=lambda c: c.dataset)
def test_unique_and_duplicate_keys(spark, check):
    from pyspark.sql import functions as F

    original = spark.sql(VALID_SQL[check.dataset])
    assert check_uniqueness(original, check).first().status == "PASS"
    different = original.withColumn(check.columns[-1], F.lit(2))
    duplicated = original.unionByName(original).unionByName(different)
    row = check_uniqueness(duplicated, check).first()
    assert (row.status, row.failed_rows, row.total_rows, row.severity, row.check_type) == (
        "FAIL", 2, 3, "ERROR", "UNIQUENESS",
    )
    assert row.failure_rate == pytest.approx(2 / 3)


def test_empty_datasets_and_complete_result(spark):
    tables = {name: frame.limit(0) for name, frame in synthetic_tables(spark).items()}
    result = run_dataset_checks(tables)
    assert result.columns == ["dataset", "check_name", "check_type", "status", "failed_rows",
                              "total_rows", "failure_rate", "severity", "checked_at"]
    rows = result.collect()  # Only 14 aggregate result rows, never source data.
    assert len(rows) == 14
    assert len({(r.dataset, r.check_name) for r in rows}) == 14
    assert all((r.status, r.total_rows, r.failed_rows, r.failure_rate) == ("PASS", 0, 0, 0.0)
               for r in rows)
    assert {t.value for t in CheckType} == {"ROW_RULE", "REFERENTIAL_INTEGRITY", "UNIQUENESS"}


def test_reference_duplicates_null_and_empty_reference(spark):
    check = ReferentialCheck("fk", "child", ("id",), "parent", ("key",))
    child = spark.sql("SELECT * FROM VALUES (1), (2), (2), (NULL) AS t(id)")
    parent = spark.sql("SELECT * FROM VALUES (1), (1), (NULL) AS t(key)")
    row = check_referential_integrity(child, parent, check).first()
    assert (row.total_rows, row.failed_rows, row.failure_rate) == (4, 3, 0.75)
    assert check_referential_integrity(child, parent.limit(0), check).first().failed_rows == 4


def test_null_duplicate_keys(spark):
    check = next(c for c in UNIQUENESS_CHECKS if c.dataset == "orders")
    frame = spark.sql("SELECT * FROM VALUES (NULL), (NULL), (1) AS t(order_id)")
    assert check_uniqueness(frame, check).first().failed_rows == 2


def test_missing_inputs(spark):
    with pytest.raises(ValueError, match="Missing datasets"):
        run_dataset_checks({})
    with pytest.raises(ValueError, match="Missing check columns"):
        check_uniqueness(spark.sql("SELECT 1 unrelated"), UNIQUENESS_CHECKS[0])
