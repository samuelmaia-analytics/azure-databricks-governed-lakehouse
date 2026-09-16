import pytest
from pyspark.sql import functions as F

from src.gold.common import safe_divide
from src.gold.user_behavior import build_user_behavior


def test_measures_grouping_and_rates(gold_facts):
    result = gold_facts[2]
    rows = {(r.user_id, r.eval_set): r for r in result.collect()}
    assert result.count() == len(rows) == 5
    row = rows[1, "prior"]
    assert (row.orders_observed, row.orders_with_items, row.total_items,
            row.reordered_items, row.distinct_products) == (2, 2, 4, 1, 3)
    assert row.reorder_rate == pytest.approx(0.25)
    assert row.avg_items_per_eligible_order == pytest.approx(2)
    assert (row.days_since_prior_sum, row.days_since_prior_count) == (4, 1)
    assert row.avg_days_since_prior_order == pytest.approx(4)
    train = rows[1, "train"]
    assert (train.orders_observed, train.orders_with_items, train.total_items,
            train.reordered_items, train.distinct_products) == (1, 1, 2, 1, 2)
    assert train.reorder_rate == pytest.approx(0.5)
    assert train.avg_days_since_prior_order == pytest.approx(8)
    assert rows[2, "prior"].days_since_prior_count == 0
    assert rows[2, "prior"].avg_days_since_prior_order is None


def test_test_basket_metrics_unknown_and_coverage_zero(gold_facts):
    rows = {r.user_id: r for r in gold_facts[2].filter("eval_set = 'test'").collect()}
    for row in rows.values():
        assert row.orders_observed == 1
        assert row.orders_with_items == 0
        for column in ("total_items", "reordered_items", "distinct_products", "reorder_rate",
                       "avg_items_per_eligible_order"):
            assert row[column] is None
    assert rows[2].avg_days_since_prior_order == 12
    assert rows[3].days_since_prior_count == 0
    assert rows[3].avg_days_since_prior_order is None


def test_weighted_rate_is_not_simple_mean(gold_facts):
    prior = gold_facts[2].filter("eval_set = 'prior'")
    row = prior.agg(
        safe_divide(F.sum("reordered_items"), F.sum("total_items")).alias("weighted"),
        F.avg("reorder_rate").alias("simple"),
    ).first()
    assert row.weighted == pytest.approx(2 / 5)
    assert row.simple == pytest.approx((0.25 + 1) / 2)
    assert row.weighted != row.simple
    direct = gold_facts[0].filter("eval_set = 'prior'").agg(F.avg("reordered")).first()[0]
    assert row.weighted == pytest.approx(direct)


def test_safe_division_zero_and_null_even_with_ansi(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        result = spark.sql("""
            SELECT * FROM VALUES (1, 0), (0, 0), (1, CAST(NULL AS INT)), (1, 2)
            AS t(numerator, denominator)
        """).select(safe_divide(F.col("numerator"), F.col("denominator")).alias("ratio"))
        assert [r.ratio for r in result.collect()] == [None, None, None, 0.5]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_empty_facts(gold_facts):
    items, orders, _ = gold_facts
    result = build_user_behavior(orders.limit(0), items.limit(0))
    assert result.isEmpty()
    assert "reorder_rate" in result.columns
