import pytest
from pyspark.sql import functions as F

from src.gold.fact_orders import build_fact_orders


def test_baskets_and_all_orders_preserved(gold_sources, gold_facts):
    orders = gold_sources[0]
    baskets = gold_facts[1]
    rows = {r.order_id: r for r in baskets.collect()}
    assert baskets.count() == orders.count() == len(rows) == 6
    assert baskets.select(orders.columns).orderBy("order_id").collect() == (
        orders.orderBy("order_id").collect()
    )
    for order_id, size, reorders in [(1, 3, 0), (2, 1, 1), (3, 1, 1), (4, 2, 1)]:
        row = rows[order_id]
        assert (row.item_count, row.reordered_item_count) == (size, reorders)
        assert row.items_available is True
        assert row.has_reordered_item is (reorders > 0)
    for order_id in (5, 6):
        row = rows[order_id]
        assert row.items_available is False
        assert row.item_count is row.reordered_item_count is row.has_reordered_item is None


@pytest.mark.parametrize("missing_id", [1, 4])
def test_missing_prior_or_train_basket_rejected(gold_sources, gold_facts, missing_id):
    with pytest.raises(ValueError, match="prior/train order missing items"):
        build_fact_orders(gold_sources[0], gold_facts[0].filter(F.col("order_id") != missing_id))


def test_duplicate_items_cannot_inflate_baskets(gold_sources, gold_facts):
    items = gold_facts[0]
    with pytest.raises(ValueError, match="duplicate key"):
        build_fact_orders(gold_sources[0], items.unionByName(items.limit(1)))


def test_test_items_rejected(gold_sources, gold_facts):
    items = gold_facts[0].limit(1).withColumn("order_id", F.lit(5)).withColumn("eval_set", F.lit("test"))
    with pytest.raises(ValueError, match="inconsistent eval_set"):
        build_fact_orders(gold_sources[0], items)


def test_only_test_orders(gold_sources, gold_facts):
    result = build_fact_orders(gold_sources[0].filter("eval_set = 'test'"), gold_facts[0].limit(0))
    assert result.count() == 2
    assert result.filter("items_available OR item_count IS NOT NULL").isEmpty()


def test_empty_orders(gold_sources, gold_facts):
    assert build_fact_orders(gold_sources[0].limit(0), gold_facts[0].limit(0)).isEmpty()
