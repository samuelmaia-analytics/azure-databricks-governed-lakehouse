import pytest
from pyspark.sql import functions as F

from src.gold.fact_order_items import build_fact_order_items


def test_union_keys_context_and_item_values(gold_sources, gold_facts):
    orders, prior, train = gold_sources
    items = gold_facts[0]
    rows = items.collect()
    assert len(rows) == prior.count() + train.count() == 7
    assert len({(r.order_id, r.product_id) for r in rows}) == 7
    assert {r.eval_set for r in rows} == {"prior", "train"}
    assert all(r.item_count == 1 for r in rows)
    actual = items.select(prior.columns).orderBy("order_id", "product_id").collect()
    expected = prior.unionByName(train).orderBy("order_id", "product_id").collect()
    assert actual == expected
    contexts = {r.order_id: r.asDict() for r in orders.collect()}
    assert all(all(r[column] == contexts[r.order_id][column] for column in orders.columns)
               for r in rows)


@pytest.mark.parametrize("order_id", [4, 5, 999])
def test_wrong_set_test_and_orphan_rejected(gold_sources, order_id):
    orders, prior, train = gold_sources
    wrong = prior.limit(1).withColumn("order_id", F.lit(order_id))
    with pytest.raises(ValueError, match="missing order or inconsistent eval_set"):
        build_fact_order_items(wrong, train.limit(0), orders)


def test_train_cannot_reference_prior(gold_sources):
    orders, prior, train = gold_sources
    with pytest.raises(ValueError, match="inconsistent eval_set"):
        build_fact_order_items(prior.limit(0), train.withColumn("order_id", F.lit(1)), orders)


@pytest.mark.parametrize("across_sets", [False, True])
def test_duplicate_composite_key_rejected(gold_sources, across_sets):
    orders, prior, train = gold_sources
    if across_sets:
        train = prior.limit(1)
    else:
        prior = prior.unionByName(prior.limit(1))
    with pytest.raises(ValueError, match="duplicate key"):
        build_fact_order_items(prior, train, orders)


def test_duplicate_orders_cannot_multiply_items(gold_sources):
    orders, prior, train = gold_sources
    with pytest.raises(ValueError, match="orders: null or duplicate key"):
        build_fact_order_items(prior, train, orders.unionByName(orders.limit(1)))


@pytest.mark.parametrize("value", [None, 2])
def test_invalid_reordered_rejected(gold_sources, value):
    orders, prior, train = gold_sources
    with pytest.raises(ValueError, match="reordered must be 0 or 1"):
        build_fact_order_items(prior.withColumn("reordered", F.lit(value).cast("int")),
                               train, orders)


def test_empty_inputs_and_missing_column(gold_sources):
    orders, prior, train = gold_sources
    assert build_fact_order_items(prior.limit(0), train.limit(0), orders.limit(0)).isEmpty()
    with pytest.raises(ValueError, match="Missing columns"):
        build_fact_order_items(prior.drop("product_id"), train, orders)
