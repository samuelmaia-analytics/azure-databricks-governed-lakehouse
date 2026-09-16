"""Order-product occurrences, preserving prior/train provenance."""

from pyspark.sql import DataFrame, functions as F

from src.gold.common import (
    ITEM_COLUMNS,
    ORDER_COLUMNS,
    prepare_orders,
    project,
    reject_rows,
    require_key,
)


def build_fact_order_items(
    order_products_prior: DataFrame, order_products_train: DataFrame, orders: DataFrame,
) -> DataFrame:
    """Union all items and reject orphan, duplicated or incorrectly classified items."""
    orders = prepare_orders(orders)
    prior = project(order_products_prior, ITEM_COLUMNS).withColumn("_item_set", F.lit("prior"))
    train = project(order_products_train, ITEM_COLUMNS).withColumn("_item_set", F.lit("train"))
    items = prior.unionByName(train)
    require_key(items, ("order_id", "product_id"), "order items")
    reject_rows(items.filter(F.col("reordered").isNull() | ~F.col("reordered").isin(0, 1)),
                "order items: reordered must be 0 or 1")
    expected = orders.select("order_id", F.col("eval_set").alias("_item_set"))
    reject_rows(items.join(expected, ["order_id", "_item_set"], "left_anti"),
                "order items: missing order or inconsistent eval_set")
    return (items.join(orders, ["order_id"], "inner")
            .select(*ORDER_COLUMNS, "product_id", "add_to_cart_order", "reordered")
            .withColumn("item_count", F.lit(1).cast("long")))
