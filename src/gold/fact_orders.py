"""One row per order, with unknown baskets preserved as NULL."""

from pyspark.sql import DataFrame, functions as F

from src.gold.common import ORDER_COLUMNS, prepare_orders, project, reject_rows, require_key


def build_fact_orders(orders: DataFrame, fact_order_items: DataFrame) -> DataFrame:
    """Reuse Gold items; fail if a prior/train basket is unexpectedly absent."""
    orders = prepare_orders(orders)
    items = project(fact_order_items, ("order_id", "product_id", "eval_set", "reordered"))
    require_key(items, ("order_id", "product_id"), "order items")
    reject_rows(items.filter(F.col("reordered").isNull() | ~F.col("reordered").isin(0, 1)),
                "order items: reordered must be 0 or 1")
    eligible = orders.filter(F.col("eval_set").isin("prior", "train"))
    reject_rows(items.join(eligible.select("order_id", "eval_set"),
                           ["order_id", "eval_set"], "left_anti"),
                "order items: missing order or inconsistent eval_set")
    baskets = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("reordered").cast("long").alias("reordered_item_count"),
    )
    reject_rows(eligible.join(baskets, ["order_id"], "left_anti"),
                "orders: prior/train order missing items")
    return (orders.join(baskets, ["order_id"], "left")
            .withColumn("items_available", F.col("eval_set").isin("prior", "train"))
            .withColumn("has_reordered_item", F.col("reordered_item_count") > 0)
            .select(*ORDER_COLUMNS, "item_count", "reordered_item_count",
                    "has_reordered_item", "items_available"))
