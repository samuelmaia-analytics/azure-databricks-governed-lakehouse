"""User metrics from the validated Gold facts of the same source snapshot."""

from pyspark.sql import DataFrame, functions as F

from src.gold.common import project, safe_divide


def build_user_behavior(fact_orders: DataFrame, fact_order_items: DataFrame) -> DataFrame:
    """Aggregate canonical Gold facts separately to avoid order fanout.

    Inputs must be outputs of build_fact_orders/build_fact_order_items from the
    same source snapshot. No independent, potentially inconsistent facts should
    be mixed. Snapshot orchestration belongs to the future publication layer.
    """
    keys = ["user_id", "eval_set"]
    orders = project(fact_orders, (
        "user_id", "eval_set", "items_available", "item_count",
        "reordered_item_count", "days_since_prior_order",
    ))
    items = project(fact_order_items, ("user_id", "eval_set", "product_id"))
    measures = orders.groupBy(*keys).agg(
        F.count("*").alias("orders_observed"),
        F.sum(F.col("items_available").cast("long")).alias("orders_with_items"),
        F.sum("item_count").alias("total_items"),
        F.sum("reordered_item_count").alias("reordered_items"),
        F.sum("days_since_prior_order").alias("days_since_prior_sum"),
        F.count("days_since_prior_order").alias("days_since_prior_count"),
    )
    # Unlike basket metrics, zero eligible orders is a known coverage count.
    products = items.groupBy(*keys).agg(F.countDistinct("product_id").alias("distinct_products"))
    return (measures.join(products, keys, "left")
            .withColumn("reorder_rate", safe_divide(F.col("reordered_items"), F.col("total_items")))
            .withColumn("avg_items_per_eligible_order",
                        safe_divide(F.col("total_items"), F.col("orders_with_items")))
            .withColumn("avg_days_since_prior_order",
                        safe_divide(F.col("days_since_prior_sum"), F.col("days_since_prior_count"))))
