"""Shared contracts and native Spark expressions for Gold."""

from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame, functions as F

ORDER_COLUMNS = (
    "order_id", "user_id", "eval_set", "order_number", "order_dow",
    "order_hour_of_day", "days_since_prior_order",
)
ITEM_COLUMNS = ("order_id", "product_id", "add_to_cart_order", "reordered")


def project(dataframe: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Reject ambiguous/missing columns and drop unrelated source metadata early."""
    if len(dataframe.columns) != len(set(dataframe.columns)):
        raise ValueError("Duplicate column names")
    missing = set(columns) - set(dataframe.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")
    return dataframe.select(*columns)


def reject_rows(invalid: DataFrame, message: str) -> None:
    """Execute a distributed existence check, without collecting records to Python."""
    if not invalid.isEmpty():
        raise ValueError(message)


def require_key(dataframe: DataFrame, keys: tuple[str, ...], label: str) -> None:
    """Fail explicitly on null or duplicate logical keys; never deduplicate."""
    invalid = dataframe.groupBy(*keys).count().filter(
        (F.col("count") > 1) | reduce(or_, (F.col(key).isNull() for key in keys))
    )
    reject_rows(invalid, f"{label}: null or duplicate key {keys}")


def prepare_orders(orders: DataFrame) -> DataFrame:
    orders = project(orders, ORDER_COLUMNS)
    require_key(orders, ("order_id",), "orders")
    reject_rows(
        orders.filter(F.col("user_id").isNull() | F.col("eval_set").isNull()
                      | ~F.col("eval_set").isin("prior", "train", "test")),
        "orders: invalid user_id or eval_set",
    )
    return orders


def safe_divide(numerator: Column, denominator: Column) -> Column:
    """Return a double ratio, or NULL when the denominator is zero/unknown."""
    return F.when(denominator > 0, numerator.cast("double") / denominator)
