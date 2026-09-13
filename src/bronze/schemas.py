"""Explicit, nullable schemas for the six source CSVs."""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("eval_set", StringType()),
    StructField("order_number", IntegerType()),
    StructField("order_dow", IntegerType()),
    StructField("order_hour_of_day", IntegerType()),
    StructField("days_since_prior_order", DoubleType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("aisle_id", IntegerType()),
    StructField("department_id", IntegerType()),
])

AISLES_SCHEMA = StructType([
    StructField("aisle_id", IntegerType()),
    StructField("aisle", StringType()),
])

DEPARTMENTS_SCHEMA = StructType([
    StructField("department_id", IntegerType()),
    StructField("department", StringType()),
])

ORDER_PRODUCTS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("add_to_cart_order", IntegerType()),
    StructField("reordered", IntegerType()),
])
