"""Schema contracts and reader configuration without starting Spark."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StructType

from src.bronze.ingest import read_csv
from src.bronze.schemas import (
    AISLES_SCHEMA,
    DEPARTMENTS_SCHEMA,
    ORDER_PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
)


@pytest.mark.parametrize("schema,expected", [
    (ORDERS_SCHEMA, [
        ("order_id", "int"), ("user_id", "int"), ("eval_set", "string"),
        ("order_number", "int"), ("order_dow", "int"), ("order_hour_of_day", "int"),
        ("days_since_prior_order", "double"),
    ]),
    (PRODUCTS_SCHEMA, [
        ("product_id", "int"), ("product_name", "string"),
        ("aisle_id", "int"), ("department_id", "int"),
    ]),
    (AISLES_SCHEMA, [("aisle_id", "int"), ("aisle", "string")]),
    (DEPARTMENTS_SCHEMA, [("department_id", "int"), ("department", "string")]),
    (ORDER_PRODUCTS_SCHEMA, [
        ("order_id", "int"), ("product_id", "int"),
        ("add_to_cart_order", "int"), ("reordered", "int"),
    ]),
])
def test_explicit_schema_contract(schema, expected):
    assert isinstance(schema, StructType)
    assert [(field.name, field.dataType.simpleString()) for field in schema] == expected


def test_reader_disables_inference(tmp_path):
    source = tmp_path / "aisles.csv"
    source.write_text("aisle_id,aisle\n1,fruit\n", encoding="utf-8")
    spark = MagicMock()
    reader = spark.read
    reader.schema.return_value = reader
    reader.option.return_value = reader

    read_csv(spark, source, AISLES_SCHEMA)

    reader.schema.assert_called_once_with(AISLES_SCHEMA)
    reader.option.assert_any_call("inferSchema", False)
    reader.option.assert_any_call("header", True)
    reader.csv.assert_called_once_with(source.resolve().as_posix())
