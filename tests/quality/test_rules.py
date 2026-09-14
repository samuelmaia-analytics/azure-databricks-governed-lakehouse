import pytest

from src.quality.engine import apply_quality_rules
from src.quality.rules import RULES, Severity, get_rules


VALID_SQL = {
    "orders": "SELECT 1 order_id, 1 user_id, 'prior' eval_set, 1 order_number, "
              "0 order_dow, 23 order_hour_of_day, cast(NULL as double) days_since_prior_order",
    "products": "SELECT 1 product_id, 'Apple' product_name, 1 aisle_id, 1 department_id",
    "aisles": "SELECT 1 aisle_id, 'Fruit' aisle",
    "departments": "SELECT 1 department_id, 'Produce' department",
    "order_products_prior": "SELECT 1 order_id, 1 product_id, 1 add_to_cart_order, 0 reordered",
    "order_products_train": "SELECT 1 order_id, 1 product_id, 1 add_to_cart_order, 1 reordered",
}


def test_registry_contract():
    assert {name: len(rules) for name, rules in RULES.items()} == {
        "orders": 9, "products": 6, "aisles": 4, "departments": 4,
        "order_products_prior": 6, "order_products_train": 6,
    }
    for dataset, rules in RULES.items():
        assert len({rule.name for rule in rules}) == len(rules)
        assert all(rule.dataset == dataset and rule.description and rule.condition
                   and rule.severity == Severity.ERROR for rule in rules)


@pytest.mark.parametrize("dataset", list(VALID_SQL))
def test_valid_dataset(spark, dataset):
    row = apply_quality_rules(spark.sql(VALID_SQL[dataset]), dataset).first()
    assert row._dq_failed_rules == []
    assert row._dq_error_count == 0
    assert row._dq_warning_count == 0


@pytest.mark.parametrize("dataset,column,value,failed", [
    ("orders", "order_dow", "7", ["order_dow_range"]),
    ("orders", "order_hour_of_day", "24", ["order_hour_range"]),
    ("orders", "order_number", "0", ["order_number_min"]),
    ("orders", "eval_set", "'other'", ["eval_set_allowed"]),
    ("orders", "days_since_prior_order", "-1.0", ["days_since_prior_order_nonnegative"]),
    ("orders", "order_id", "cast(NULL as int)", ["order_id_not_null", "order_id_positive"]),
    ("products", "product_name", "''", ["product_name_not_blank"]),
    ("products", "product_name", "'   '", ["product_name_not_blank"]),
    ("aisles", "aisle", "' '", ["aisle_not_blank"]),
    ("departments", "department_id", "0", ["department_id_positive"]),
    ("order_products_prior", "reordered", "2", ["reordered_allowed"]),
    ("order_products_prior", "add_to_cart_order", "0", ["add_to_cart_order_min"]),
    ("order_products_train", "reordered", "2", ["reordered_allowed"]),
    ("order_products_train", "add_to_cart_order", "0", ["add_to_cart_order_min"]),
])
def test_invalid_field(spark, dataset, column, value, failed):
    from pyspark.sql import functions as F

    dataframe = spark.sql(VALID_SQL[dataset]).withColumn(column, F.expr(value))
    row = apply_quality_rules(dataframe, dataset).first()
    assert row._dq_failed_rules == failed
    assert row._dq_error_count == len(failed)


def test_unknown_dataset():
    with pytest.raises(ValueError, match="No quality rules configured.*unknown"):
        get_rules("unknown")
