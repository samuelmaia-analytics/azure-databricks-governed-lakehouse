"""Small in-memory fixtures; never read lakehouse paths."""

import pytest

from src.gold.fact_order_items import build_fact_order_items
from src.gold.fact_orders import build_fact_orders
from src.gold.user_behavior import build_user_behavior


@pytest.fixture(scope="session")
def gold_sources(spark):
    orders = spark.sql("""
        SELECT * FROM VALUES
        (1, 1, 'prior', 1, 0, 8, CAST(NULL AS DOUBLE)),
        (2, 1, 'prior', 2, 1, 9, 4D),
        (3, 2, 'prior', 1, 2, 10, NULL),
        (4, 1, 'train', 3, 3, 11, 8D),
        (5, 2, 'test', 2, 4, 12, 12D),
        (6, 3, 'test', 1, 5, 13, NULL)
        AS t(order_id, user_id, eval_set, order_number, order_dow,
             order_hour_of_day, days_since_prior_order)
    """)
    prior = spark.sql("""
        SELECT * FROM VALUES (1, 10, 1, 0), (1, 20, 2, 0), (1, 30, 3, 0),
            (2, 10, 1, 1), (3, 20, 1, 1)
        AS t(order_id, product_id, add_to_cart_order, reordered)
    """)
    train = spark.sql("""
        SELECT * FROM VALUES (4, 10, 1, 1), (4, 30, 2, 0)
        AS t(order_id, product_id, add_to_cart_order, reordered)
    """)
    return orders, prior, train


@pytest.fixture(scope="session")
def gold_facts(gold_sources):
    orders, prior, train = gold_sources
    items = build_fact_order_items(prior, train, orders).cache()
    baskets = build_fact_orders(orders, items).cache()
    users = build_user_behavior(baskets, items).cache()
    try:
        yield items, baskets, users
    finally:
        users.unpersist()
        baskets.unpersist()
        items.unpersist()


@pytest.fixture
def catalog(spark):
    products = spark.sql("""
        SELECT * FROM VALUES (10, 'Apple', 1, 8), (20, 'Pear', 1, 8),
            (30, 'Rice', 2, 9), (40, 'No observed purchases', 2, 9)
        AS t(product_id, product_name, aisle_id, department_id)
    """)
    aisles = spark.sql("SELECT * FROM VALUES (1, 'Fruit'), (2, 'Grains') AS t(aisle_id, aisle)")
    departments = spark.sql("""
        SELECT * FROM VALUES (8, 'Fresh'), (9, 'Pantry') AS t(department_id, department)
    """)
    return products, aisles, departments
