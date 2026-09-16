import pytest
from pyspark.sql import functions as F

from src.gold.dim_product import build_dim_product


def test_catalog_preserved_and_correctly_enriched(catalog):
    products, aisles, departments = catalog
    result = build_dim_product(products.withColumn("_source_file", F.lit("synthetic")),
                               aisles, departments)
    rows = {r.product_id: r for r in result.collect()}
    assert result.columns == ["product_id", "product_name", "aisle_id", "aisle",
                              "department_id", "department"]
    assert result.count() == products.count() == len(rows) == 4
    assert set(rows) == {10, 20, 30, 40}
    assert (rows[10].aisle, rows[10].department) == ("Fruit", "Fresh")
    assert (rows[40].aisle, rows[40].department) == ("Grains", "Pantry")


@pytest.mark.parametrize("index", [0, 1, 2])
def test_duplicate_keys_rejected(catalog, index):
    frames = list(catalog)
    frames[index] = frames[index].unionByName(frames[index].limit(1))
    with pytest.raises(ValueError, match="duplicate key"):
        build_dim_product(*frames)


@pytest.mark.parametrize("index", [1, 2])
def test_missing_reference_rejected(catalog, index):
    frames = list(catalog)
    frames[index] = frames[index].limit(0)
    with pytest.raises(ValueError, match="missing .* reference"):
        build_dim_product(*frames)


def test_null_key_rejected(catalog):
    products, aisles, departments = catalog
    with pytest.raises(ValueError, match="null or duplicate key"):
        build_dim_product(products.withColumn("product_id", F.lit(None).cast("int")),
                          aisles, departments)


def test_empty_catalog(catalog):
    products, aisles, departments = catalog
    assert build_dim_product(products.limit(0), aisles, departments).isEmpty()
