"""Product catalog dimension with natural keys."""

from pyspark.sql import DataFrame

from src.gold.common import project, reject_rows, require_key


def build_dim_product(
    products: DataFrame, aisles: DataFrame, departments: DataFrame,
) -> DataFrame:
    """Preserve the full catalog; reject ambiguous keys and missing references."""
    products = project(products, ("product_id", "product_name", "aisle_id", "department_id"))
    aisles = project(aisles, ("aisle_id", "aisle"))
    departments = project(departments, ("department_id", "department"))
    for frame, key in ((products, "product_id"), (aisles, "aisle_id"),
                       (departments, "department_id")):
        require_key(frame, (key,), key)
    for reference, key in ((aisles, "aisle_id"), (departments, "department_id")):
        reject_rows(products.join(reference, [key], "left_anti"),
                    f"products: missing {key} reference")
    return (products.join(aisles, ["aisle_id"], "left")
            .join(departments, ["department_id"], "left")
            .select("product_id", "product_name", "aisle_id", "aisle",
                    "department_id", "department"))
