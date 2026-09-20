"""Validate all four Gold outputs before any write: python -m src.gold.run_local.

Explicit local execution only. Transformations remain in their own modules.
Failures before the write barrier abort publication; writes are per-table Delta
transactions, not an atomic multi-table transaction.
"""

import hashlib
import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.spark import get_spark_session, to_spark_path
from src.common.runtime import require_local_audit
from src.gold.common import ITEM_COLUMNS, ORDER_COLUMNS, require_key
from src.gold.dim_product import build_dim_product
from src.gold.fact_order_items import build_fact_order_items
from src.gold.fact_orders import build_fact_orders
from src.gold.user_behavior import build_user_behavior

ROOT = Path(__file__).resolve().parents[2]
BASELINE = {
    "orders": 3421083, "products": 49688, "aisles": 134, "departments": 21,
    "order_products_prior": 32434489, "order_products_train": 1384617,
}
KEYS = {
    "gold_dim_product": ("product_id",),
    "gold_fact_order_items": ("order_id", "product_id"),
    "gold_fact_orders": ("order_id",),
    "gold_user_behavior": ("user_id", "eval_set"),
}
ITEM_TOTAL = BASELINE["order_products_prior"] + BASELINE["order_products_train"]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def progress(message):
    print(message, flush=True)


def inventory(directory):
    """Detect additions/removals/changes without hashing large data files.

    Compare size, modification and creation times of every file; additionally hash
    Delta transaction-log files. Reads may change access time, which is excluded.
    This is a preservation check for this controlled execution, not a forensic
    guarantee against an external actor restoring timestamps.
    """
    result = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            stat = path.stat()
            entry = [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]
            if "_delta_log" in path.parts:
                entry.append(hashlib.sha256(path.read_bytes()).hexdigest())
            result[path.relative_to(directory).as_posix()] = entry
    return result


def valid(frame, predicate, label):
    """NULL predicates are failures, rather than disappearing in SQL filters."""
    require(frame.filter(~F.coalesce(F.expr(predicate), F.lit(False))).isEmpty(), label)


def same(left, right, label):
    require(left.exceptAll(right).isEmpty(), f"Missing/changed rows: {label}")
    require(right.exceptAll(left).isEmpty(), f"Extra/changed rows: {label}")


def validate(gold, silver, stage):
    counts = {}
    for name, frame in gold.items():
        counts[name] = frame.count()
        require_key(frame, KEYS[name], name)
        progress(f"{stage} KEY {name}: {counts[name]}")
    dim = gold["gold_dim_product"]
    items = gold["gold_fact_order_items"]
    orders = gold["gold_fact_orders"]
    users = gold["gold_user_behavior"]
    require(counts["gold_dim_product"] == BASELINE["products"], "Catalog count")
    require(counts["gold_fact_order_items"] == ITEM_TOTAL, "Item count")
    require(counts["gold_fact_orders"] == BASELINE["orders"], "Order count")

    product_columns = ["product_id", "product_name", "aisle_id", "department_id"]
    same(dim.select(product_columns), silver["products"].select(product_columns), "catalog")
    valid(dim, "aisle IS NOT NULL AND department IS NOT NULL", "Missing catalog labels")
    for source, key, label in (("aisles", "aisle_id", "aisle"),
                                ("departments", "department_id", "department")):
        reference = silver[source].select(key, F.col(label).alias("expected_label"))
        valid(dim.join(reference, [key], "left"), f"{label} <=> expected_label",
              f"Wrong {label} association")
    progress(f"{stage} DIM associations PASS")

    valid(items, "item_count = 1 AND reordered IN (0, 1) AND add_to_cart_order >= 1 "
          "AND eval_set IN ('prior', 'train')", "Invalid item domains")
    # Independent comparison with each source also proves origin and exact preservation.
    for subset in ("prior", "train"):
        same(items.filter(F.col("eval_set") == subset).select(*ITEM_COLUMNS),
             silver[f"order_products_{subset}"].select(*ITEM_COLUMNS), f"{subset} items")
    context = silver["orders"].select(
        "order_id", *[F.col(c).alias(f"expected_{c}") for c in ORDER_COLUMNS if c != "order_id"]
    )
    joined = items.join(context, ["order_id"], "left")
    valid(joined, " AND ".join(f"({c} <=> expected_{c})" for c in ORDER_COLUMNS
                               if c != "order_id"), "Item order context mismatch")
    require(items.join(dim.select("product_id"), ["product_id"], "left_anti").isEmpty(),
            "Product coverage")
    require(items.join(orders.select("order_id"), ["order_id"], "left_anti").isEmpty(),
            "Order coverage")
    progress(f"{stage} ITEMS preservation and references PASS")

    same(orders.select(*ORDER_COLUMNS), silver["orders"].select(*ORDER_COLUMNS), "orders")
    eligible = orders.filter("eval_set IN ('prior', 'train')")
    test = orders.filter("eval_set = 'test'")
    valid(eligible, "items_available = true AND item_count >= 1 "
          "AND reordered_item_count >= 0 AND reordered_item_count <= item_count "
          "AND has_reordered_item = (reordered_item_count > 0)", "Invalid eligible basket")
    valid(test, "items_available = false AND item_count IS NULL "
          "AND reordered_item_count IS NULL AND has_reordered_item IS NULL", "Test basket not NULL")
    # Check every basket, not only the global sum.
    independent_baskets = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"), F.sum("reordered").alias("reordered_item_count"),
    )
    same(eligible.select("order_id", "item_count", "reordered_item_count"),
         independent_baskets, "per-order basket reconciliation")
    coverage = {r.eval_set: r["count"] for r in orders.groupBy("eval_set").count().collect()}
    progress(f"{stage} ORDERS coverage {coverage}")

    valid(users, "orders_observed > 0 AND orders_with_items >= 0 "
          "AND orders_with_items <= orders_observed AND days_since_prior_count >= 0 "
          "AND days_since_prior_count <= orders_observed", "Invalid user counts")
    valid(users.filter("eval_set IN ('prior', 'train')"),
          "orders_with_items = orders_observed AND total_items >= reordered_items "
          "AND reordered_items >= 0 AND total_items > 0 AND distinct_products > 0 "
          "AND distinct_products <= total_items AND reorder_rate BETWEEN 0 AND 1 "
          "AND avg_items_per_eligible_order > 0", "Invalid user item measures")
    valid(users.filter("eval_set = 'test'"), "orders_with_items = 0 AND total_items IS NULL "
          "AND reordered_items IS NULL AND distinct_products IS NULL AND reorder_rate IS NULL "
          "AND avg_items_per_eligible_order IS NULL", "Test user basket not NULL")
    for numerator, denominator, metric in (
        ("reordered_items", "total_items", "reorder_rate"),
        ("total_items", "orders_with_items", "avg_items_per_eligible_order"),
        ("days_since_prior_sum", "days_since_prior_count", "avg_days_since_prior_order"),
    ):
        valid(users, f"{metric} <=> CASE WHEN {denominator} > 0 "
              f"THEN CAST({numerator} AS DOUBLE) / {denominator} END", f"Invalid {metric}")
    independent_users = orders.groupBy("user_id", "eval_set").agg(
        F.count("*").alias("orders_observed"),
        F.sum(F.col("items_available").cast("long")).alias("orders_with_items"),
        F.sum("item_count").alias("total_items"),
        F.sum("reordered_item_count").alias("reordered_items"),
        F.sum("days_since_prior_order").alias("days_since_prior_sum"),
        F.count("days_since_prior_order").alias("days_since_prior_count"),
    )
    same(users.select(independent_users.columns), independent_users, "user base measures")
    distinct = items.select("user_id", "eval_set", "product_id").distinct().groupBy(
        "user_id", "eval_set"
    ).count().withColumnRenamed("count", "distinct_products")
    same(users.filter("eval_set IN ('prior', 'train')").select(distinct.columns),
         distinct, "user distinct products")
    item_reordered = items.agg(F.sum("reordered")).first()[0]
    basket_totals = eligible.agg(F.sum("item_count"), F.sum("reordered_item_count")).first()
    user_totals = users.agg(F.sum("orders_observed"), F.sum("total_items"),
                           F.sum("reordered_items"), F.sum("orders_with_items")).first()
    require(basket_totals[0] == user_totals[1] == ITEM_TOTAL, "Item totals reconciliation")
    require(basket_totals[1] == user_totals[2] == item_reordered, "Reorder totals reconciliation")
    require(user_totals[0] == BASELINE["orders"], "User order totals reconciliation")
    require(user_totals[3] == coverage["prior"] + coverage["train"], "Eligible count")
    progress(f"{stage} ALL VALIDATIONS PASS")
    return {
        "rows": counts, "coverage": coverage, "reordered_items": item_reordered,
        "eligible_orders": user_totals[3], "user_orders": user_totals[0],
        "item_sum": basket_totals[0], "reordered_sum": basket_totals[1],
    }


def main():
    require_local_audit()
    layers = ("raw", "bronze", "silver", "quarantine")
    before = {name: inventory(ROOT / "data" / name) for name in layers}
    spark = get_spark_session("Gold real validation and publication")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.sparkContext.setLogLevel("WARN")
    require(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory() >= 8 * 1024**3,
            "Expected at least 8 GiB driver heap")
    cached = []
    try:
        silver = {}
        for name, expected in BASELINE.items():
            path = ROOT / "data/silver" / name
            require(path.is_dir() and (path / "_delta_log").is_dir(), f"Missing Delta: {name}")
            location = to_spark_path(path)
            version = DeltaTable.forPath(spark, location).history(1).select("version").first()[0]
            frame = spark.read.format("delta").option("versionAsOf", version).load(location)
            measured = frame.count()
            require(measured == expected, f"Baseline mismatch {name}: {measured} != {expected}")
            silver[name] = frame
            progress(f"BASELINE {name}: {measured} PASS")

        def retain(name, frame):
            # Materialized Spark intermediate only; not a lakehouse publication.
            cached.append(frame.persist(StorageLevel.DISK_ONLY))
            progress(f"BUILD {name}: {frame.count()}")
            return frame

        gold = {}
        gold["gold_dim_product"] = retain("dim", build_dim_product(
            silver["products"], silver["aisles"], silver["departments"]))
        gold["gold_fact_order_items"] = retain("items", build_fact_order_items(
            silver["order_products_prior"], silver["order_products_train"], silver["orders"]))
        gold["gold_fact_orders"] = retain("orders", build_fact_orders(
            silver["orders"], gold["gold_fact_order_items"]))
        gold["gold_user_behavior"] = retain("users", build_user_behavior(
            gold["gold_fact_orders"], gold["gold_fact_order_items"]))
        pre = validate(gold, silver, "PREWRITE")
        require(before == {name: inventory(ROOT / "data" / name) for name in layers},
                "Source layer changed before publication")

        items = gold["gold_fact_order_items"]
        dim = gold["gold_dim_product"]
        descriptive = {
            "orders": BASELINE["orders"], "items": ITEM_TOTAL,
            "users": silver["orders"].select("user_id").distinct().count(),
            "products": BASELINE["products"], "reordered_items": pre["reordered_items"],
            "global_reorder_rate": pre["reordered_items"] / ITEM_TOTAL,
            "avg_items_per_eligible_order": ITEM_TOTAL / pre["eligible_orders"],
            "orders_by_eval_set": pre["coverage"],
        }
        top_products = (items.groupBy("product_id").agg(F.countDistinct("order_id").alias("orders"))
                        .join(dim.select("product_id", "product_name"), ["product_id"])
                        .orderBy(F.desc("orders"), "product_id").limit(10))
        top_departments = (items.join(dim.select("product_id", "department_id", "department"),
                                      ["product_id"])
                           .groupBy("department_id", "department").count()
                           .orderBy(F.desc("count"), "department_id").limit(10))
        progress("DESCRIPTIVE " + json.dumps(descriptive, ensure_ascii=False))
        progress("TOP_PRODUCTS " + json.dumps([r.asDict() for r in top_products.collect()]))
        progress("TOP_DEPARTMENTS " + json.dumps([r.asDict() for r in top_departments.collect()]))

        # Publication barrier: every pre-write check above must complete successfully.
        restored = {}
        for name, frame in gold.items():
            destination = ROOT / "data/gold" / name
            frame.write.format("delta").mode("overwrite").save(to_spark_path(destination))
            require(destination.is_dir() and (destination / "_delta_log").is_dir(), name)
            readback = spark.read.format("delta").load(to_spark_path(destination))
            # Delta makes fields nullable on read. Names, order, types and metadata must match.
            expected_schema = [(f.name, f.dataType, f.metadata) for f in frame.schema]
            require([(f.name, f.dataType, f.metadata) for f in readback.schema] == expected_schema,
                    f"Persisted schema mismatch: {name}")
            require(readback.count() == pre["rows"][name], f"Persisted count mismatch: {name}")
            same(frame, readback.select(frame.columns), f"persisted {name}")
            restored[name] = readback
            progress(f"WRITE AND EXACT READBACK {name}: PASS")
        post = validate(restored, silver, "POSTWRITE")
        require(pre == post, "Pre/post validation results differ")
        require(before == {name: inventory(ROOT / "data" / name) for name in layers},
                "Source layer changed during publication")
        report = {
            "validation_status": "PASS",
            "gold": {name: {"rows": count, "unique_key": True, "delta_log": True,
                            "readback_schema": "PASS", "exact_readback": "PASS"}
                     for name, count in post["rows"].items()},
            "reconciliations": {
                "expected_order_items": ITEM_TOTAL, "order_items": post["item_sum"],
                "orders": BASELINE["orders"], "user_orders_observed": post["user_orders"],
                "items_reordered": post["reordered_items"],
                "orders_reordered": post["reordered_sum"],
                "product_and_order_coverage": "PASS", "test_baskets_null": "PASS",
                "safe_divisions": "PASS", "lost_records": 0, "multiplied_records": 0,
                "previous_layers_preserved": True,
            },
        }
        (ROOT / "docs/gold_publication_audit.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        progress("COMPLETE " + json.dumps(report))
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
