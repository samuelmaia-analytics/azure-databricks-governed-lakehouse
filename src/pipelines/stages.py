"""Stage orchestration using existing transformations and publication contracts."""

from pyspark.sql import functions as F

from src.bronze.ingest import DATASETS, run_bronze_ingestion, write_delta
from src.gold.common import require_key
from src.gold.dim_product import build_dim_product
from src.gold.fact_order_items import build_fact_order_items
from src.gold.fact_orders import build_fact_orders
from src.gold.user_behavior import build_user_behavior
from src.quality.checks import run_dataset_checks
from src.quality.engine import apply_quality_rules, build_quality_summary, split_valid_invalid
from src.quality.publication_gate import PublicationStatus, evaluate_publication_gate
from src.silver.publish import publish_silver


class GateRejected(RuntimeError):
    def __init__(self, decisions):
        self.decisions = {name: {"status": gate.status.value, "reason": gate.reason}
                          for name, gate in decisions.items()}
        super().__init__("Silver publication refused by Publication Gate")


def read_layer(spark, config, layer):
    return {name: spark.read.format("delta").load(config.dataset(layer, name))
            for name in DATASETS}


def run_bronze(spark, config):
    run_bronze_ingestion(spark=spark, config=config)
    return {"status": "SUCCESS", "datasets_written": len(DATASETS)}


def run_silver(spark, config):
    bronze = read_layer(spark, config, "bronze")
    # Only the 14 small check summaries cross the driver boundary.
    checks = [row.asDict() for row in run_dataset_checks(bronze).collect()]
    checked = {name: apply_quality_rules(frame, name) for name, frame in bronze.items()}
    decisions = {
        name: evaluate_publication_gate(
            build_quality_summary(frame, name).first().asDict(),
            [check for check in checks if check["dataset"] == name],
        ) for name, frame in checked.items()
    }
    # Complete every gate before publishing any Silver dataset.
    if any(gate.status != PublicationStatus.APPROVED for gate in decisions.values()):
        raise GateRejected(decisions)
    counts = {}
    for name, frame in checked.items():
        valid, _ = split_valid_invalid(frame)
        result = publish_silver(valid, name, config.dataset("silver", name), decisions[name])
        if not result.written:
            raise RuntimeError(f"Silver publication did not write {name}")
        counts[name] = result.row_count
    return {"status": "SUCCESS", "rows": counts,
            "gates": {name: gate.status.value for name, gate in decisions.items()}}


def run_gold(spark, config):
    silver = read_layer(spark, config, "silver")
    dim = build_dim_product(silver["products"], silver["aisles"], silver["departments"])
    items = build_fact_order_items(silver["order_products_prior"],
                                   silver["order_products_train"], silver["orders"])
    orders = build_fact_orders(silver["orders"], items)
    users = build_user_behavior(orders, items)
    outputs = {
        "gold_dim_product": (dim, ("product_id",)),
        "gold_fact_order_items": (items, ("order_id", "product_id")),
        "gold_fact_orders": (orders, ("order_id",)),
        "gold_user_behavior": (users, ("user_id", "eval_set")),
    }
    counts = {}
    for name, (frame, key) in outputs.items():
        require_key(frame, key, name)
        counts[name] = frame.count()
        if not counts[name]:
            raise ValueError(f"Gold publication refuses empty output: {name}")
    expected_items = silver["order_products_prior"].count() + silver["order_products_train"].count()
    if (counts["gold_dim_product"] != silver["products"].count()
            or counts["gold_fact_order_items"] != expected_items
            or counts["gold_fact_orders"] != silver["orders"].count()):
        raise ValueError("Gold/source row reconciliation failed")
    if not items.join(dim.select("product_id"), ["product_id"], "left_anti").isEmpty():
        raise ValueError("Gold product coverage failed")
    item_totals = items.agg(F.sum("reordered")).first()
    order_totals = orders.agg(F.sum("item_count"), F.sum("reordered_item_count")).first()
    user_totals = users.agg(F.sum("orders_observed"), F.sum("total_items"),
                           F.sum("reordered_items")).first()
    if (order_totals[0] != expected_items or order_totals[1] != item_totals[0]
            or user_totals[0] != counts["gold_fact_orders"]
            or user_totals[1] != expected_items or user_totals[2] != item_totals[0]):
        raise ValueError("Gold cross-table reconciliation failed")
    # No publication until all outputs and reconciliations above have passed.
    for name, (frame, _) in outputs.items():
        write_delta(frame, config.dataset("gold", name))
    return {"status": "SUCCESS", "rows": counts}
