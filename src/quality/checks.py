"""Dataset checks over caller-supplied DataFrames, without storage access."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import reduce

from pyspark.sql import DataFrame, functions as F

from src.quality.rules import Severity


class CheckType(str, Enum):
    ROW_RULE = "ROW_RULE"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
    UNIQUENESS = "UNIQUENESS"


@dataclass(frozen=True)
class ReferentialCheck:
    name: str
    dataset: str
    columns: tuple[str, ...]
    reference_dataset: str
    reference_columns: tuple[str, ...]
    reference_condition: str | None = None
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class UniquenessCheck:
    name: str
    dataset: str
    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


REFERENTIAL_CHECKS = (
    ReferentialCheck("aisle_exists", "products", ("aisle_id",), "aisles", ("aisle_id",)),
    ReferentialCheck("department_exists", "products", ("department_id",),
                     "departments", ("department_id",)),
    *tuple(check for dataset, eval_set in (
        ("order_products_prior", "prior"), ("order_products_train", "train")
    ) for check in (
        ReferentialCheck("product_exists", dataset, ("product_id",), "products", ("product_id",)),
        ReferentialCheck("order_exists", dataset, ("order_id",), "orders", ("order_id",)),
        ReferentialCheck("order_eval_set_matches", dataset, ("order_id",), "orders", ("order_id",),
                         f"eval_set = '{eval_set}'"),
    )),
)

UNIQUENESS_CHECKS = tuple(
    UniquenessCheck("primary_key_unique", dataset, columns)
    for dataset, columns in {
        "orders": ("order_id",), "products": ("product_id",),
        "aisles": ("aisle_id",), "departments": ("department_id",),
        "order_products_prior": ("order_id", "product_id"),
        "order_products_train": ("order_id", "product_id"),
    }.items()
)


def _require_columns(dataframe: DataFrame, columns: tuple[str, ...]) -> None:
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("Check keys must be nonempty and distinct")
    missing = set(columns) - set(dataframe.columns)
    if missing:
        raise ValueError(f"Missing check columns: {sorted(missing)}")
    if len(dataframe.columns) != len(set(dataframe.columns)):
        raise ValueError("Input contains duplicate columns")


def _result(
    metrics: DataFrame, dataset: str, name: str, check_type: CheckType, severity: Severity
) -> DataFrame:
    """Build a uniform single-row result; empty datasets pass with rate zero."""
    return metrics.select(
        F.lit(dataset).alias("dataset"), F.lit(name).alias("check_name"),
        F.lit(check_type.value).alias("check_type"),
        F.when(F.col("failed_rows") == 0, "PASS").otherwise("FAIL").alias("status"),
        "failed_rows", "total_rows",
        F.when(F.col("total_rows") > 0, F.col("failed_rows") / F.col("total_rows"))
        .otherwise(F.lit(0.0)).alias("failure_rate"),
        F.lit(Severity(severity).value).alias("severity"),
        F.lit(datetime.now(timezone.utc)).alias("checked_at"),
    )


def check_referential_integrity(
    dataframe: DataFrame, reference: DataFrame, check: ReferentialCheck
) -> DataFrame:
    """Count source rows without a matching reference key (NULL keys also fail)."""
    _require_columns(dataframe, check.columns)
    _require_columns(reference, check.reference_columns)
    if len(check.columns) != len(check.reference_columns):
        raise ValueError("Source and reference keys must have equal lengths")
    if check.reference_condition:
        reference = reference.filter(check.reference_condition)
    keys = reference.select(*[
        F.col(remote).alias(local)
        for local, remote in zip(check.columns, check.reference_columns)
    ])
    missing = dataframe.select(*check.columns).join(keys, list(check.columns), "left_anti")
    metrics = dataframe.agg(F.count("*").alias("total_rows")).crossJoin(
        missing.agg(F.count("*").alias("failed_rows"))
    )
    return _result(metrics, check.dataset, check.name, CheckType.REFERENTIAL_INTEGRITY, check.severity)


def check_uniqueness(dataframe: DataFrame, check: UniquenessCheck) -> DataFrame:
    """Count ALL rows in duplicate key groups, not just surplus occurrences."""
    _require_columns(dataframe, check.columns)
    groups = dataframe.select(*check.columns).groupBy(*check.columns).count()
    metrics = groups.agg(
        F.coalesce(F.sum("count"), F.lit(0).cast("long")).alias("total_rows"),
        F.coalesce(F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)),
                   F.lit(0).cast("long")).alias("failed_rows"),
    )
    return _result(metrics, check.dataset, check.name, CheckType.UNIQUENESS, check.severity)


def run_dataset_checks(datasets: Mapping[str, DataFrame]) -> DataFrame:
    """Return 14 result rows for the complete six-dataset mapping, lazily."""
    required = {c.dataset for c in UNIQUENESS_CHECKS}
    missing = required - datasets.keys()
    if missing:
        raise ValueError(f"Missing datasets for quality checks: {sorted(missing)}")
    results = [check_referential_integrity(
        datasets[c.dataset], datasets[c.reference_dataset], c
    ) for c in REFERENTIAL_CHECKS]
    results.extend(check_uniqueness(datasets[c.dataset], c) for c in UNIQUENESS_CHECKS)
    return reduce(lambda left, right: left.unionByName(right), results)
