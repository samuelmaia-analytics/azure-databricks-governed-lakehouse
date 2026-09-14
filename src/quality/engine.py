"""Apply row predicates and return DataFrames; never read or write storage."""

from collections.abc import Sequence
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

from src.quality.rules import QualityRule, Severity, get_rules

DQ_COLUMNS = ("_dq_failed_rules", "_dq_error_count", "_dq_warning_count", "_dq_checked_at")


def apply_quality_rules(
    dataframe: DataFrame, dataset: str, rules: Sequence[QualityRule] | None = None
) -> DataFrame:
    """Annotate all rows. NULL predicates fail; warning-only rows remain valid.

    Custom rules replace the registry selection. A timestamp is fixed when this
    function is called so separate Spark actions share the same check instant.
    """
    selected = tuple(get_rules(dataset) if rules is None else rules)
    if not selected:
        raise ValueError(f"No quality rules configured for dataset: {dataset}")
    if any(rule.dataset != dataset for rule in selected):
        raise ValueError("All rules must belong to the requested dataset")
    if len({rule.name for rule in selected}) != len(selected):
        raise ValueError("Rule names must be unique within a dataset")
    if len(dataframe.columns) != len(set(dataframe.columns)):
        raise ValueError("Input contains duplicate columns")
    if set(DQ_COLUMNS).intersection(dataframe.columns):
        raise ValueError("Input already contains reserved quality columns")

    failures = [(rule, ~F.coalesce(F.expr(rule.condition), F.lit(False))) for rule in selected]
    names = F.array(*[F.when(failed, F.lit(rule.name)) for rule, failed in failures])

    def count_failures(severity: Severity):
        return sum((F.when(failed, 1).otherwise(0) for rule, failed in failures
                    if rule.severity == severity), F.lit(0))

    return dataframe.select(
        "*",
        F.filter(names, lambda name: name.isNotNull()).alias("_dq_failed_rules"),
        count_failures(Severity.ERROR).alias("_dq_error_count"),
        count_failures(Severity.WARNING).alias("_dq_warning_count"),
        F.lit(datetime.now(timezone.utc)).alias("_dq_checked_at"),
    )


def split_valid_invalid(checked_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split annotated rows by ERROR count, retaining all original and DQ columns."""
    return (checked_df.filter(F.col("_dq_error_count") == 0),
            checked_df.filter(F.col("_dq_error_count") > 0))


def build_quality_summary(checked_df: DataFrame, dataset: str) -> DataFrame:
    """Return one summary row, including zero counts/rate for empty input.

    warning_rows includes rows with warnings whether valid or invalid.
    error_rate is invalid_rows / total_rows, not the number of failed rules.
    """
    summary = checked_df.agg(
        F.count("*").alias("total_rows"),
        F.count(F.when(F.col("_dq_error_count") == 0, 1)).alias("valid_rows"),
        F.count(F.when(F.col("_dq_error_count") > 0, 1)).alias("invalid_rows"),
        F.count(F.when(F.col("_dq_warning_count") > 0, 1)).alias("warning_rows"),
        F.coalesce(F.max("_dq_checked_at"), F.lit(datetime.now(timezone.utc))).alias("checked_at"),
    )
    return summary.select(
        F.lit(dataset).alias("dataset"), "total_rows", "valid_rows", "invalid_rows",
        F.when(F.col("total_rows") > 0, F.col("invalid_rows") / F.col("total_rows"))
        .otherwise(F.lit(0.0)).alias("error_rate"), "warning_rows", "checked_at",
    )
