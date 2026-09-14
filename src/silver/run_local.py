"""Explicit local six-table publication: python -m src.silver.run_local.

Writes only Silver and an audit report; never persists Quarantine, Bronze or Raw.
Run after synthetic tests pass, with HADOOP_HOME and the JVM heap configured.
"""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.spark import get_spark_session, to_spark_path
from src.quality.checks import (
    REFERENTIAL_CHECKS,
    UNIQUENESS_CHECKS,
    check_referential_integrity,
    check_uniqueness,
)
from src.quality.engine import apply_quality_rules, build_quality_summary, split_valid_invalid
from src.quality.publication_gate import PublicationStatus, evaluate_publication_gate
from src.silver.publish import SILVER_COLUMNS, publish_silver

ROOT = Path(__file__).resolve().parents[2]
DATASETS = (
    "orders", "products", "aisles", "departments",
    "order_products_prior", "order_products_train",
)


def fingerprint(directory):
    """Hash every file, including Delta logs, to verify source byte preservation."""
    result = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            with path.open("rb") as stream:
                result[path.relative_to(directory).as_posix()] = {
                    "bytes": path.stat().st_size,
                    "sha256": hashlib.file_digest(stream, "sha256").hexdigest(),
                }
    return result


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    before = {name: fingerprint(ROOT / "data" / name) for name in ("raw", "bronze")}
    quarantine = fingerprint(ROOT / "data/quarantine")
    require(set(quarantine) <= {".gitkeep"}, "Quarantine contains real files")
    report = {"started_at": datetime.now(timezone.utc), "datasets": [], "sources_before": before}
    report_path = ROOT / "docs/silver_publication_audit.json"
    spark = get_spark_session("Phase 2 approved Silver publication")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.sparkContext.setLogLevel("WARN")
    heap = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
    require(heap >= 8 * 1024**3, f"Expected 8 GiB JVM heap, got {heap}")
    report["jvm_max_heap_bytes"] = heap
    report["shuffle_partitions"] = spark.conf.get("spark.sql.shuffle.partitions")
    bronze = {name: spark.read.format("delta").load(to_spark_path(ROOT / "data/bronze" / name))
              for name in DATASETS}
    try:
        decisions = {}
        # Complete all six gates before writing the first Silver table.
        for name, frame in bronze.items():
            checked = apply_quality_rules(frame, name)
            summary = build_quality_summary(checked, name).first().asDict()
            checks = []
            for check in REFERENTIAL_CHECKS:
                if check.dataset == name:
                    checks.append(check_referential_integrity(
                        frame, bronze[check.reference_dataset], check,
                    ).first().asDict())
            for check in UNIQUENESS_CHECKS:
                if check.dataset == name:
                    checks.append(check_uniqueness(frame, check).first().asDict())
            gate = evaluate_publication_gate(summary, checks)
            require(gate.status == PublicationStatus.APPROVED, f"Gate refused {name}: {gate}")
            require(summary["invalid_rows"] == summary["warning_rows"] == 0, name)
            decisions[name] = (checked, gate)
            report["datasets"].append({"dataset": name, "summary": summary,
                                       "checks": checks, "gate": asdict(gate)})
            print(f"GATE {name}: {gate.status.value}, rows={summary['total_rows']}", flush=True)
        for entry in report["datasets"]:
            name = entry["dataset"]
            checked, gate = decisions[name]
            valid_df, _ = split_valid_invalid(checked)
            valid_df = valid_df.persist(StorageLevel.DISK_ONLY)
            try:
                destination = ROOT / "data/silver" / name
                result = publish_silver(valid_df, name, destination, gate)
                require(result.written, f"No Silver written for {name}")
                silver = spark.read.format("delta").load(to_spark_path(destination))
                count = silver.count()
                require(count == gate.total_rows == result.row_count, f"Row mismatch: {name}")
                require(silver.columns == valid_df.columns + list(SILVER_COLUMNS), name)
                require([(f.name, f.dataType) for f in silver.schema][:len(valid_df.columns)]
                        == [(f.name, f.dataType) for f in valid_df.schema], name)
                # Exact multiset equality checks every business and Bronze/DQ value,
                # including duplicates and nulls, in both directions.
                restored = silver.select(valid_df.columns)
                require(not valid_df.exceptAll(restored).take(1), f"Missing/changed rows: {name}")
                require(not restored.exceptAll(valid_df).take(1), f"Unexpected rows: {name}")
                bad_metadata = silver.filter(
                    F.col("_published_at").isNull()
                    | ~F.col("_publication_status").eqNullSafe(F.lit("APPROVED"))
                    | ~F.col("_publication_reason").eqNullSafe(F.lit(gate.reason))
                ).limit(1).count()
                require(bad_metadata == 0, f"Invalid publication metadata: {name}")
                require((destination / "_delta_log").is_dir(), f"No Delta log: {name}")
                entry["validation"] = {
                    "bronze_rows": gate.total_rows, "silver_rows": count,
                    "bronze_columns": bronze[name].columns, "silver_columns": silver.columns,
                    "bronze_column_count": len(bronze[name].columns),
                    "silver_column_count": len(silver.columns), "delta_log": True,
                    "delta_read": True, "exact_input_multiset_preserved": True,
                    "publication_metadata_valid": True, "status": "OK",
                }
                print(f"VALIDATED {name}: Bronze={gate.total_rows}, Silver={count}", flush=True)
                report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
            finally:
                valid_df.unpersist()
        after = {name: fingerprint(ROOT / "data" / name) for name in ("raw", "bronze")}
        require(before == after, "Raw or Bronze files changed")
        require(quarantine == fingerprint(ROOT / "data/quarantine"), "Quarantine changed")
        report.update({"sources_unchanged": True, "quarantine_unchanged": True,
                       "quarantined_rows": 0, "lost_rows": 0, "completed": True,
                       "finished_at": datetime.now(timezone.utc)})
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print("COMPLETE: 6/6 verified; Raw/Bronze hashes unchanged; Quarantine untouched", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
