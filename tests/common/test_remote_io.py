from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.bronze.ingest import read_csv, run_bronze_ingestion
from src.bronze.schemas import ORDERS_SCHEMA
from src.common.runtime import RuntimeConfig
from src.quality.engine import DQ_COLUMNS
from src.quality.publication_gate import PublicationStatus
from src.quality.quarantine import persist_quarantine
from src.silver.publish import publish_silver


@pytest.mark.parametrize("source", ["dbfs:/raw/orders.csv", "abfss://c@a/raw/orders.csv",
                                    "/Volumes/c/s/v/orders.csv"])
def test_csv_remote_path_passed_unchanged(source):
    session = MagicMock()
    reader = session.read.schema.return_value
    reader.option.return_value = reader
    read_csv(session, source, ORDERS_SCHEMA)
    reader.csv.assert_called_once_with(source)


def test_remote_ingestion_preflight_failure_prevents_write(monkeypatch):
    config = RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS", "DATABRICKS_BASE_PATH": "dbfs:/lake"})
    session = MagicMock()
    reader = MagicMock()
    reader.return_value.limit.return_value.count.side_effect = OSError("missing remote source")
    ingestion = MagicMock()
    monkeypatch.setattr("src.bronze.ingest.read_csv", reader)
    monkeypatch.setattr("src.bronze.ingest.ingest_dataset", ingestion)
    with pytest.raises(OSError, match="missing remote"):
        run_bronze_ingestion(spark=session, config=config)
    ingestion.assert_not_called()
    session.stop.assert_not_called()


def test_databricks_bronze_does_not_stop_acquired_platform_session(monkeypatch):
    config = RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS", "DATABRICKS_BASE_PATH": "dbfs:/lake"})
    session, ingestion = MagicMock(), MagicMock()
    monkeypatch.setattr("src.bronze.ingest.get_spark_session", MagicMock(return_value=session))
    monkeypatch.setattr("src.bronze.ingest.read_csv", MagicMock())
    monkeypatch.setattr("src.bronze.ingest.ingest_dataset", ingestion)
    run_bronze_ingestion(config=config)
    assert ingestion.call_count == 6
    assert ingestion.call_args.args[2:4] == ("dbfs:/lake/raw", "dbfs:/lake/bronze")
    session.stop.assert_not_called()


@pytest.mark.parametrize("kind", ["silver", "quarantine"])
def test_remote_publication_destination_preserved(spark, kind):
    # Real Spark expressions, mocked writer: no remote I/O occurs.
    frame = MagicMock()
    frame.columns = ["id", "_ingested_at", "_source_file", "_batch_id", *DQ_COLUMNS]
    frame.count.return_value = 1
    destination = f"abfss://container@account/{kind}/orders"
    if kind == "silver":
        gate = SimpleNamespace(dataset="orders", status=PublicationStatus.APPROVED, reason="approved")
        result = publish_silver(frame, "orders", destination, gate)
        writer = frame.select.return_value.write
    else:
        result = persist_quarantine(frame, "orders", destination, "synthetic")
        writer = frame.select.return_value.write
    assert result.destination_path == destination
    writer.format.return_value.mode.return_value.save.assert_called_once_with(destination)


@pytest.mark.parametrize("module,probe", [("silver", "fingerprint"), ("gold", "inventory")])
def test_local_audit_runner_refuses_databricks_before_access(monkeypatch, module, probe):
    import importlib

    runner = importlib.import_module(f"src.{module}.run_local")
    monkeypatch.setenv("RUNTIME_ENV", "DATABRICKS")
    access = MagicMock()
    monkeypatch.setattr(runner, probe, access)
    with pytest.raises(ValueError, match="Local audit runner only"):
        runner.main()
    access.assert_not_called()
