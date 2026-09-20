from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from src.common.runtime import RuntimeConfig
from src.pipelines import stages
from src.pipelines.run import PipelineFailure, main, run_pipeline


@pytest.fixture
def config(tmp_path):
    return RuntimeConfig.from_env({"DATA_BASE_PATH": str(tmp_path)})


@pytest.mark.parametrize("stage", ["bronze", "silver", "gold", "full"])
def test_dispatch_order_and_caller_session_lifetime(monkeypatch, config, stage):
    calls = []
    session = MagicMock()
    for name in ("bronze", "silver", "gold"):
        def execute(spark, received, name=name):
            assert spark is session and received is config
            calls.append(name)
            return {"status": "SUCCESS"}
        monkeypatch.setattr(stages, f"run_{name}", execute)
    report = run_pipeline(stage, config=config, spark=session)
    assert calls == (["bronze", "silver", "gold"] if stage == "full" else [stage])
    assert report["status"] == "SUCCESS"
    session.stop.assert_not_called()


@pytest.mark.parametrize("runtime", ["LOCAL", "DATABRICKS"])
@pytest.mark.parametrize("fail", [False, True])
def test_session_lifetime_and_error_propagation(monkeypatch, tmp_path, runtime, fail):
    config = RuntimeConfig.from_env({"RUNTIME_ENV": runtime, "DATA_BASE_PATH": (
        str(tmp_path) if runtime == "LOCAL" else "abfss://container@account/lake")})
    session = MagicMock()
    monkeypatch.setattr("src.pipelines.run.get_spark_session", MagicMock(return_value=session))
    action = MagicMock(return_value={"status": "SUCCESS"})
    if fail:
        action.side_effect = OSError("storage unavailable")
    monkeypatch.setattr(stages, "run_bronze", action)
    if fail:
        with pytest.raises(PipelineFailure) as exc:
            run_pipeline("bronze", config=config)
        assert isinstance(exc.value.__cause__, OSError)
        assert exc.value.report["failed_stage"] == "bronze"
    else:
        assert run_pipeline("bronze", config=config)["status"] == "SUCCESS"
    assert session.stop.call_count == (1 if runtime == "LOCAL" else 0)


def test_cli_summary(monkeypatch, capsys):
    execute = MagicMock(return_value={"status": "SUCCESS"})
    monkeypatch.setattr("src.pipelines.run.run_pipeline", execute)
    main(["--stage", "gold"])
    execute.assert_called_once_with("gold")
    assert '"status": "SUCCESS"' in capsys.readouterr().out


def test_cli_failure_printed_and_raised(monkeypatch, capsys):
    failure = PipelineFailure({"status": "FAILED", "failed_stage": "silver"})
    monkeypatch.setattr("src.pipelines.run.run_pipeline", MagicMock(side_effect=failure))
    with pytest.raises(PipelineFailure):
        main(["--stage", "full"])
    assert '"status": "FAILED"' in capsys.readouterr().out


def test_bronze_entrypoint_calls_ingestion(monkeypatch, config):
    ingestion = MagicMock()
    monkeypatch.setattr(stages, "run_bronze_ingestion", ingestion)
    session = MagicMock()
    stages.run_bronze(session, config)
    ingestion.assert_called_once_with(spark=session, config=config)


def test_read_layer_reads_all_datasets_as_delta():
    spark = MagicMock()
    config = MagicMock()
    layer = "bronze"
    config.dataset.side_effect = lambda requested_layer, name: (
        f"/data/{requested_layer}/{name}"
    )
    loaded = {name: object() for name in stages.DATASETS}
    spark.read.format.return_value.load.side_effect = loaded.values()

    result = stages.read_layer(spark, config, layer)

    assert result == loaded
    assert config.dataset.call_args_list == [call(layer, name) for name in stages.DATASETS]
    assert spark.read.format.call_args_list == [call("delta")] * len(stages.DATASETS)
    assert spark.read.format.return_value.load.call_args_list == [
        call(f"/data/{layer}/{name}") for name in stages.DATASETS
    ]


@pytest.fixture
def bronze(spark):
    definitions = {
        "orders": """SELECT * FROM VALUES
            (1, 1, 'prior', 1, 0, 8, CAST(NULL AS DOUBLE)),
            (2, 1, 'train', 2, 1, 9, 1D), (3, 2, 'test', 1, 2, 10, NULL)
            AS t(order_id, user_id, eval_set, order_number, order_dow,
                 order_hour_of_day, days_since_prior_order)""",
        "products": "SELECT 10 product_id, 'Apple' product_name, 1 aisle_id, 1 department_id",
        "aisles": "SELECT 1 aisle_id, 'fruit' aisle",
        "departments": "SELECT 1 department_id, 'produce' department",
        "order_products_prior": "SELECT 1 order_id, 10 product_id, 1 add_to_cart_order, 0 reordered",
        "order_products_train": "SELECT 2 order_id, 10 product_id, 1 add_to_cart_order, 1 reordered",
    }
    return {name: spark.sql(query).selectExpr(
        "*", "timestamp'2026-01-01' AS _ingested_at", "'synthetic' AS _source_file",
        "'synthetic-batch' AS _batch_id") for name, query in definitions.items()}


def test_full_pipeline_with_real_synthetic_transformations(spark, bronze, config, monkeypatch):
    published = []
    monkeypatch.setattr(stages, "run_bronze_ingestion", MagicMock())
    monkeypatch.setattr(stages, "read_layer", lambda *args: bronze)

    def publish(frame, name, destination, gate):
        assert gate.status.value == "APPROVED"
        published.append(("silver", name))
        return SimpleNamespace(written=True, row_count=frame.count())

    def write(frame, destination):
        published.append(("gold", destination.rsplit("/", 1)[-1]))

    monkeypatch.setattr(stages, "publish_silver", publish)
    monkeypatch.setattr(stages, "write_delta", write)
    report = run_pipeline(config=config, spark=spark)
    assert report["status"] == "SUCCESS"
    assert len(published) == 10
    assert [layer for layer, _ in published] == ["silver"] * 6 + ["gold"] * 4
    assert report["stages"]["gold"]["rows"] == {
        "gold_dim_product": 1, "gold_fact_order_items": 2,
        "gold_fact_orders": 3, "gold_user_behavior": 3,
    }


@pytest.mark.parametrize("status", ["BLOCKED", "NEEDS_REVIEW"])
def test_gate_refusal_prevents_all_silver_and_gold_writes(
    spark, bronze, config, monkeypatch, status,
):
    from src.quality.publication_gate import PublicationStatus

    monkeypatch.setattr(stages, "run_bronze_ingestion", MagicMock())
    monkeypatch.setattr(stages, "read_layer", lambda *args: bronze)
    evaluator = stages.evaluate_publication_gate

    def reject_last(summary, checks):
        if summary["dataset"] == "order_products_train":
            return SimpleNamespace(status=PublicationStatus(status), reason="synthetic rejection")
        return evaluator(summary, checks)

    monkeypatch.setattr(stages, "evaluate_publication_gate", reject_last)
    publisher, gold = MagicMock(), MagicMock()
    monkeypatch.setattr(stages, "publish_silver", publisher)
    monkeypatch.setattr(stages, "run_gold", gold)
    with pytest.raises(PipelineFailure) as exc:
        run_pipeline(config=config, spark=spark)
    assert exc.value.report["gates"]["order_products_train"]["status"] == status
    assert len(exc.value.report["gates"]) == 6
    assert list(exc.value.report["stages"]) == ["bronze"]
    publisher.assert_not_called()
    gold.assert_not_called()


def test_gold_validation_failure_before_writes(spark, bronze, config, monkeypatch):
    monkeypatch.setattr(stages, "read_layer", lambda *args: bronze)
    invalid = bronze.copy()
    invalid["products"] = bronze["products"].filter("product_id != 10")
    monkeypatch.setattr(stages, "read_layer", lambda *args: invalid)
    writer = MagicMock()
    monkeypatch.setattr(stages, "write_delta", writer)
    with pytest.raises(ValueError, match="empty output"):
        stages.run_gold(spark, config)
    writer.assert_not_called()
