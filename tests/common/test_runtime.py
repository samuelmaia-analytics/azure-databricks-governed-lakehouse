from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.common.paths import join_path, require_separate_paths, to_spark_path
from src.common.runtime import PROJECT_ROOT, RuntimeConfig, RuntimeEnv, runtime_env
from src.common.spark import get_spark_session


def test_local_defaults():
    config = RuntimeConfig.from_env({})
    assert config.runtime == RuntimeEnv.LOCAL
    for layer in ("raw", "bronze", "silver", "gold", "quarantine"):
        assert getattr(config, layer) == (PROJECT_ROOT / "data" / layer).as_posix()


def test_local_overrides(tmp_path):
    config = RuntimeConfig.from_env({"DATA_BASE_PATH": str(tmp_path),
                                     "RAW_PATH": str(tmp_path / "input files")})
    assert config.raw == (tmp_path / "input files").as_posix()
    assert config.dataset("gold", "gold_dim_product") == (
        tmp_path / "gold/gold_dim_product").as_posix()


@pytest.mark.parametrize("base", ["dbfs:/example/lake", "abfss://container@account/lake",
                                  "/Volumes/catalog/schema/volume/lake"])
def test_databricks_paths(base):
    config = RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS", "DATABRICKS_BASE_PATH": base})
    assert config.runtime == RuntimeEnv.DATABRICKS
    assert config.raw == base + "/raw"
    assert config.dataset("silver", "orders") == base + "/silver/orders"


def test_databricks_explicit_paths_without_base():
    env = {f"{layer.upper()}_PATH": f"abfss://container@account/{layer}"
           for layer in ("raw", "bronze", "silver", "gold", "quarantine")}
    env.update(RUNTIME_ENV="DATABRICKS", RAW_PATH="/Volumes/catalog/schema/volume/raw")
    assert RuntimeConfig.from_env(env).raw == env["RAW_PATH"]


def test_databricks_base_precedence():
    config = RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS", "DATA_BASE_PATH": "dbfs:/a",
                                     "DATABRICKS_BASE_PATH": "dbfs:/b", "RAW_PATH": "dbfs:/input"})
    assert config.raw == "dbfs:/input"
    assert config.gold == "dbfs:/b/gold"


def test_databricks_requires_explicit_storage():
    with pytest.raises(ValueError, match="BASE_PATH"):
        RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS"})
    with pytest.raises(ValueError, match="Databricks storage"):
        RuntimeConfig.from_env({"RUNTIME_ENV": "DATABRICKS", "DATA_BASE_PATH": "data"})


def test_runtime_detection_and_invalid_value():
    assert runtime_env({"DATABRICKS_RUNTIME_VERSION": "example"}) == RuntimeEnv.DATABRICKS
    assert runtime_env({"RUNTIME_ENV": "local", "DATABRICKS_RUNTIME_VERSION": "example"}) == (
        RuntimeEnv.LOCAL)
    with pytest.raises(ValueError):
        runtime_env({"RUNTIME_ENV": "unknown"})


@pytest.mark.parametrize("path", ["dbfs:/lake/raw", "abfss://container@account/lake/raw",
                                  "/Volumes/catalog/schema/volume/raw"])
def test_remote_never_resolved_with_pathlib(path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Path.resolve must not touch remote paths")
    monkeypatch.setattr(Path, "resolve", forbidden)
    assert to_spark_path(path) == path
    assert join_path(path, "orders.csv") == path + "/orders.csv"


def test_windows_and_relative_paths(tmp_path, monkeypatch):
    assert to_spark_path(r"C:\project with spaces\data") == "C:/project with spaces/data"
    monkeypatch.chdir(tmp_path)
    assert to_spark_path("data/raw") == (tmp_path / "data/raw").as_posix()


@pytest.mark.parametrize("child", ["..", "/orders", "a/b", r"a\b", "dbfs:/orders", ""])
def test_invalid_child(child):
    with pytest.raises(ValueError, match="single relative"):
        join_path("dbfs:/base", child)


@pytest.mark.parametrize("left,right", [
    ("dbfs:/lake", "dbfs:/lake/bronze"),
    ("abfss://container@account/a", "abfss://container@account/a/../a"),
    ("/Volumes/c/s/v/raw", "dbfs:/Volumes/c/s/v/raw"),
    ("C:/DATA", "c:/data/bronze"),
])
def test_overlapping_storage_rejected(left, right):
    with pytest.raises(ValueError, match="non-overlapping"):
        require_separate_paths([left, right])


def test_config_rejects_layer_collision(tmp_path):
    with pytest.raises(ValueError, match="non-overlapping"):
        RuntimeConfig.from_env({"DATA_BASE_PATH": str(tmp_path), "RAW_PATH": str(tmp_path)})


@pytest.mark.parametrize("path", ["", "https://example/data", "dbfs://authority/raw",
                                  "abfss://container@account/raw?secret=value"])
def test_invalid_storage_paths(path):
    with pytest.raises(ValueError):
        to_spark_path(path)


def test_local_spark_builder_preserved(monkeypatch):
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    platform = MagicMock(builder=builder)
    delta = MagicMock(return_value=builder)
    monkeypatch.setattr("src.common.spark.SparkSession", platform)
    monkeypatch.setattr("src.common.spark.configure_spark_with_delta_pip", delta)
    assert get_spark_session("local test", runtime=RuntimeEnv.LOCAL) is builder.getOrCreate.return_value
    builder.master.assert_called_once_with("local[*]")
    delta.assert_called_once_with(builder)


def test_databricks_reuses_active_session_without_builder(monkeypatch):
    platform, delta = MagicMock(), MagicMock()
    monkeypatch.setattr("src.common.spark.SparkSession", platform)
    monkeypatch.setattr("src.common.spark.configure_spark_with_delta_pip", delta)
    assert get_spark_session("platform", runtime=RuntimeEnv.DATABRICKS) is (
        platform.getActiveSession.return_value)
    platform.builder.appName.assert_not_called()
    delta.assert_not_called()


def test_databricks_missing_session_fails_without_local_fallback(monkeypatch):
    platform = MagicMock()
    platform.getActiveSession.return_value = None
    monkeypatch.setattr("src.common.spark.SparkSession", platform)
    with pytest.raises(RuntimeError, match="No active Databricks"):
        get_spark_session("platform", runtime=RuntimeEnv.DATABRICKS)
    platform.builder.appName.assert_not_called()


def test_explicit_platform_session(monkeypatch):
    platform, supplied = MagicMock(), MagicMock()
    monkeypatch.setattr("src.common.spark.SparkSession", platform)
    assert get_spark_session("platform", runtime=RuntimeEnv.DATABRICKS, existing=supplied) is supplied
    platform.getActiveSession.assert_not_called()
