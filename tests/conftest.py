"""Shared Spark lifecycle; test data remains isolated in per-test tmp_path directories."""

import pytest
from pyspark.sql import SparkSession

from src.common.spark import get_spark_session, to_spark_path


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("spark_session")
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(root)
        SparkSession.builder.config("spark.sql.warehouse.dir", to_spark_path(root / "warehouse"))
        session = get_spark_session("Synthetic project tests")
        try:
            yield session
        finally:
            session.stop()
