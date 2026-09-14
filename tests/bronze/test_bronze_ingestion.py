"""Integration tests use only synthetic CSVs under pytest temporary directories."""

from uuid import UUID, uuid4

import pytest
from pyspark.sql.types import StringType, TimestampType

from src.bronze.ingest import (
    DATASETS,
    add_ingestion_metadata,
    ingest_dataset,
    read_csv,
    run_bronze_ingestion,
    validate_source_file,
    write_delta,
)
from src.bronze.schemas import AISLES_SCHEMA, PRODUCTS_SCHEMA
from src.common.spark import to_spark_path


@pytest.fixture
def source_csv(tmp_path):
    source = tmp_path / "raw" / "aisles.csv"
    source.parent.mkdir()
    source.write_text("aisle_id,aisle\n1,fruit\n2,bread\n", encoding="utf-8")
    return source


def test_products_csv_double_quotes_and_literal_backslash(spark, tmp_path):
    source = tmp_path / "products.csv"
    source.write_text(
        'product_id,product_name,aisle_id,department_id\n'
        '1,Simple name,10,17\n'
        '2,"Name, with comma",10,17\n'
        '3,"Name with ""quotes""",10,17\n'
        + r'6816,"Scotch Kids 5\"" Scissors, Blunted, Red",87,17' + '\n',
        encoding="utf-8",
    )
    original = source.read_bytes()
    dataframe = read_csv(spark, source, PRODUCTS_SCHEMA)
    assert dataframe.schema == PRODUCTS_SCHEMA
    assert [tuple(row) for row in dataframe.orderBy("product_id").collect()] == [
        (1, "Simple name", 10, 17),
        (2, "Name, with comma", 10, 17),
        (3, 'Name with "quotes"', 10, 17),
        (6816, r'Scotch Kids 5\" Scissors, Blunted, Red', 87, 17),
    ]
    assert source.read_bytes() == original


def test_missing_source_has_clear_error(tmp_path):
    source = tmp_path / "missing.csv"
    with pytest.raises(FileNotFoundError, match="Source CSV file not found") as error:
        validate_source_file(source)
    assert str(source) in str(error.value)


def test_missing_batch_source_fails_before_spark(tmp_path, monkeypatch):
    def unexpected_session(*args):
        pytest.fail("Spark must not start when a source is missing")

    monkeypatch.setattr("src.bronze.ingest.get_spark_session", unexpected_session)
    with pytest.raises(FileNotFoundError, match="orders.csv"):
        run_bronze_ingestion(tmp_path / "raw", tmp_path / "bronze")
    assert not (tmp_path / "bronze").exists()


def test_overlapping_paths_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="non-overlapping"):
        run_bronze_ingestion(tmp_path, tmp_path / "bronze")


def test_synthetic_csv_and_metadata(spark, source_csv):
    dataframe = read_csv(spark, source_csv, AISLES_SCHEMA)
    assert dataframe.schema == AISLES_SCHEMA
    assert [tuple(row) for row in dataframe.orderBy("aisle_id").collect()] == [
        (1, "fruit"), (2, "bread"),
    ]
    batch_id = str(uuid4())
    enriched = add_ingestion_metadata(dataframe, source_csv, batch_id)
    assert enriched.columns == ["aisle_id", "aisle", "_ingested_at", "_source_file", "_batch_id"]
    assert isinstance(enriched.schema["_ingested_at"].dataType, TimestampType)
    assert isinstance(enriched.schema["_source_file"].dataType, StringType)
    assert isinstance(enriched.schema["_batch_id"].dataType, StringType)
    for row in enriched.collect():
        assert row._source_file == source_csv.resolve().as_posix()
        assert row._batch_id == batch_id
        assert UUID(row._batch_id).version == 4
        assert row._ingested_at is not None
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_delta_round_trip_and_overwrite(spark, source_csv, tmp_path):
    original = source_csv.read_bytes()
    destination = tmp_path / "bronze" / "aisles"
    dataframe = add_ingestion_metadata(
        read_csv(spark, source_csv, AISLES_SCHEMA), source_csv, str(uuid4())
    )
    write_delta(dataframe, destination)
    assert (destination / "_delta_log").is_dir()
    restored = spark.read.format("delta").load(to_spark_path(destination))
    assert restored.count() == 2
    assert [(f.name, f.dataType) for f in restored.schema] == [
        (f.name, f.dataType) for f in dataframe.schema
    ]
    batch_id = str(uuid4())
    ingest_dataset(spark, "aisles", source_csv.parent, tmp_path / "bronze", batch_id)
    replaced = spark.read.format("delta").load(to_spark_path(destination))
    assert replaced.count() == 2
    assert [row._batch_id for row in replaced.select("_batch_id").distinct().collect()] == [batch_id]
    assert source_csv.read_bytes() == original


def test_complete_synthetic_batch(spark, tmp_path):
    raw = tmp_path / "raw"
    bronze = tmp_path / "bronze"
    raw.mkdir()
    rows = {
        "orders": "1,2,prior,1,0,10,\n",
        "products": '1,"Apple, green",1,1\n',
        "aisles": "1,fruit\n",
        "departments": "1,produce\n",
        "order_products_prior": "1,1,1,0\n",
        "order_products_train": "2,1,1,1\n",
    }
    originals = {}
    for name, dataset in DATASETS.items():
        source = raw / dataset.filename
        source.write_text(
            ",".join(dataset.schema.fieldNames()) + "\n" + rows[name], encoding="utf-8"
        )
        originals[source] = source.read_bytes()

    batch_id = run_bronze_ingestion(raw, bronze, spark=spark)
    assert UUID(batch_id).version == 4
    assert {path.name for path in bronze.iterdir()} == set(rows)
    for name, dataset in DATASETS.items():
        destination = bronze / name
        assert (destination / "_delta_log").is_dir()
        restored = spark.read.format("delta").load(to_spark_path(destination))
        assert restored.columns == dataset.schema.fieldNames() + [
            "_ingested_at", "_source_file", "_batch_id",
        ]
        records = restored.collect()
        assert len(records) == 1
        assert records[0]._batch_id == batch_id
        assert records[0]._source_file == (raw / dataset.filename).resolve().as_posix()
        assert records[0]._ingested_at is not None
    for source, original in originals.items():
        assert source.read_bytes() == original
    assert spark.range(1).count() == 1  # Caller-owned session is still usable.


def test_ingestion_with_spaces_in_local_paths(spark, tmp_path):
    project = tmp_path / "Azure Databricks Governed Lakehouse" / "projeto com espaços"
    raw = project / "raw files"
    bronze = project / "bronze tables"
    raw.mkdir(parents=True)
    source = raw / "aisles.csv"
    source.write_text("aisle_id,aisle\n1,fruit\n2,bread\n", encoding="utf-8")
    original = source.read_bytes()
    batch_id = str(uuid4())

    destination = ingest_dataset(spark, "aisles", raw, bronze, batch_id)

    assert destination == bronze / "aisles"
    assert (destination / "_delta_log").is_dir()
    restored = spark.read.format("delta").load(to_spark_path(destination))
    assert restored.count() == 2
    rows = restored.orderBy("aisle_id").collect()
    assert [(row.aisle_id, row.aisle) for row in rows] == [(1, "fruit"), (2, "bread")]
    for row in rows:
        assert row._source_file == source.resolve().as_posix()
        assert "Azure Databricks Governed Lakehouse" in row._source_file
        assert "%20" not in row._source_file
        assert row._batch_id == batch_id
        assert row._ingested_at is not None
    assert source.read_bytes() == original
