from __future__ import annotations

import shutil

import pytest


@pytest.fixture(scope="session")
def spark_local():
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java no está instalado")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("motor-spark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
