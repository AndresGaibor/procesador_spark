from __future__ import annotations

import shutil
from pathlib import Path

import pytest


def _check_spark_available():
    try:
        import pyspark
    except ImportError:
        return False
    if shutil.which("java") is None:
        return False
    return True


HAS_SPARK = _check_spark_available()


@pytest.fixture(scope="session")
def spark_local():
    if not HAS_SPARK:
        pytest.skip("PySpark o Java no disponible")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("motor-spark-dataflow-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def recursos_dataflow():
    return Path(__file__).parent.parent / "recursos" / "dataflow"


@pytest.fixture
def script_path(recursos_dataflow):
    def _get_script(nombre: str) -> Path:
        return recursos_dataflow / "scripts" / nombre
    return _get_script


@pytest.fixture
def conexion_path(recursos_dataflow):
    def _get_conexion(nombre: str) -> Path:
        return recursos_dataflow / "conexiones" / nombre
    return _get_conexion


@pytest.fixture
def datos_path(recursos_dataflow):
    return recursos_dataflow / "datos"
