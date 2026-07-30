"""Fixtures globales de pytest para el motor de recetas."""

from __future__ import annotations

import pytest

from tests.soporte_spark import crear_spark_local


@pytest.fixture(scope="session")
def spark_local():
    """Comparte una sola SparkSession para reducir el coste de arranque de JVM."""
    spark = crear_spark_local("motor-spark-tests")
    yield spark

    # Detener explícitamente la sesión evita que la JVM sobreviva al proceso de
    # pruebas y contamine ejecuciones posteriores con configuración antigua.
    spark.stop()
