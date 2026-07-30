"""Utilidades compartidas para crear sesiones Spark deterministas en pruebas.

Spark inicia procesos Python separados para ejecutar transformaciones. Si el
proceso principal usa el entorno virtual pero los workers heredan otro Python
del sistema, las acciones distribuidas fallan antes de probar el código real.
Este módulo obliga a que driver y workers usen el mismo intérprete.
"""

from __future__ import annotations

import os
import shutil
import sys

import pytest


def spark_disponible() -> bool:
    """Comprueba las dependencias mínimas sin intentar iniciar una JVM."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False

    return shutil.which("java") is not None


def crear_spark_local(app_name: str, *, case_sensitive: bool = False):
    """Crea una sesión local que usa ``sys.executable`` en todos los procesos.

    ``PYSPARK_PYTHON`` controla los workers. Las propiedades Spark repiten la
    selección para conservarla aunque el proceso reutilice una sesión existente.
    """
    if not spark_disponible():
        pytest.skip("PySpark o Java no disponible")

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.caseSensitive", str(case_sensitive).lower())
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
