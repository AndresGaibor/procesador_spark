"""Fixtures específicas para las pruebas de integración del modo Dataflow."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.soporte_spark import crear_spark_local, spark_disponible

# La constante permite que los módulos de pruebas marquen casos sin arrancar
# Spark durante la fase de colección de pytest.
HAS_SPARK = spark_disponible()


@pytest.fixture(scope="session")
def spark_local():
    """Crea Spark con nombres de columnas sensibles a mayúsculas como Qlik."""
    spark = crear_spark_local(
        "motor-spark-dataflow-tests",
        case_sensitive=True,
    )
    yield spark

    # El cierre explícito garantiza que una configuración case-sensitive no se
    # reutilice accidentalmente en otra suite ejecutada en el mismo proceso.
    spark.stop()


@pytest.fixture
def recursos_dataflow() -> Path:
    """Raíz única de scripts, catálogos y datos de prueba del compilador."""
    return Path(__file__).parent.parent / "recursos" / "dataflow"


@pytest.fixture
def script_path(recursos_dataflow: Path):
    """Devuelve un resolvedor de nombres dentro del corpus de scripts Qlik."""

    def _get_script(nombre: str) -> Path:
        return recursos_dataflow / "scripts" / nombre

    return _get_script


@pytest.fixture
def conexion_path(recursos_dataflow: Path):
    """Devuelve un resolvedor de catálogos de conexión de prueba."""

    def _get_conexion(nombre: str) -> Path:
        return recursos_dataflow / "conexiones" / nombre

    return _get_conexion


@pytest.fixture
def datos_path(recursos_dataflow: Path) -> Path:
    """Directorio con CSV pequeños usados por pruebas deterministas."""
    return recursos_dataflow / "datos"
