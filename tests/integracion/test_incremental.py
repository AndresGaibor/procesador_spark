import pytest

pytest.importorskip("pyspark")

from motor_spark.aplicacion.ejecutor_incremental import ejecutar_incremental
from motor_spark.configuracion.modelos.incremental import IncrementalConfig
from motor_spark.configuracion.modelos.salida import SalidaConfig

pytestmark = pytest.mark.spark


def test_incremental_sin_destino_deduplica_lote(spark_local, tmp_path):
    datos = spark_local.createDataFrame(
        [(1, "a"), (1, "a"), (2, "b")],
        ["id", "valor"],
    )
    salida = (tmp_path / "incremental").as_uri()
    resultado = ejecutar_incremental(
        spark=spark_local,
        procesados=datos,
        ruta_salida=salida,
        configuracion_incremental=IncrementalConfig(
            activo=True,
            claves=["id"],
        ),
        configuracion_salida=SalidaConfig(),
    )
    assert resultado.total_registros == 2
    assert resultado.metricas_incrementales[
        "total_registros_entrada"
    ] == 3
    assert resultado.metricas_incrementales[
        "total_registros_nuevos"
    ] == 2
    assert resultado.metricas_incrementales[
        "total_registros_duplicados"
    ] == 1
