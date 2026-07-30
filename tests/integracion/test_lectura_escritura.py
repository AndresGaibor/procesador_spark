import pytest

pytest.importorskip("pyspark")

from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.configuracion.modelos.salida import SalidaConfig
from motor_spark.dominio.esquemas import construir_esquema
from motor_spark.infraestructura.spark.escritor import escribir_datos
from motor_spark.infraestructura.spark.lector import leer_datos

pytestmark = pytest.mark.spark


def test_csv_estricto_a_parquet(spark_local, tmp_path):
    entrada = tmp_path / "ventas.csv"
    entrada.write_text(
        "id,nombre,total\n1,Ana,10.50\n2,Luis,20.00\n",
        encoding="utf-8",
    )
    salida = tmp_path / "salida"
    datos = leer_datos(
        spark=spark_local,
        ruta=str(entrada),
        esquema=construir_esquema("id:entero|nombre:texto|total:decimal(10,2)"),
        configuracion=EntradaConfig(
            formato="csv",
            opciones={"header": True},
        ),
    )
    metricas = escribir_datos(
        spark=spark_local,
        datos=datos,
        ruta=salida.as_uri(),
        configuracion=SalidaConfig(modo="error"),
    )
    assert metricas["archivo_success"] is True
    assert metricas["cantidad_archivos_parquet"] >= 1
    assert metricas["bytes_parquet"] > 0
    assert (salida / "_SUCCESS").exists()
