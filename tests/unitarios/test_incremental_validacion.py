import pytest

from motor_spark.aplicacion.ejecutor_incremental import ejecutar_incremental
from motor_spark.configuracion.modelos.incremental import IncrementalConfig
from motor_spark.configuracion.modelos.salida import SalidaConfig
from motor_spark.dominio.errores import ErrorReceta


class DataFrameNoUsado:
    columns = ["id"]


def test_incremental_rechaza_politica_distinta_ignorar():
    with pytest.raises(ErrorReceta, match="única política incremental"):
        ejecutar_incremental(
            spark=object(),
            procesados=DataFrameNoUsado(),
            ruta_salida="file:///tmp/out",
            configuracion_incremental=IncrementalConfig(
                activo=True,
                duplicados="actualizar",
                claves=["id"],
            ),
            configuracion_salida=SalidaConfig(),
        )


def test_incremental_rechaza_claves_vacias():
    with pytest.raises(ErrorReceta, match="incremental.claves no puede"):
        ejecutar_incremental(
            spark=object(),
            procesados=DataFrameNoUsado(),
            ruta_salida="file:///tmp/out",
            configuracion_incremental=IncrementalConfig(
                activo=True,
                claves=[],
            ),
            configuracion_salida=SalidaConfig(),
        )
