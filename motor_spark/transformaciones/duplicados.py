from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import EliminarDuplicadosPaso
from motor_spark.dominio.columnas import exigir_columnas


def aplicar_eliminacion_duplicados(
    datos: Any,
    paso: EliminarDuplicadosPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, paso.columnas, numero_paso)
    return datos.dropDuplicates(paso.columnas)
