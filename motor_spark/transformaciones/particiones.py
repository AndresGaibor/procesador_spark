from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import ReparticionarPaso
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta


def aplicar_reparticion(
    datos: Any,
    paso: ReparticionarPaso,
    numero_paso: int,
) -> Any:
    from pyspark.sql import functions as F

    cantidad = int(paso.cantidad)
    if cantidad < 1:
        raise ErrorReceta(f"Paso {numero_paso}: particiones inválidas")

    if paso.columnas:
        exigir_columnas(datos, paso.columnas, numero_paso)
        return datos.repartition(
            cantidad,
            *[F.col(columna) for columna in paso.columnas],
        )
    return datos.repartition(cantidad)
