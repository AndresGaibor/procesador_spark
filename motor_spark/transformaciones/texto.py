from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import NormalizarTextoPaso
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta


def aplicar_normalizacion_texto(
    datos: Any,
    paso: NormalizarTextoPaso,
    numero_paso: int,
) -> Any:
    from pyspark.sql import functions as F

    exigir_columnas(datos, paso.columnas, numero_paso)
    resultado = datos

    for columna in paso.columnas:
        expresion = F.col(columna)
        for operacion_cruda in paso.operaciones:
            operacion = operacion_cruda.lower()
            if operacion == "trim":
                expresion = F.trim(expresion)
            elif operacion == "lower":
                expresion = F.lower(expresion)
            elif operacion == "upper":
                expresion = F.upper(expresion)
            else:
                raise ErrorReceta(
                    f"Paso {numero_paso}: normalización no soportada: {operacion}"
                )
        resultado = resultado.withColumn(columna, expresion)

    return resultado
