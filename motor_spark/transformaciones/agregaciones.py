from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import AgruparPaso, MetricaConfig
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta


def construir_agregacion(metrica: MetricaConfig) -> Any:
    from pyspark.sql import functions as F

    operacion = metrica.operacion.lower()
    columna = metrica.columna
    alias = metrica.alias

    if operacion == "sum":
        expresion = F.sum(F.col(columna))
    elif operacion == "avg":
        expresion = F.avg(F.col(columna))
    elif operacion == "min":
        expresion = F.min(F.col(columna))
    elif operacion == "max":
        expresion = F.max(F.col(columna))
    elif operacion == "count":
        expresion = (
            F.count(F.lit(1))
            if columna == "*"
            else F.count(F.col(columna))
        )
    elif operacion == "count_distinct":
        expresion = F.countDistinct(F.col(columna))
    elif operacion == "first":
        expresion = F.first(F.col(columna), ignorenulls=True)
    elif operacion == "last":
        expresion = F.last(F.col(columna), ignorenulls=True)
    else:
        raise ErrorReceta(f"Agregación no soportada: {operacion}")

    return expresion.alias(alias)


def aplicar_agrupacion(
    datos: Any,
    paso: AgruparPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, paso.columnas, numero_paso)
    if not paso.metricas:
        raise ErrorReceta(
            f"Paso {numero_paso}: la agrupación no tiene métricas"
        )

    expresiones: list[Any] = []
    for metrica in paso.metricas:
        if metrica.columna != "*":
            exigir_columnas(
                datos,
                [metrica.columna],
                numero_paso,
            )
        expresiones.append(construir_agregacion(metrica))

    return datos.groupBy(*paso.columnas).agg(*expresiones)
