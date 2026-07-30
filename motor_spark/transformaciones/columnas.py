from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import (
    CrearColumnaPaso,
    EliminarColumnasPaso,
    RellenarNulosPaso,
    RenombrarColumnaPaso,
    SeleccionarColumnasPaso,
)
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta


def aplicar_seleccion(
    datos: Any,
    paso: SeleccionarColumnasPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, paso.columnas, numero_paso)
    return datos.select(*paso.columnas)


def aplicar_eliminacion(
    datos: Any,
    paso: EliminarColumnasPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, paso.columnas, numero_paso)
    return datos.drop(*paso.columnas)


def aplicar_renombrado(
    datos: Any,
    paso: RenombrarColumnaPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, [paso.origen], numero_paso)
    if paso.destino in datos.columns:
        raise ErrorReceta(f"Paso {numero_paso}: ya existe {paso.destino}")
    return datos.withColumnRenamed(paso.origen, paso.destino)


def aplicar_creacion_columna(
    datos: Any,
    paso: CrearColumnaPaso,
    numero_paso: int,
) -> Any:
    from pyspark.sql import functions as F

    del numero_paso
    return datos.withColumn(paso.nombre, F.expr(paso.expresion))


def aplicar_relleno_nulos(
    datos: Any,
    paso: RellenarNulosPaso,
    numero_paso: int,
) -> Any:
    exigir_columnas(datos, list(paso.valores.keys()), numero_paso)
    return datos.fillna(paso.valores)
