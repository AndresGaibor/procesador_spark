from __future__ import annotations

from typing import Any

from motor_spark.configuracion.argumentos import ArgumentosEjecucion
from motor_spark.configuracion.modelos.receta import RecetaConfig


def construir_resultado_exito(
    *,
    argumentos: ArgumentosEjecucion,
    receta: RecetaConfig,
    modo_esquema: str,
    datos_entrada: Any,
    datos_salida: Any,
    total_registros: int | None,
    metricas_incrementales: dict[str, Any],
    metricas_salida: dict[str, Any],
) -> dict[str, Any]:
    resultado: dict[str, Any] = {
        "estado": "COMPLETADO",
        "ejecucion_id": argumentos.ejecucion_id,
        "receta": receta.nombre,
        "version_receta": receta.version,
        "entrada": argumentos.entrada,
        "salida": argumentos.salida,
        "modo_esquema_entrada": modo_esquema,
        "columnas_entrada": datos_entrada.columns,
        "esquema_entrada": datos_entrada.schema.jsonValue(),
        "esquema_entrada_simple": datos_entrada.schema.simpleString(),
        "columnas_salida": datos_salida.columns,
        "esquema_salida": datos_salida.schema.jsonValue(),
        "esquema_salida_simple": datos_salida.schema.simpleString(),
        "total_registros": total_registros,
    }
    resultado.update(metricas_incrementales)
    resultado.update(metricas_salida)
    return resultado


def construir_resultado_error(
    argumentos: ArgumentosEjecucion,
    excepcion: Exception,
) -> dict[str, Any]:
    return {
        "estado": "ERROR",
        "ejecucion_id": argumentos.ejecucion_id,
        "entrada": argumentos.entrada,
        "salida": argumentos.salida,
        "tipo_error": type(excepcion).__name__,
        "mensaje": str(excepcion),
    }
