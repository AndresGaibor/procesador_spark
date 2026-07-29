from __future__ import annotations

import json
import traceback
from typing import Any

from motor_spark.aplicacion.ejecutor_incremental import ejecutar_incremental
from motor_spark.aplicacion.resultado_ejecucion import (
    construir_resultado_error,
    construir_resultado_exito,
)
from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.argumentos import ArgumentosEjecucion
from motor_spark.configuracion.cargador_receta import cargar_receta
from motor_spark.dominio.esquemas import resolver_esquema_entrada
from motor_spark.infraestructura.resultados.escritor_json import guardar_resultado
from motor_spark.infraestructura.spark.escritor import escribir_datos
from motor_spark.infraestructura.spark.lector import leer_datos
from motor_spark.infraestructura.spark.sesion import crear_sesion_spark
from motor_spark.transformaciones.ejecutor import aplicar_pasos


def _ejecutar_carga_completa(
    *,
    spark: Any,
    procesados: Any,
    ruta_salida: str,
    configuracion_salida: Any,
    contar_registros: bool,
) -> tuple[int | None, dict[str, Any]]:
    if not contar_registros:
        return None, escribir_datos(
            spark=spark,
            datos=procesados,
            ruta=ruta_salida,
            configuracion=configuracion_salida,
        )

    procesados.persist()
    try:
        total_registros = procesados.count()
        metricas_salida = escribir_datos(
            spark=spark,
            datos=procesados,
            ruta=ruta_salida,
            configuracion=configuracion_salida,
        )
        return total_registros, metricas_salida
    finally:
        procesados.unpersist()


def ejecutar_motor(argumentos: ArgumentosEjecucion) -> int:
    spark: Any | None = None

    try:
        receta = cargar_receta(argumentos.receta)
        modo_esquema, esquema = resolver_esquema_entrada(
            argumentos.esquema,
            receta.entrada,
        )
        spark = crear_sesion_spark(
            receta.nombre,
            argumentos.ejecucion_id,
            receta.spark,
        )

        emitir("EJECUCION_INICIO=" + argumentos.ejecucion_id)
        emitir("MODO_ESQUEMA_ENTRADA=" + modo_esquema)

        if esquema is not None:
            emitir(
                "ESQUEMA_ENTRADA_DECLARADO="
                + esquema.simpleString()
            )
        elif argumentos.esquema.strip():
            emitir("SCHEMA_SPEC_IGNORADO=true")

        datos = leer_datos(
            spark=spark,
            ruta=argumentos.entrada,
            esquema=esquema,
            configuracion=receta.entrada,
        )
        emitir(
            "ESQUEMA_ENTRADA_REAL="
            + datos.schema.simpleString()
        )

        procesados = aplicar_pasos(datos, receta.pasos)
        metricas_incrementales: dict[str, Any] = {}

        if bool(receta.incremental.activo):
            resultado_incremental = ejecutar_incremental(
                spark=spark,
                procesados=procesados,
                ruta_salida=argumentos.salida,
                configuracion_incremental=receta.incremental,
                configuracion_salida=receta.salida,
            )
            total_registros = resultado_incremental.total_registros
            metricas_incrementales = (
                resultado_incremental.metricas_incrementales
            )
            metricas_salida = resultado_incremental.metricas_salida
        else:
            total_registros, metricas_salida = _ejecutar_carga_completa(
                spark=spark,
                procesados=procesados,
                ruta_salida=argumentos.salida,
                configuracion_salida=receta.salida,
                contar_registros=bool(
                    receta.auditoria.contar_registros
                ),
            )

        resultado = construir_resultado_exito(
            argumentos=argumentos,
            receta=receta,
            modo_esquema=modo_esquema,
            datos_entrada=datos,
            datos_salida=procesados,
            total_registros=total_registros,
            metricas_incrementales=metricas_incrementales,
            metricas_salida=metricas_salida,
        )
        guardar_resultado(argumentos.resultado, resultado)
        emitir(
            "RESULTADO_MOTOR="
            + json.dumps(resultado, ensure_ascii=False)
        )
        return 0

    except Exception as excepcion:
        resultado_error = construir_resultado_error(
            argumentos,
            excepcion,
        )
        guardar_resultado(argumentos.resultado, resultado_error)
        emitir(
            "RESULTADO_MOTOR="
            + json.dumps(resultado_error, ensure_ascii=False),
            error=True,
        )
        traceback.print_exc()
        return 1

    finally:
        if spark is not None:
            spark.stop()
