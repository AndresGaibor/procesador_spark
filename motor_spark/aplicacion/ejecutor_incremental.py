from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.incremental import IncrementalConfig
from motor_spark.configuracion.modelos.salida import SalidaConfig
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.esquemas import validar_evolucion_esquema
from motor_spark.infraestructura.spark.escritor import escribir_datos
from motor_spark.infraestructura.spark.sistema_archivos import (
    obtener_metricas_salida,
    ruta_existe_hadoop,
)


@dataclass(frozen=True, slots=True)
class ResultadoIncremental:
    total_registros: int
    metricas_incrementales: dict[str, Any]
    metricas_salida: dict[str, Any]


def ejecutar_incremental(
    *,
    spark: Any,
    procesados: Any,
    ruta_salida: str,
    configuracion_incremental: IncrementalConfig,
    configuracion_salida: SalidaConfig,
) -> ResultadoIncremental:
    politica_duplicados = str(
        configuracion_incremental.duplicados
    ).strip().lower()
    if politica_duplicados != "ignorar":
        raise ErrorReceta(
            "La única política incremental "
            "soportada actualmente es duplicados=ignorar"
        )

    claves = [
        str(clave).strip()
        for clave in configuracion_incremental.claves
        if str(clave).strip()
    ]
    if not claves:
        raise ErrorReceta("incremental.claves no puede estar vacío")

    exigir_columnas(procesados, claves, 0)
    condicion_nulos = " OR ".join(
        f"`{clave}` IS NULL" for clave in claves
    )
    if procesados.where(condicion_nulos).limit(1).count() > 0:
        raise ErrorReceta(
            "Las claves incrementales no pueden contener valores nulos: "
            + ", ".join(claves)
        )

    procesados.persist()
    try:
        total_entrada = procesados.count()
        if total_entrada <= 0:
            raise ErrorReceta(
                "El lote incremental no contiene registros procesables"
            )

        lote_unico = procesados.dropDuplicates(claves).persist()
        try:
            total_unicos_lote = lote_unico.count()
            total_antes = 0
            destino_existe = ruta_existe_hadoop(
                spark,
                ruta_salida,
            )
            nuevos = lote_unico
            nuevos_persistidos = False

            if destino_existe:
                existentes = (
                    spark.read
                    .format("parquet")
                    .option("mergeSchema", "true")
                    .load(ruta_salida)
                    .persist()
                )
                try:
                    validar_evolucion_esquema(
                        existentes.schema,
                        procesados.schema,
                    )
                    exigir_columnas(existentes, claves, 0)
                    total_antes = existentes.count()
                    claves_existentes = (
                        existentes
                        .select(*claves)
                        .dropDuplicates(claves)
                    )
                    nuevos = (
                        lote_unico
                        .join(
                            claves_existentes,
                            on=claves,
                            how="left_anti",
                        )
                        .persist()
                    )
                    nuevos_persistidos = True
                    total_nuevos = nuevos.count()
                finally:
                    existentes.unpersist()
            else:
                total_nuevos = total_unicos_lote

            try:
                total_duplicados = total_entrada - total_nuevos
                total_despues = total_antes + total_nuevos
                salida_append = configuracion_salida.model_copy(
                    update={"modo": "append"}
                )
                if total_nuevos > 0:
                    metricas_salida = escribir_datos(
                        spark=spark,
                        datos=nuevos,
                        ruta=ruta_salida,
                        configuracion=salida_append,
                    )
                else:
                    metricas_salida = obtener_metricas_salida(
                        spark=spark,
                        ruta_salida=ruta_salida,
                    )
            finally:
                if nuevos_persistidos:
                    nuevos.unpersist()

            metricas_incrementales = {
                "modo_carga": "incremental",
                "claves_deduplicacion": claves,
                "total_registros_entrada": total_entrada,
                "total_registros_unicos_lote": total_unicos_lote,
                "total_registros_nuevos": total_nuevos,
                "total_registros_duplicados": total_duplicados,
                "total_registros_antes": total_antes,
            }
            emitir(
                "INCREMENTAL_RESULTADO="
                f"entrada={total_entrada} "
                f"unicos_lote={total_unicos_lote} "
                f"nuevos={total_nuevos} "
                f"duplicados={total_duplicados} "
                f"antes={total_antes} "
                f"despues={total_despues}"
            )
            return ResultadoIncremental(
                total_registros=total_despues,
                metricas_incrementales=metricas_incrementales,
                metricas_salida=metricas_salida,
            )
        finally:
            lote_unico.unpersist()
    finally:
        procesados.unpersist()
