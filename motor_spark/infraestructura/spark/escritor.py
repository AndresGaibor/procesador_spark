from __future__ import annotations

from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.salida import SalidaConfig
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.infraestructura.spark.sistema_archivos import (
    obtener_metricas_salida,
    preparar_salida_local,
)


def escribir_datos(
    spark: Any,
    datos: Any,
    ruta: str,
    configuracion: SalidaConfig,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    formato = configuracion.formato.lower()
    modo = configuracion.modo.lower()
    modo_efectivo = preparar_salida_local(ruta=ruta, modo=modo)
    compresion = configuracion.compresion
    numero_particiones = configuracion.numero_particiones
    columnas_reparticion = configuracion.columnas_reparticion
    particionar_por = configuracion.particionar_por
    resultado = datos

    if numero_particiones:
        cantidad = int(numero_particiones)
        if cantidad < 1:
            raise ErrorReceta(
                "numero_particiones debe ser mayor que cero"
            )
        if columnas_reparticion:
            exigir_columnas(resultado, columnas_reparticion, 0)
            resultado = resultado.repartition(
                cantidad,
                *[F.col(columna) for columna in columnas_reparticion],
            )
        else:
            resultado = resultado.repartition(cantidad)

    if particionar_por:
        exigir_columnas(resultado, particionar_por, 0)

    escritor = resultado.write.format(formato).mode(modo_efectivo)
    if compresion and compresion != "none":
        escritor = escritor.option("compression", compresion)
    if particionar_por:
        escritor = escritor.partitionBy(*particionar_por)

    emitir(
        "ESCRITURA_INICIO="
        f"ruta={ruta} "
        f"formato={formato} "
        f"modo={modo_efectivo}"
    )
    escritor.save(ruta)
    emitir(f"ESCRITURA_FIN=ruta={ruta}")

    if formato != "parquet":
        raise ErrorReceta(
            "La publicación actual hacia Impala requiere "
            f"formato parquet, pero se recibió: {formato}"
        )
    return obtener_metricas_salida(
        spark=spark,
        ruta_salida=ruta,
    )
