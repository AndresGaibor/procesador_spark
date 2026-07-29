from __future__ import annotations

from typing import Any

from motor_spark.compartido.booleanos import convertir_booleano
from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.dominio.columnas import (
    aplicar_tipos_forzados_entrada,
    normalizar_columnas_entrada,
)
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.esquemas import (
    convertir_columnas_void_a_string,
    obtener_modo_esquema,
)


def convertir_opciones(
    opciones: dict[str, Any],
) -> dict[str, str]:
    resultado: dict[str, str] = {}
    for clave, valor in opciones.items():
        if isinstance(valor, bool):
            resultado[clave] = str(valor).lower()
        else:
            resultado[clave] = str(valor)
    return resultado


def leer_datos(
    spark: Any,
    ruta: str,
    esquema: Any | None,
    configuracion: EntradaConfig,
) -> Any:
    formato = configuracion.formato
    modo_esquema = obtener_modo_esquema(configuracion)
    opciones = convertir_opciones(configuracion.opciones)

    if modo_esquema == "inferir":
        if esquema is not None:
            raise ErrorReceta(
                "En modo inferir no debe aplicarse "
                "un StructType fijo"
            )
        if formato == "csv":
            tiene_cabecera = convertir_booleano(
                opciones.get("header"),
                True,
            )
            if not tiene_cabecera:
                raise ErrorReceta(
                    "El modo inferir para CSV requiere "
                    "entrada.opciones.header=true"
                )
            inferir_tipos = convertir_booleano(
                configuracion.inferir_tipos,
                True,
            )
            opciones["header"] = "true"
            opciones["inferSchema"] = str(inferir_tipos).lower()
            opciones.setdefault("mode", "FAILFAST")

    lector = spark.read.format(formato).options(**opciones)
    if esquema is not None:
        lector = lector.schema(esquema)
    datos = lector.load(ruta)

    if modo_esquema == "inferir":
        normalizar_nombres = convertir_booleano(
            configuracion.normalizar_nombres_columnas,
            True,
        )
        datos = normalizar_columnas_entrada(
            datos,
            activar=normalizar_nombres,
        )
        datos = convertir_columnas_void_a_string(datos)
        datos = aplicar_tipos_forzados_entrada(
            datos,
            configuracion,
        )

    emitir(
        "LECTURA_COMPLETADA="
        f"formato={formato} "
        f"modo_esquema={modo_esquema} "
        f"columnas={len(datos.columns)}"
    )
    return datos
