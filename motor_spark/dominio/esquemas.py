from __future__ import annotations

import re
from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.tipos_spark import convertir_tipo_spark

PATRON_NOMBRE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def construir_esquema(especificacion: str) -> Any:
    from pyspark.sql.types import StructField, StructType

    if not especificacion or not especificacion.strip():
        raise ErrorReceta("SCHEMA_SPEC está vacío")

    campos: list[Any] = []
    nombres: set[str] = set()

    for posicion, definicion in enumerate(
        especificacion.split("|"),
        start=1,
    ):
        definicion = definicion.strip()
        if not definicion:
            raise ErrorReceta(f"Definición vacía en la posición {posicion}")
        if ":" not in definicion:
            raise ErrorReceta(f"Falta ':' en la definición: {definicion}")

        nombre, tipo = definicion.split(":", 1)
        nombre = nombre.strip()
        tipo = tipo.strip()

        if not PATRON_NOMBRE.fullmatch(nombre):
            raise ErrorReceta(f"Nombre de columna inválido: {nombre}")

        nombre_normalizado = nombre.lower()
        if nombre_normalizado in nombres:
            raise ErrorReceta(f"Columna repetida: {nombre}")
        nombres.add(nombre_normalizado)
        campos.append(
            StructField(
                nombre,
                convertir_tipo_spark(tipo),
                nullable=True,
            )
        )

    return StructType(campos)


def obtener_modo_esquema(configuracion: EntradaConfig) -> str:
    return configuracion.modo_esquema


def resolver_esquema_entrada(
    especificacion: str | None,
    configuracion: EntradaConfig,
) -> tuple[str, Any | None]:
    modo = obtener_modo_esquema(configuracion)
    if modo == "inferir":
        return modo, None
    return modo, construir_esquema(especificacion or "")


def convertir_columnas_void_a_string(datos: Any) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql.types import NullType

    resultado = datos
    convertidas: list[str] = []
    for campo in datos.schema.fields:
        if isinstance(campo.dataType, NullType):
            resultado = resultado.withColumn(
                campo.name,
                F.col(campo.name).cast("string"),
            )
            convertidas.append(campo.name)

    if convertidas:
        emitir("COLUMNAS_VOID_CONVERTIDAS_STRING=" + ",".join(convertidas))
    return resultado


def validar_evolucion_esquema(
    esquema_actual: Any,
    esquema_nuevo: Any,
) -> list[Any]:
    campos_actuales = list(esquema_actual.fields)
    campos_nuevos = list(esquema_nuevo.fields)

    if len(campos_actuales) > len(campos_nuevos):
        raise ErrorReceta(
            "EVOLUCION_ESQUEMA_RECHAZADA=true; "
            "MOTIVO=ELIMINACION_COLUMNAS; "
            f"ACTUAL={len(campos_actuales)}; "
            f"NUEVO={len(campos_nuevos)}"
        )

    for indice, campo_actual in enumerate(campos_actuales):
        campo_nuevo = campos_nuevos[indice]
        if campo_actual.name != campo_nuevo.name:
            raise ErrorReceta(
                "EVOLUCION_ESQUEMA_RECHAZADA=true; "
                "MOTIVO=RENOMBRE_REORDENAMIENTO_"
                "O_INSERCION_INTERMEDIA; "
                f"POSICION={indice}; "
                f"ACTUAL={campo_actual.name}; "
                f"NUEVO={campo_nuevo.name}"
            )

        tipo_actual = campo_actual.dataType.simpleString()
        tipo_nuevo = campo_nuevo.dataType.simpleString()
        if tipo_actual != tipo_nuevo:
            raise ErrorReceta(
                "EVOLUCION_ESQUEMA_RECHAZADA=true; "
                "MOTIVO=CAMBIO_TIPO; "
                f"COLUMNA={campo_actual.name}; "
                f"ACTUAL={tipo_actual}; "
                f"NUEVO={tipo_nuevo}"
            )

    nuevas_columnas = campos_nuevos[len(campos_actuales) :]
    emitir(
        "EVOLUCION_ESQUEMA_SPARK_VALIDADA=true "
        f"actual={len(campos_actuales)} "
        f"nuevo={len(campos_nuevos)} "
        f"agregadas={len(nuevas_columnas)}"
    )
    return nuevas_columnas
