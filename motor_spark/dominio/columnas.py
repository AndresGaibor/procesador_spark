from __future__ import annotations

import re
import unicodedata
from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.esquemas import PATRON_NOMBRE
from motor_spark.dominio.tipos_spark import convertir_tipo_spark


def normalizar_nombre_columna(nombre: str) -> str:
    original = str(nombre or "").strip()
    if not original:
        raise ErrorReceta("Se encontró una columna sin nombre")

    sin_tildes = "".join(
        caracter
        for caracter in unicodedata.normalize("NFKD", original)
        if not unicodedata.combining(caracter)
    )
    normalizado = re.sub(r"[^A-Za-z0-9_]+", "_", sin_tildes)
    normalizado = re.sub(r"_+", "_", normalizado).strip("_").lower()

    if not normalizado:
        raise ErrorReceta(
            "El nombre de columna no contiene "
            f"caracteres utilizables: {original!r}"
        )
    if normalizado[0].isdigit():
        normalizado = f"col_{normalizado}"
    if not PATRON_NOMBRE.fullmatch(normalizado):
        raise ErrorReceta(
            "No se pudo normalizar el nombre de columna: "
            f"{original!r} -> {normalizado!r}"
        )
    return normalizado


def exigir_columnas(
    datos: Any,
    columnas: list[str],
    numero_paso: int,
) -> None:
    disponibles = set(datos.columns)
    faltantes = [
        columna for columna in columnas if columna not in disponibles
    ]
    if faltantes:
        raise ErrorReceta(
            f"Paso {numero_paso}: columnas inexistentes: "
            f"{faltantes}. Disponibles: {datos.columns}"
        )


def normalizar_columnas_entrada(datos: Any, activar: bool) -> Any:
    if not activar:
        nombres = [str(nombre).strip() for nombre in datos.columns]
        invalidos = [
            nombre
            for nombre in nombres
            if not PATRON_NOMBRE.fullmatch(nombre)
        ]
        if invalidos:
            raise ErrorReceta(
                "Hay columnas inválidas y "
                "normalizar_nombres_columnas=false: "
                f"{invalidos}"
            )
        return datos

    originales = list(datos.columns)
    normalizados = [
        normalizar_nombre_columna(nombre) for nombre in originales
    ]
    repetidos = sorted({
        nombre
        for nombre in normalizados
        if normalizados.count(nombre) > 1
    })
    if repetidos:
        raise ErrorReceta(
            "Dos o más columnas producen el mismo "
            f"nombre normalizado: {repetidos}. "
            f"Originales: {originales}"
        )

    cambios = [
        f"{origen}->{destino}"
        for origen, destino in zip(originales, normalizados)
        if origen != destino
    ]
    if cambios:
        emitir("COLUMNAS_NORMALIZADAS=" + ";;".join(cambios))
    return datos.toDF(*normalizados)


def aplicar_tipos_forzados_entrada(
    datos: Any,
    configuracion: EntradaConfig,
) -> Any:
    from pyspark.sql import functions as F

    tipos_forzados = configuracion.tipos_forzados
    if tipos_forzados is None:
        tipos_forzados = {}
    if not isinstance(tipos_forzados, dict):
        raise ErrorReceta(
            "entrada.tipos_forzados debe ser un objeto"
        )
    if not tipos_forzados:
        return datos

    resultado = datos
    for nombre_crudo, tipo_crudo in tipos_forzados.items():
        nombre = normalizar_nombre_columna(str(nombre_crudo))
        exigir_columnas(resultado, [nombre], 0)
        tipo_spark = convertir_tipo_spark(str(tipo_crudo))
        expresion_convertida = F.col(nombre).cast(
            tipo_spark.simpleString()
        )
        conversion_invalida = (
            F.col(nombre).isNotNull()
            & (F.trim(F.col(nombre).cast("string")) != "")
            & expresion_convertida.isNull()
        )
        if resultado.where(conversion_invalida).limit(1).count() > 0:
            raise ErrorReceta(
                "No se pudo convertir la columna "
                f"{nombre!r} al tipo "
                f"{tipo_spark.simpleString()}"
            )
        resultado = resultado.withColumn(nombre, expresion_convertida)

    emitir(
        "TIPOS_FORZADOS_ENTRADA="
        + ";;".join(
            f"{normalizar_nombre_columna(str(nombre))}"
            f"->{str(tipo).strip().lower()}"
            for nombre, tipo in tipos_forzados.items()
        )
    )
    return resultado
