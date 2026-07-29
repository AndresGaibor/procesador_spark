#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import grp
import os
import shutil
from urllib.parse import unquote, urlparse



PATRON_NOMBRE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PATRON_DECIMAL = re.compile(
    r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
    re.IGNORECASE,
)


class ErrorReceta(Exception):
    """Error de validación o ejecución de una receta."""


def preparar_salida_local(
    ruta: str,
    modo: str,
) -> str:
    """
    Prepara una salida file:// compartida por el driver
    y los executors en un Spark Standalone de un solo nodo.

    Para overwrite:
    1. elimina la salida anterior;
    2. crea la carpeta con grupo spark;
    3. conserva escritura grupal;
    4. devuelve append para impedir que Spark la elimine otra vez.
    """

    ruta_analizada = urlparse(ruta)

    if ruta_analizada.scheme not in {"", "file"}:
        return modo

    ruta_local = (
        unquote(ruta_analizada.path)
        if ruta_analizada.scheme == "file"
        else ruta
    )

    if modo != "overwrite":
        return modo

    if not ruta_local.startswith(
        "/srv/talend-motor/salida/"
    ):
        raise ErrorReceta(
            "Se rechazó overwrite fuera del directorio permitido: "
            f"{ruta_local}"
        )

    if os.path.lexists(ruta_local):
        shutil.rmtree(ruta_local)

    os.makedirs(
        ruta_local,
        mode=0o2770,
        exist_ok=False,
    )

    gid_spark = grp.getgrnam("spark").gr_gid

    os.chown(
        ruta_local,
        -1,
        gid_spark,
    )

    os.chmod(
        ruta_local,
        0o2770,
    )

    print(
        "SALIDA_LOCAL_PREPARADA="
        f"{ruta_local} modo_spark=append",
        flush=True,
    )

    return "append"

def obtener_metricas_salida(
    spark: SparkSession,
    ruta_salida: str,
) -> Dict[str, Any]:
    """
    Verifica mediante Hadoop FileSystem que la salida exista,
    contenga _SUCCESS y tenga archivos Parquet válidos.

    Funciona tanto para:
    - file:///
    - hdfs:///
    """

    configuracion_hadoop = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    jvm = spark._jvm

    ruta = jvm.org.apache.hadoop.fs.Path(
        ruta_salida
    )

    sistema_archivos = ruta.getFileSystem(
        configuracion_hadoop
    )

    if not sistema_archivos.exists(ruta):
        raise ErrorReceta(
            "No existe la ruta de salida generada: "
            f"{ruta_salida}"
        )

    if not sistema_archivos.isDirectory(ruta):
        raise ErrorReceta(
            "La ruta de salida no es un directorio: "
            f"{ruta_salida}"
        )

    ruta_success = jvm.org.apache.hadoop.fs.Path(
        ruta,
        "_SUCCESS",
    )

    archivo_success = sistema_archivos.exists(
        ruta_success
    )

    if not archivo_success:
        raise ErrorReceta(
            "Spark no generó el archivo _SUCCESS en: "
            f"{ruta_salida}"
        )

    cantidad_archivos_parquet = 0
    bytes_parquet = 0

    archivos = sistema_archivos.listFiles(
        ruta,
        True,
    )

    while archivos.hasNext():
        estado = archivos.next()

        nombre = (
            estado
            .getPath()
            .getName()
        )

        if nombre.endswith(".parquet"):
            cantidad_archivos_parquet += 1
            bytes_parquet += int(
                estado.getLen()
            )

    if cantidad_archivos_parquet <= 0:
        raise ErrorReceta(
            "No se encontraron archivos Parquet en: "
            f"{ruta_salida}"
        )

    if bytes_parquet <= 0:
        raise ErrorReceta(
            "Los archivos Parquet están vacíos en: "
            f"{ruta_salida}"
        )

    esquema_ruta = (
        urlparse(ruta_salida).scheme
        or "file"
    )

    print(
        "SALIDA_VALIDADA="
        f"ruta={ruta_salida} "
        f"esquema={esquema_ruta} "
        f"success={archivo_success} "
        f"archivos_parquet={cantidad_archivos_parquet} "
        f"bytes_parquet={bytes_parquet}",
        flush=True,
    )

    return {
        "archivo_success": archivo_success,
        "cantidad_archivos_parquet": (
            cantidad_archivos_parquet
        ),
        "bytes_parquet": bytes_parquet,
        "esquema_almacenamiento": esquema_ruta,
    }

def convertir_tipo_spark(tipo_crudo: str):
    tipo = tipo_crudo.strip().lower()

    tipos_simples = {
        "string": StringType(),
        "texto": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "entero": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "fecha": DateType(),
        "timestamp": TimestampType(),
    }

    if tipo in tipos_simples:
        return tipos_simples[tipo]

    coincidencia = PATRON_DECIMAL.fullmatch(tipo)

    if coincidencia:
        precision = int(coincidencia.group(1))
        escala = int(coincidencia.group(2))

        if precision < 1 or precision > 38:
            raise ErrorReceta(
                f"Precisión decimal fuera del rango 1-38: {tipo}"
            )

        if escala < 0 or escala > precision:
            raise ErrorReceta(
                f"Escala decimal inválida: {tipo}"
            )

        return DecimalType(precision, escala)

    raise ErrorReceta(f"Tipo no soportado: {tipo_crudo}")


def construir_esquema(especificacion: str) -> StructType:
    if not especificacion or not especificacion.strip():
        raise ErrorReceta("SCHEMA_SPEC está vacío")

    campos: List[StructField] = []
    nombres: Set[str] = set()

    for posicion, definicion in enumerate(
        especificacion.split("|"),
        start=1,
    ):
        definicion = definicion.strip()

        if not definicion:
            raise ErrorReceta(
                f"Definición vacía en la posición {posicion}"
            )

        if ":" not in definicion:
            raise ErrorReceta(
                f"Falta ':' en la definición: {definicion}"
            )

        nombre, tipo = definicion.split(":", 1)

        nombre = nombre.strip()
        tipo = tipo.strip()

        if not PATRON_NOMBRE.fullmatch(nombre):
            raise ErrorReceta(
                f"Nombre de columna inválido: {nombre}"
            )

        nombre_normalizado = nombre.lower()

        if nombre_normalizado in nombres:
            raise ErrorReceta(
                f"Columna repetida: {nombre}"
            )

        nombres.add(nombre_normalizado)

        campos.append(
            StructField(
                nombre,
                convertir_tipo_spark(tipo),
                nullable=True,
            )
        )

    return StructType(campos)


def convertir_booleano(
    valor: Any,
    predeterminado: bool,
) -> bool:
    if valor is None:
        return predeterminado

    if isinstance(valor, bool):
        return valor

    normalizado = str(valor).strip().lower()

    if normalizado in {"1", "true", "si", "sí", "yes"}:
        return True

    if normalizado in {"0", "false", "no"}:
        return False

    raise ErrorReceta(
        f"Valor booleano inválido: {valor!r}"
    )


def obtener_modo_esquema(
    configuracion: Dict[str, Any],
) -> str:
    modo = str(
        configuracion.get(
            "modo_esquema",
            "estricto",
        )
    ).strip().lower()

    aliases = {
        "strict": "estricto",
        "schema": "estricto",
        "infer": "inferir",
        "inferido": "inferir",
        "dinamico": "inferir",
        "dinámico": "inferir",
    }

    modo = aliases.get(modo, modo)

    if modo not in {"estricto", "inferir"}:
        raise ErrorReceta(
            "entrada.modo_esquema debe ser "
            "'estricto' o 'inferir', pero se recibió: "
            f"{modo!r}"
        )

    return modo


def resolver_esquema_entrada(
    especificacion: Optional[str],
    configuracion: Dict[str, Any],
) -> tuple[str, Optional[StructType]]:
    modo = obtener_modo_esquema(configuracion)

    if modo == "inferir":
        return modo, None

    return modo, construir_esquema(
        especificacion or ""
    )


def normalizar_nombre_columna(
    nombre: str,
) -> str:
    original = str(nombre or "").strip()

    if not original:
        raise ErrorReceta(
            "Se encontró una columna sin nombre"
        )

    sin_tildes = "".join(
        caracter
        for caracter in unicodedata.normalize(
            "NFKD",
            original,
        )
        if not unicodedata.combining(caracter)
    )

    normalizado = re.sub(
        r"[^A-Za-z0-9_]+",
        "_",
        sin_tildes,
    )

    normalizado = re.sub(
        r"_+",
        "_",
        normalizado,
    ).strip("_").lower()

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


def normalizar_columnas_entrada(
    datos: DataFrame,
    activar: bool,
) -> DataFrame:
    if not activar:
        nombres = [
            str(nombre).strip()
            for nombre in datos.columns
        ]

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
        normalizar_nombre_columna(nombre)
        for nombre in originales
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
        for origen, destino in zip(
            originales,
            normalizados,
        )
        if origen != destino
    ]

    if cambios:
        print(
            "COLUMNAS_NORMALIZADAS="
            + ";;".join(cambios),
            flush=True,
        )

    return datos.toDF(*normalizados)


def aplicar_tipos_forzados_entrada(
    datos: DataFrame,
    configuracion: Dict[str, Any],
) -> DataFrame:
    tipos_forzados = configuracion.get(
        "tipos_forzados",
        {},
    )

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
        nombre = normalizar_nombre_columna(
            str(nombre_crudo)
        )

        exigir_columnas(
            resultado,
            [nombre],
            0,
        )

        tipo_spark = convertir_tipo_spark(
            str(tipo_crudo)
        )

        expresion_convertida = F.col(
            nombre
        ).cast(
            tipo_spark.simpleString()
        )

        conversion_invalida = (
            F.col(nombre).isNotNull()
            & (
                F.trim(
                    F.col(nombre).cast("string")
                )
                != ""
            )
            & expresion_convertida.isNull()
        )

        if (
            resultado
            .where(conversion_invalida)
            .limit(1)
            .count()
            > 0
        ):
            raise ErrorReceta(
                "No se pudo convertir la columna "
                f"{nombre!r} al tipo "
                f"{tipo_spark.simpleString()}"
            )

        resultado = resultado.withColumn(
            nombre,
            expresion_convertida,
        )

    print(
        "TIPOS_FORZADOS_ENTRADA="
        + ";;".join(
            f"{normalizar_nombre_columna(str(nombre))}"
            f"->{str(tipo).strip().lower()}"
            for nombre, tipo in tipos_forzados.items()
        ),
        flush=True,
    )

    return resultado


def convertir_columnas_void_a_string(
    datos: DataFrame,
) -> DataFrame:
    resultado = datos
    convertidas: List[str] = []

    for campo in datos.schema.fields:
        if isinstance(campo.dataType, NullType):
            resultado = resultado.withColumn(
                campo.name,
                F.col(campo.name).cast("string"),
            )
            convertidas.append(campo.name)

    if convertidas:
        print(
            "COLUMNAS_VOID_CONVERTIDAS_STRING="
            + ",".join(convertidas),
            flush=True,
        )

    return resultado


def validar_evolucion_esquema(
    esquema_actual: StructType,
    esquema_nuevo: StructType,
) -> List[StructField]:
    campos_actuales = list(esquema_actual.fields)
    campos_nuevos = list(esquema_nuevo.fields)

    if len(campos_actuales) > len(campos_nuevos):
        raise ErrorReceta(
            "EVOLUCION_ESQUEMA_RECHAZADA=true; "
            "MOTIVO=ELIMINACION_COLUMNAS; "
            f"ACTUAL={len(campos_actuales)}; "
            f"NUEVO={len(campos_nuevos)}"
        )

    for indice, campo_actual in enumerate(
        campos_actuales
    ):
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

        tipo_actual = (
            campo_actual.dataType.simpleString()
        )
        tipo_nuevo = (
            campo_nuevo.dataType.simpleString()
        )

        if tipo_actual != tipo_nuevo:
            raise ErrorReceta(
                "EVOLUCION_ESQUEMA_RECHAZADA=true; "
                "MOTIVO=CAMBIO_TIPO; "
                f"COLUMNA={campo_actual.name}; "
                f"ACTUAL={tipo_actual}; "
                f"NUEVO={tipo_nuevo}"
            )

    nuevas_columnas = campos_nuevos[
        len(campos_actuales):
    ]

    print(
        "EVOLUCION_ESQUEMA_SPARK_VALIDADA=true "
        f"actual={len(campos_actuales)} "
        f"nuevo={len(campos_nuevos)} "
        f"agregadas={len(nuevas_columnas)}",
        flush=True,
    )

    return nuevas_columnas


def exigir_columnas(
    datos: DataFrame,
    columnas: List[str],
    numero_paso: int,
) -> None:
    disponibles = set(datos.columns)
    faltantes = [
        columna
        for columna in columnas
        if columna not in disponibles
    ]

    if faltantes:
        raise ErrorReceta(
            f"Paso {numero_paso}: columnas inexistentes: "
            f"{faltantes}. Disponibles: {datos.columns}"
        )


def aplicar_normalizacion_texto(
    datos: DataFrame,
    paso: Dict[str, Any],
    numero_paso: int,
) -> DataFrame:
    columnas = paso.get("columnas", [])
    operaciones = paso.get("operaciones", [])

    exigir_columnas(datos, columnas, numero_paso)

    resultado = datos

    for columna in columnas:
        expresion = F.col(columna)

        for operacion in operaciones:
            operacion = operacion.lower()

            if operacion == "trim":
                expresion = F.trim(expresion)

            elif operacion == "lower":
                expresion = F.lower(expresion)

            elif operacion == "upper":
                expresion = F.upper(expresion)

            else:
                raise ErrorReceta(
                    f"Paso {numero_paso}: normalización "
                    f"no soportada: {operacion}"
                )

        resultado = resultado.withColumn(
            columna,
            expresion,
        )

    return resultado


def aplicar_conversion_tipo(
    datos: DataFrame,
    paso: Dict[str, Any],
    numero_paso: int,
) -> DataFrame:
    columna = paso["columna"]
    destino = paso["destino"].strip().lower()
    formato = paso.get("formato")

    exigir_columnas(datos, [columna], numero_paso)

    if destino in {"date", "fecha"}:
        expresion = (
            F.to_date(F.col(columna), formato)
            if formato
            else F.to_date(F.col(columna))
        )

    elif destino == "timestamp":
        expresion = (
            F.to_timestamp(F.col(columna), formato)
            if formato
            else F.to_timestamp(F.col(columna))
        )

    else:
        tipo_spark = convertir_tipo_spark(destino)
        expresion = F.col(columna).cast(
            tipo_spark.simpleString()
        )

    return datos.withColumn(
        columna,
        expresion,
    )


def construir_agregacion(
    metrica: Dict[str, Any],
):
    operacion = metrica["operacion"].lower()
    columna = metrica.get("columna", "*")
    alias = metrica["alias"]

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
        expresion = F.first(
            F.col(columna),
            ignorenulls=True,
        )

    elif operacion == "last":
        expresion = F.last(
            F.col(columna),
            ignorenulls=True,
        )

    else:
        raise ErrorReceta(
            f"Agregación no soportada: {operacion}"
        )

    return expresion.alias(alias)


def aplicar_agrupacion(
    datos: DataFrame,
    paso: Dict[str, Any],
    numero_paso: int,
) -> DataFrame:
    columnas = paso.get("columnas", [])
    metricas = paso.get("metricas", [])

    exigir_columnas(datos, columnas, numero_paso)

    if not metricas:
        raise ErrorReceta(
            f"Paso {numero_paso}: la agrupación no tiene métricas"
        )

    expresiones = []

    for metrica in metricas:
        columna = metrica.get("columna", "*")

        if columna != "*":
            exigir_columnas(
                datos,
                [columna],
                numero_paso,
            )

        expresiones.append(
            construir_agregacion(metrica)
        )

    return (
        datos
        .groupBy(*columnas)
        .agg(*expresiones)
    )


def aplicar_pasos(
    datos: DataFrame,
    pasos: List[Dict[str, Any]],
) -> DataFrame:
    resultado = datos

    for numero, paso in enumerate(pasos, start=1):
        tipo = paso.get("tipo", "").strip().lower()

        if not tipo:
            raise ErrorReceta(
                f"El paso {numero} no tiene tipo"
            )

        print(
            f"PASO_INICIO numero={numero} tipo={tipo}",
            flush=True,
        )

        if tipo == "seleccionar_columnas":
            columnas = paso["columnas"]
            exigir_columnas(resultado, columnas, numero)
            resultado = resultado.select(*columnas)

        elif tipo == "eliminar_columnas":
            columnas = paso["columnas"]
            exigir_columnas(resultado, columnas, numero)
            resultado = resultado.drop(*columnas)

        elif tipo == "renombrar_columna":
            origen = paso["origen"]
            destino = paso["destino"]

            exigir_columnas(resultado, [origen], numero)

            if destino in resultado.columns:
                raise ErrorReceta(
                    f"Paso {numero}: ya existe {destino}"
                )

            resultado = resultado.withColumnRenamed(
                origen,
                destino,
            )

        elif tipo == "convertir_tipo":
            resultado = aplicar_conversion_tipo(
                resultado,
                paso,
                numero,
            )

        elif tipo == "crear_columna":
            resultado = resultado.withColumn(
                paso["nombre"],
                F.expr(paso["expresion"]),
            )

        elif tipo == "filtrar":
            resultado = resultado.filter(
                F.expr(paso["expresion"])
            )

        elif tipo == "rellenar_nulos":
            valores = paso.get("valores", {})

            exigir_columnas(
                resultado,
                list(valores.keys()),
                numero,
            )

            resultado = resultado.fillna(valores)

        elif tipo == "normalizar_texto":
            resultado = aplicar_normalizacion_texto(
                resultado,
                paso,
                numero,
            )

        elif tipo == "eliminar_duplicados":
            columnas = paso.get("columnas", [])

            exigir_columnas(
                resultado,
                columnas,
                numero,
            )

            resultado = resultado.dropDuplicates(
                columnas
            )

        elif tipo == "agrupar":
            resultado = aplicar_agrupacion(
                resultado,
                paso,
                numero,
            )

        elif tipo == "reparticionar":
            cantidad = int(paso["cantidad"])
            columnas = paso.get("columnas", [])

            if cantidad < 1:
                raise ErrorReceta(
                    f"Paso {numero}: particiones inválidas"
                )

            if columnas:
                exigir_columnas(
                    resultado,
                    columnas,
                    numero,
                )

                resultado = resultado.repartition(
                    cantidad,
                    *[
                        F.col(columna)
                        for columna in columnas
                    ],
                )
            else:
                resultado = resultado.repartition(
                    cantidad
                )

        else:
            raise ErrorReceta(
                f"Operación no soportada en el paso "
                f"{numero}: {tipo}"
            )

        print(
            f"PASO_FIN numero={numero} "
            f"tipo={tipo} columnas={resultado.columns}",
            flush=True,
        )

    return resultado


def convertir_opciones(
    opciones: Dict[str, Any],
) -> Dict[str, str]:
    resultado: Dict[str, str] = {}

    for clave, valor in opciones.items():
        if isinstance(valor, bool):
            resultado[clave] = str(valor).lower()
        else:
            resultado[clave] = str(valor)

    return resultado


def leer_datos(
    spark: SparkSession,
    ruta: str,
    esquema: Optional[StructType],
    configuracion: Dict[str, Any],
) -> DataFrame:
    formato = str(
        configuracion.get(
            "formato",
            "csv",
        )
    ).strip().lower()

    modo_esquema = obtener_modo_esquema(
        configuracion
    )

    opciones = convertir_opciones(
        configuracion.get("opciones", {})
    )

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
                configuracion.get(
                    "inferir_tipos",
                    True,
                ),
                True,
            )

            opciones["header"] = "true"
            opciones["inferSchema"] = str(
                inferir_tipos
            ).lower()
            opciones.setdefault(
                "mode",
                "FAILFAST",
            )

    lector = (
        spark.read
        .format(formato)
        .options(**opciones)
    )

    if esquema is not None:
        lector = lector.schema(esquema)

    datos = lector.load(ruta)

    if modo_esquema == "inferir":
        normalizar_nombres = convertir_booleano(
            configuracion.get(
                "normalizar_nombres_columnas",
                True,
            ),
            True,
        )

        datos = normalizar_columnas_entrada(
            datos,
            activar=normalizar_nombres,
        )

        datos = convertir_columnas_void_a_string(
            datos
        )

        datos = aplicar_tipos_forzados_entrada(
            datos,
            configuracion,
        )

    print(
        "LECTURA_COMPLETADA="
        f"formato={formato} "
        f"modo_esquema={modo_esquema} "
        f"columnas={len(datos.columns)}",
        flush=True,
    )

    return datos



def ruta_existe_hadoop(
    spark,
    ruta: str,
) -> bool:
    ruta_hadoop = (
        spark._jvm
        .org.apache.hadoop.fs.Path(ruta)
    )

    sistema_archivos = (
        ruta_hadoop.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )
    )

    return bool(
        sistema_archivos.exists(ruta_hadoop)
    )


def escribir_datos(
    spark: SparkSession,
    datos: DataFrame,
    ruta: str,
    configuracion: Dict[str, Any],
) -> Dict[str, Any]:
    formato = configuracion.get(
        "formato",
        "parquet",
    ).lower()

    modo = configuracion.get(
        "modo",
        "error",
    ).lower()

    modo_efectivo = preparar_salida_local(
        ruta=ruta,
        modo=modo,
    )

    compresion = configuracion.get(
        "compresion",
        "snappy",
    )

    numero_particiones = configuracion.get(
        "numero_particiones"
    )

    columnas_reparticion = configuracion.get(
        "columnas_reparticion",
        [],
    )

    particionar_por = configuracion.get(
        "particionar_por",
        [],
    )

    resultado = datos

    if numero_particiones:
        numero_particiones = int(
            numero_particiones
        )

        if numero_particiones < 1:
            raise ErrorReceta(
                "numero_particiones debe ser mayor que cero"
            )

        if columnas_reparticion:
            exigir_columnas(
                resultado,
                columnas_reparticion,
                0,
            )

            resultado = resultado.repartition(
                numero_particiones,
                *[
                    F.col(columna)
                    for columna in columnas_reparticion
                ],
            )
        else:
            resultado = resultado.repartition(
                numero_particiones
            )

    if particionar_por:
        exigir_columnas(
            resultado,
            particionar_por,
            0,
        )

    escritor = (
        resultado.write
        .format(formato)
        .mode(modo_efectivo)
    )

    if compresion and compresion != "none":
        escritor = escritor.option(
            "compression",
            compresion,
        )

    if particionar_por:
        escritor = escritor.partitionBy(
            *particionar_por
        )

    print(
        "ESCRITURA_INICIO="
        f"ruta={ruta} "
        f"formato={formato} "
        f"modo={modo_efectivo}",
        flush=True,
    )

    escritor.save(ruta)

    print(
        "ESCRITURA_FIN="
        f"ruta={ruta}",
        flush=True,
    )

    if formato != "parquet":
        raise ErrorReceta(
            "La publicación actual hacia Impala requiere "
            f"formato parquet, pero se recibió: {formato}"
        )

    return obtener_metricas_salida(
        spark=spark,
        ruta_salida=ruta,
    )


def guardar_resultado(
    ruta: Optional[str],
    contenido: Dict[str, Any],
) -> None:
    if not ruta:
        return

    archivo = Path(ruta)
    archivo.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporal = archivo.with_suffix(
        archivo.suffix + ".tmp"
    )

    temporal.write_text(
        json.dumps(
            contenido,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temporal.replace(archivo)


def crear_argumentos() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Motor Spark mecánico dirigido por recetas JSON"
        )
    )

    parser.add_argument(
        "--receta",
        required=True,
    )

    parser.add_argument(
        "--entrada",
        required=True,
    )

    parser.add_argument(
        "--salida",
        required=True,
    )

    parser.add_argument(
        "--esquema",
        required=False,
        default="",
        help=(
            "Esquema columna:tipo separado por |. "
            "Solo se usa cuando "
            "entrada.modo_esquema=estricto."
        ),
    )

    parser.add_argument(
        "--resultado",
        default=None,
    )

    parser.add_argument(
        "--ejecucion-id",
        required=True,
    )

    return parser

def cargar_receta(valor: str) -> Dict[str, Any]:
    """
    Acepta:
    - Un JSON enviado directamente.
    - Una ruta hacia un archivo JSON, para compatibilidad.
    """
    texto = str(valor or "").strip()

    if not texto:
        raise ErrorReceta("La receta está vacía")

    try:
        if texto.startswith("{"):
            receta = json.loads(texto)
        else:
            ruta = Path(texto)

            if not ruta.is_file():
                raise ErrorReceta(
                    f"No existe el archivo de receta: {ruta}"
                )

            receta = json.loads(
                ruta.read_text(encoding="utf-8")
            )

    except json.JSONDecodeError as excepcion:
        raise ErrorReceta(
            "JSON de receta inválido: "
            f"línea={excepcion.lineno}, "
            f"columna={excepcion.colno}, "
            f"detalle={excepcion.msg}"
        ) from excepcion

    if not isinstance(receta, dict):
        raise ErrorReceta(
            "La receta debe ser un objeto JSON"
        )

    return receta


def main() -> int:
    argumentos = crear_argumentos().parse_args()

    spark: Optional[SparkSession] = None

    try:
        receta = cargar_receta(argumentos.receta)

        configuracion_entrada = receta.get(
            "entrada",
            {},
        )

        if not isinstance(
            configuracion_entrada,
            dict,
        ):
            raise ErrorReceta(
                "receta.entrada debe ser un objeto"
            )

        modo_esquema, esquema = (
            resolver_esquema_entrada(
                argumentos.esquema,
                configuracion_entrada,
            )
        )

        nombre = receta.get(
            "nombre",
            "Motor Spark mecánico",
        )

        spark = (
            SparkSession.builder
            .appName(
                f"{nombre} - {argumentos.ejecucion_id}"
            )
            .getOrCreate()
        )

        configuracion_spark = receta.get(
            "spark",
            {},
        )

        nivel_log = configuracion_spark.get(
            "nivel_log",
            "WARN",
        )

        spark.sparkContext.setLogLevel(nivel_log)

        shuffle_partitions = configuracion_spark.get(
            "shuffle_partitions"
        )

        if shuffle_partitions:
            spark.conf.set(
                "spark.sql.shuffle.partitions",
                int(shuffle_partitions),
            )

        print(
            "EJECUCION_INICIO="
            + argumentos.ejecucion_id,
            flush=True,
        )

        print(
            "MODO_ESQUEMA_ENTRADA="
            + modo_esquema,
            flush=True,
        )

        if esquema is not None:
            print(
                "ESQUEMA_ENTRADA_DECLARADO="
                + esquema.simpleString(),
                flush=True,
            )
        elif argumentos.esquema.strip():
            print(
                "SCHEMA_SPEC_IGNORADO=true",
                flush=True,
            )

        datos = leer_datos(
            spark=spark,
            ruta=argumentos.entrada,
            esquema=esquema,
            configuracion=configuracion_entrada,
        )

        print(
            "ESQUEMA_ENTRADA_REAL="
            + datos.schema.simpleString(),
            flush=True,
        )

        procesados = aplicar_pasos(
            datos,
            receta.get("pasos", []),
        )

        configuracion_incremental = (
            receta.get(
                "incremental",
                {},
            )
        )

        incremental_activo = bool(
            configuracion_incremental.get(
                "activo",
                False,
            )
        )

        metricas_incrementales = {}

        if incremental_activo:
            politica_duplicados = str(
                configuracion_incremental.get(
                    "duplicados",
                    "ignorar",
                )
            ).strip().lower()

            if politica_duplicados != "ignorar":
                raise ErrorReceta(
                    "La única política incremental "
                    "soportada actualmente es "
                    "duplicados=ignorar"
                )

            claves = [
                str(clave).strip()
                for clave in (
                    configuracion_incremental.get(
                        "claves",
                        [],
                    )
                )
                if str(clave).strip()
            ]

            if not claves:
                raise ErrorReceta(
                    "incremental.claves no puede "
                    "estar vacío"
                )

            exigir_columnas(
                procesados,
                claves,
                0,
            )

            condicion_nulos = " OR ".join(
                f"`{clave}` IS NULL"
                for clave in claves
            )

            if (
                procesados
                .where(condicion_nulos)
                .limit(1)
                .count()
                > 0
            ):
                raise ErrorReceta(
                    "Las claves incrementales no "
                    "pueden contener valores nulos: "
                    + ", ".join(claves)
                )

            procesados.persist()

            total_entrada = procesados.count()

            if total_entrada <= 0:
                procesados.unpersist()

                raise ErrorReceta(
                    "El lote incremental no contiene "
                    "registros procesables"
                )

            lote_unico = (
                procesados
                .dropDuplicates(claves)
                .persist()
            )

            total_unicos_lote = (
                lote_unico.count()
            )

            procesados.unpersist()

            total_antes = 0
            destino_existe = ruta_existe_hadoop(
                spark,
                argumentos.salida,
            )

            if destino_existe:
                existentes = (
                    spark.read
                    .format("parquet")
                    .option(
                        "mergeSchema",
                        "true",
                    )
                    .load(argumentos.salida)
                    .persist()
                )

                validar_evolucion_esquema(
                    existentes.schema,
                    procesados.schema,
                )

                exigir_columnas(
                    existentes,
                    claves,
                    0,
                )

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

                total_nuevos = nuevos.count()

                existentes.unpersist()
            else:
                nuevos = lote_unico
                total_nuevos = total_unicos_lote

            total_duplicados = (
                total_entrada
                - total_nuevos
            )

            total_despues = (
                total_antes
                + total_nuevos
            )

            configuracion_salida = dict(
                receta["salida"]
            )

            configuracion_salida["modo"] = (
                "append"
            )

            if total_nuevos > 0:
                metricas_salida = escribir_datos(
                    spark=spark,
                    datos=nuevos,
                    ruta=argumentos.salida,
                    configuracion=(
                        configuracion_salida
                    ),
                )
            else:
                metricas_salida = (
                    obtener_metricas_salida(
                        spark=spark,
                        ruta_salida=(
                            argumentos.salida
                        ),
                    )
                )

            if nuevos is not lote_unico:
                nuevos.unpersist()

            lote_unico.unpersist()

            total_registros = total_despues

            metricas_incrementales = {
                "modo_carga": "incremental",
                "claves_deduplicacion": claves,
                "total_registros_entrada": (
                    total_entrada
                ),
                "total_registros_unicos_lote": (
                    total_unicos_lote
                ),
                "total_registros_nuevos": (
                    total_nuevos
                ),
                "total_registros_duplicados": (
                    total_duplicados
                ),
                "total_registros_antes": (
                    total_antes
                ),
            }

            print(
                "INCREMENTAL_RESULTADO="
                f"entrada={total_entrada} "
                f"unicos_lote={total_unicos_lote} "
                f"nuevos={total_nuevos} "
                f"duplicados={total_duplicados} "
                f"antes={total_antes} "
                f"despues={total_despues}",
                flush=True,
            )

        else:
            total_registros = None

            contar = bool(
                receta.get(
                    "auditoria",
                    {},
                ).get(
                    "contar_registros",
                    False,
                )
            )

            if contar:
                procesados.persist()
                total_registros = (
                    procesados.count()
                )

            metricas_salida = escribir_datos(
                spark=spark,
                datos=procesados,
                ruta=argumentos.salida,
                configuracion=receta["salida"],
            )

            if contar:
                procesados.unpersist()

        resultado = {
            "estado": "COMPLETADO",
            "ejecucion_id": argumentos.ejecucion_id,
            "receta": receta.get("nombre"),
            "version_receta": receta.get("version"),
            "entrada": argumentos.entrada,
            "salida": argumentos.salida,
            "modo_esquema_entrada": modo_esquema,
            "columnas_entrada": datos.columns,
            "esquema_entrada": (
                datos.schema.jsonValue()
            ),
            "esquema_entrada_simple": (
                datos.schema.simpleString()
            ),
            "columnas_salida": procesados.columns,
            "esquema_salida": (
                procesados.schema.jsonValue()
            ),
            "esquema_salida_simple": (
                procesados.schema.simpleString()
            ),
            "total_registros": total_registros,
        }
        
        resultado.update(
            metricas_incrementales
        )

        resultado.update(
            metricas_salida
        )

        guardar_resultado(
            argumentos.resultado,
            resultado,
        )

        print(
            "RESULTADO_MOTOR="
            + json.dumps(
                resultado,
                ensure_ascii=False,
            ),
            flush=True,
        )

        return 0

    except Exception as excepcion:
        resultado_error = {
            "estado": "ERROR",
            "ejecucion_id": argumentos.ejecucion_id,
            "entrada": argumentos.entrada,
            "salida": argumentos.salida,
            "tipo_error": type(excepcion).__name__,
            "mensaje": str(excepcion),
        }

        guardar_resultado(
            argumentos.resultado,
            resultado_error,
        )

        print(
            "RESULTADO_MOTOR="
            + json.dumps(
                resultado_error,
                ensure_ascii=False,
            ),
            file=sys.stderr,
            flush=True,
        )

        traceback.print_exc()

        return 1

    finally:
        if spark is not None:
            spark.stop()

           
if __name__ == "__main__":
    raise SystemExit(main())
