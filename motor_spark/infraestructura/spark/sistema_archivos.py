from __future__ import annotations

import grp
import os
import shutil
from typing import Any
from urllib.parse import unquote, urlparse

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.dominio.errores import ErrorReceta


def preparar_salida_local(ruta: str, modo: str) -> str:
    ruta_analizada = urlparse(ruta)
    if ruta_analizada.scheme not in {"", "file"}:
        return modo

    ruta_local = (
        unquote(ruta_analizada.path) if ruta_analizada.scheme == "file" else ruta
    )
    if modo != "overwrite":
        return modo
    if not ruta_local.startswith("/srv/talend-motor/salida/"):
        raise ErrorReceta(
            f"Se rechazó overwrite fuera del directorio permitido: {ruta_local}"
        )

    if os.path.lexists(ruta_local):
        shutil.rmtree(ruta_local)
    os.makedirs(ruta_local, mode=0o2770, exist_ok=False)
    gid_spark = grp.getgrnam("spark").gr_gid
    os.chown(ruta_local, -1, gid_spark)
    os.chmod(ruta_local, 0o2770)
    emitir(f"SALIDA_LOCAL_PREPARADA={ruta_local} modo_spark=append")
    return "append"


def _sistema_archivos_para_ruta(spark: Any, ruta: str) -> tuple[Any, Any]:
    ruta_hadoop = spark._jvm.org.apache.hadoop.fs.Path(ruta)
    sistema_archivos = ruta_hadoop.getFileSystem(spark._jsc.hadoopConfiguration())
    return ruta_hadoop, sistema_archivos


def ruta_existe_hadoop(spark: Any, ruta: str) -> bool:
    ruta_hadoop, sistema_archivos = _sistema_archivos_para_ruta(
        spark,
        ruta,
    )
    return bool(sistema_archivos.exists(ruta_hadoop))


def obtener_metricas_salida(
    spark: Any,
    ruta_salida: str,
) -> dict[str, Any]:
    configuracion_hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark._jvm
    ruta = jvm.org.apache.hadoop.fs.Path(ruta_salida)
    sistema_archivos = ruta.getFileSystem(configuracion_hadoop)

    if not sistema_archivos.exists(ruta):
        raise ErrorReceta(f"No existe la ruta de salida generada: {ruta_salida}")
    if not sistema_archivos.isDirectory(ruta):
        raise ErrorReceta(f"La ruta de salida no es un directorio: {ruta_salida}")

    ruta_success = jvm.org.apache.hadoop.fs.Path(ruta, "_SUCCESS")
    archivo_success = sistema_archivos.exists(ruta_success)
    if not archivo_success:
        raise ErrorReceta(f"Spark no generó el archivo _SUCCESS en: {ruta_salida}")

    cantidad_archivos_parquet = 0
    bytes_parquet = 0
    archivos = sistema_archivos.listFiles(ruta, True)
    while archivos.hasNext():
        estado = archivos.next()
        nombre = estado.getPath().getName()
        if nombre.endswith(".parquet"):
            cantidad_archivos_parquet += 1
            bytes_parquet += int(estado.getLen())

    if cantidad_archivos_parquet <= 0:
        raise ErrorReceta(f"No se encontraron archivos Parquet en: {ruta_salida}")
    if bytes_parquet <= 0:
        raise ErrorReceta(f"Los archivos Parquet están vacíos en: {ruta_salida}")

    esquema_ruta = urlparse(ruta_salida).scheme or "file"
    emitir(
        "SALIDA_VALIDADA="
        f"ruta={ruta_salida} "
        f"esquema={esquema_ruta} "
        f"success={archivo_success} "
        f"archivos_parquet={cantidad_archivos_parquet} "
        f"bytes_parquet={bytes_parquet}"
    )
    return {
        "archivo_success": archivo_success,
        "cantidad_archivos_parquet": cantidad_archivos_parquet,
        "bytes_parquet": bytes_parquet,
        "esquema_almacenamiento": esquema_ruta,
    }
